import contextlib
import json
import logging
import os
import socket
import threading
import time
import uuid
from typing import Optional

from filelock import FileLock
from filelock import Timeout as LockFileTimeout

logger = logging.getLogger(__name__)

# The Redis lock deliberately has NO TTL, and nothing ever takes it away from
# its owner.
#
# A TTL has to be renewed by the holder, and every way that renewal can stall
# (Redis unreachable, the process stopped or thrashing, a long GIL-holding call)
# ends with the lock expiring *while the holder is still writing*: two ingest
# processes on one delta table, silently. Auto-reclaiming the lock by proving
# the owner is dead has the same failure mode wherever "is that pid alive?" is
# not answerable -- another host, another pid namespace -- and it cannot work
# across hosts at all, since /proc only knows about this machine.
#
# So a holder that dies hard (OOM kill, SIGKILL, reboot) does leave the key
# behind, and that is paid for by *telling someone* rather than by guessing: the
# holder records who it is and stamps a heartbeat next to the lock while it
# works, and a contender that finds a holder whose heartbeat stopped sends a
# slack notification naming the host, the pid and how long it has been quiet, so
# a human can DEL it. A stalled heartbeat can only produce a false *alarm*,
# never a released lock -- which is what makes this safe where a lease is not.
_HEARTBEAT_INTERVAL_SECONDS = 60
# How long a holder may go unheard from before it gets called out. Generous on
# purpose (~15 missed stamps): this measures silence, not runtime, so a
# legitimately hour-long job keeps stamping and is never accused.
_STALE_HOLDER_AFTER_SECONDS = 15 * 60
# At most one notification per lock per hour, however often the cron retries.
_ALERT_REPEAT_SECONDS = 60 * 60
_ALERT_TOPIC = "exceptions"


class LockAcquisitionError(Exception):
    """Raised when a lock cannot be acquired due to contention (another process holds it).

    Callers should handle this with exit code 911.
    """

    pass


class LockConfigurationError(Exception):
    """Raised for lock misconfiguration (missing Redis URL, missing package, etc).

    This should NOT be caught as LockAcquisitionError — it indicates an infra
    problem, not lock contention.
    """

    pass


def create_lock(
    lock_name,
    blocking_timeout=1.0,
    disabled=False,
):
    """Create a distributed lock context manager.

    Backend is selected via AppConfig fields (use_redis_locks / redis_url),
    which can be set via YAML config or GRAPHSENSE_ env vars (via GoodConf).

    Args:
        lock_name: Name for the lock (used in file path or Redis key).
        blocking_timeout: Seconds to wait to acquire the lock.
        disabled: Return a no-op context manager.

    Returns:
        A context manager that acquires/releases the lock.
    """
    if disabled:
        return contextlib.nullcontext()

    from graphsenselib.config import get_config

    config = get_config()

    if config.use_redis_locks:
        return _redis_lock(lock_name, config.redis_url, blocking_timeout)

    return _file_lock(lock_name, blocking_timeout)


@contextlib.contextmanager
def _file_lock(lock_name, blocking_timeout):
    lockfile_name = f"/tmp/{lock_name}.lock"
    logger.info(f"Try acquiring file lock {lockfile_name}")
    try:
        with FileLock(lockfile_name, timeout=blocking_timeout):
            logger.info(f"File lock {lockfile_name} acquired.")
            yield
    except LockFileTimeout:
        raise LockAcquisitionError(
            f"Lock {lockfile_name} could not be acquired. "
            "Is another process running? If not delete the lockfile."
        )


@contextlib.contextmanager
def _redis_lock(lock_name, redis_url, blocking_timeout):
    try:
        import redis
    except ImportError:
        raise LockConfigurationError(
            "Redis package is required for use_redis_locks. "
            "Install it with: uv add redis"
        )

    if not redis_url:
        raise LockConfigurationError(
            "redis_url not configured. Set it in your graphsense config "
            "or via GRAPHSENSE_REDIS_URL env var."
        )

    key = f"graphsense:lock:{lock_name}"
    logger.info(f"Try acquiring Redis lock {key} at {redis_url}")

    token = _lock_owner_token()
    try:
        client = redis.from_url(redis_url)
        # thread_local=False: the lock may be held via an ExitStack whose close
        # runs somewhere other than where it was acquired.
        lock = client.lock(
            key, timeout=None, blocking_timeout=blocking_timeout, thread_local=False
        )
        acquired = lock.acquire(token=token)
        owner = None if acquired else _read_str(client, key)
    except redis.exceptions.RedisError as e:  # type: ignore[union-attr]
        raise LockAcquisitionError(
            f"Redis lock {key} could not be acquired due to connection error: {e}"
        )

    if not acquired:
        _report_holder_if_gone_quiet(client, key, owner)
        held_by = (
            f"It is held by {owner}. " if owner else "Is another process running? "
        )
        raise LockAcquisitionError(
            f"Redis lock {key} could not be acquired. {held_by}"
            f"If that process no longer exists, force-release with: redis-cli DEL {key}"
        )

    logger.info(f"Redis lock {key} acquired by {token}.")
    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=_heartbeat,
        args=(client, key, stop_heartbeat),
        name=f"redis-lock-heartbeat-{lock_name}",
        daemon=True,
    )
    heartbeat.start()
    try:
        yield
    finally:
        stop_heartbeat.set()
        try:
            lock.release()
            client.delete(_heartbeat_key(key), _alert_key(key))
            logger.info(f"Redis lock {key} released.")
        except Exception as e:
            # Never let a release problem mask what the body did or raised.
            logger.warning(f"Redis lock {key} could not be released: {e}")


def _heartbeat_key(key: str) -> str:
    return f"{key}:heartbeat"


def _alert_key(key: str) -> str:
    return f"{key}:stale-alert-sent"


def _lock_owner_token() -> str:
    """Identity stored as the lock value, so `redis-cli GET <key>` names the
    holder instead of returning an opaque uuid.

    host and pid are for the human reading the alert; nothing decides anything
    from them (in a container they are a discarded container id and pid 1). The
    nonce is what makes the value unique: release() is a compare-and-delete
    against it, and host+pid+second is collidable exactly where pid is
    meaningless.
    """
    return json.dumps(
        {
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "acquired_at": int(time.time()),
            "nonce": uuid.uuid4().hex,
        },
        sort_keys=True,
    )


def _heartbeat(client, key: str, stop: threading.Event) -> None:
    """Stamp "the holder is still alive" next to the lock until it is released.

    Never raises and never touches the lock key itself: this thread failing must
    not affect the work running under the lock, and the worst a missed stamp can
    do is trigger a false stale-holder report.
    """
    hb_key = _heartbeat_key(key)
    while True:
        try:
            client.set(hb_key, str(int(time.time())))
        except Exception as e:
            logger.debug(f"Could not refresh lock heartbeat {hb_key}: {e}")
        if stop.wait(_HEARTBEAT_INTERVAL_SECONDS):
            return


def _read_str(client, key: str) -> Optional[str]:
    value = client.get(key)
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def _holder_last_seen(client, key: str, owner: str) -> Optional[float]:
    """Unix time the holder last proved it was alive, or None when that cannot
    be known -- a lock taken by a version that stamped nothing, or a value we
    cannot parse. Unknown means silent: better no alert than a wrong one."""
    heartbeat = _read_str(client, _heartbeat_key(key))
    if heartbeat is not None:
        try:
            return float(heartbeat)
        except ValueError:
            pass
    try:
        info = json.loads(owner)
    except (TypeError, ValueError):
        return None
    if not isinstance(info, dict):
        return None
    acquired_at = info.get("acquired_at")
    return float(acquired_at) if isinstance(acquired_at, (int, float)) else None


def _report_holder_if_gone_quiet(client, key: str, owner: Optional[str]) -> None:
    """Notify slack when the lock is held by someone that stopped heartbeating.

    This only ever reports -- the lock stays exactly where it is. A human decides
    whether to `redis-cli DEL` it, which is the only safe way to break a lock
    whose owner may live on a machine we cannot see.
    """
    try:
        if owner is None:
            return
        last_seen = _holder_last_seen(client, key, owner)
        if last_seen is None:
            return
        quiet_for = time.time() - last_seen
        if quiet_for < _STALE_HOLDER_AFTER_SECONDS:
            return
        if _read_str(client, key) != owner:
            # Released (or taken over) while we were looking: nothing to report.
            return
        # NX+EX rate-limits the report. This TTL decides how often we talk, never
        # who owns the lock.
        if not client.set(_alert_key(key), "1", nx=True, ex=_ALERT_REPEAT_SECONDS):
            return
        msg = (
            f"Lock {key} has been held by {owner} for at least "
            f"{_format_duration(quiet_for)} without a sign of life. The holder "
            f"probably died without releasing it, which blocks every run that "
            f"needs this lock. Check that host, then force-release with: "
            f"redis-cli DEL {key}"
        )
        logger.warning(msg)

        from ..monitoring.notifications import send_msg_to_topic

        send_msg_to_topic(_ALERT_TOPIC, msg)
    except Exception as e:
        # Reporting must never turn lock contention into a crash.
        logger.warning(f"Could not report stale holder of lock {key}: {e}")


def _format_duration(seconds: float) -> str:
    total = int(seconds)
    hours, minutes = total // 3600, (total % 3600) // 60
    return f"{hours}h {minutes}m" if hours else f"{minutes}m"


def delta_ingest_lock_name(directory: str, network: str) -> str:
    """Derive the lock name for a delta-lake raw resource.

    Returns ``delta_ingest_{bucket}_{prefix}_{network}``. For ``s3://``
    URIs, ``bucket`` is the S3 bucket and ``prefix`` is the rest of the
    path. For local paths, ``bucket`` is ``local`` and ``prefix`` is the
    path with separators replaced. ``network`` is appended unless empty.
    """
    raw = directory.strip().rstrip("/")
    if raw.startswith("s3://"):
        rest = raw[len("s3://") :]
        bucket, _, prefix = rest.partition("/")
        components = [bucket]
        if prefix:
            components.append(prefix.replace("/", "_"))
    else:
        components = ["local", raw.lstrip("/").replace("/", "_")]
    if network:
        components.append(network)
    return "delta_ingest_" + "_".join(c for c in components if c)
