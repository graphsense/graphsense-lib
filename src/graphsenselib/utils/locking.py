import contextlib
import logging

from filelock import FileLock
from filelock import Timeout as LockFileTimeout

logger = logging.getLogger(__name__)


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

    try:
        client = redis.from_url(redis_url)
        lock = client.lock(key, timeout=None, blocking_timeout=blocking_timeout)
        acquired = lock.acquire()
    except redis.exceptions.RedisError as e:  # type: ignore[union-attr]
        raise LockAcquisitionError(
            f"Redis lock {key} could not be acquired due to connection error: {e}"
        )

    if not acquired:
        raise LockAcquisitionError(
            f"Redis lock {key} could not be acquired. "
            f"Is another process running? To force-release: redis-cli DEL {key}"
        )

    logger.info(f"Redis lock {key} acquired.")
    try:
        yield
    finally:
        lock.release()
        logger.info(f"Redis lock {key} released.")
