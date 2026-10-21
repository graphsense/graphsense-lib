"""Shared state of the sync daemons: who runs which network, what is paused,
and the last status of every network.

None of this protects data. Two jobs on one keyspace are already kept apart by
the per-keyspace ingest/delta-update locks (`utils/locking.py`); the claim here
only keeps two daemons from *scheduling* the same network. That is why a claim
may be a lease with a TTL while the locks deliberately are not: a claim that
expires under a live daemon at worst makes both daemons start a job, and one of
them gets lock contention (exit 911).
"""

import contextlib
import glob
import json
import os
import socket
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from filelock import FileLock

# Long enough to survive a slow scheduler tick, short enough that a network
# moved to another channel (or orphaned by a dead daemon) is picked up quickly.
CLAIM_TTL_SECONDS = 120
DAEMON_TTL_SECONDS = 60


def new_daemon_id(channel: str) -> str:
    """Identity of one daemon process; stored as the claim value."""
    return json.dumps(
        {
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "channel": channel,
            "started_at": int(time.time()),
            "nonce": uuid.uuid4().hex,
        },
        sort_keys=True,
    )


class StateStore(ABC):
    @abstractmethod
    def claim(self, network: str, owner: str) -> bool:
        """Take the network, or extend our claim on it. False if another
        daemon holds it."""

    @abstractmethod
    def release(self, network: str, owner: str) -> None:
        """Give the network up, if we hold it."""

    @abstractmethod
    def claim_owner(self, network: str) -> Optional[str]: ...

    @abstractmethod
    def pause(
        self, network: str, reason: str, by: str, seconds: Optional[int] = None
    ) -> None: ...

    @abstractmethod
    def resume(self, network: str) -> bool:
        """Lift a pause. False if the network was not paused."""

    @abstractmethod
    def get_pause(self, network: str) -> Optional[dict]: ...

    @abstractmethod
    def write_status(self, network: str, status: dict) -> None: ...

    @abstractmethod
    def read_status(self, network: str) -> Optional[dict]: ...

    @abstractmethod
    def announce_daemon(self, daemon_id: str, info: dict) -> None:
        """Heartbeat of a running daemon, for `sync status`."""

    @abstractmethod
    def forget_daemon(self, daemon_id: str) -> None: ...

    @abstractmethod
    def list_daemons(self) -> List[Tuple[str, dict]]: ...


class MemoryStateStore(StateStore):
    """Single-process store, for `sync run --once` without Redis and tests."""

    def __init__(self, clock=time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._data: Dict[str, Tuple[object, Optional[float]]] = {}

    def _get(self, key):
        item = self._data.get(key)
        if item is None:
            return None
        value, expires = item
        if expires is not None and expires <= self._clock():
            del self._data[key]
            return None
        return value

    def _set(self, key, value, ttl=None):
        self._data[key] = (value, None if ttl is None else self._clock() + ttl)

    def claim(self, network, owner):
        with self._lock:
            current = self._get(("claim", network))
            if current not in (None, owner):
                return False
            self._set(("claim", network), owner, CLAIM_TTL_SECONDS)
            return True

    def release(self, network, owner):
        with self._lock:
            if self._get(("claim", network)) == owner:
                del self._data[("claim", network)]

    def claim_owner(self, network):
        with self._lock:
            return self._get(("claim", network))

    def pause(self, network, reason, by, seconds=None):
        with self._lock:
            self._set(("pause", network), _pause_record(reason, by, seconds), seconds)

    def resume(self, network):
        with self._lock:
            found = self._get(("pause", network)) is not None
            self._data.pop(("pause", network), None)
            return found

    def get_pause(self, network):
        with self._lock:
            return self._get(("pause", network))

    def write_status(self, network, status):
        with self._lock:
            self._set(("status", network), dict(status))

    def read_status(self, network):
        with self._lock:
            return self._get(("status", network))

    def announce_daemon(self, daemon_id, info):
        with self._lock:
            self._set(("daemon", daemon_id), dict(info), DAEMON_TTL_SECONDS)

    def forget_daemon(self, daemon_id):
        with self._lock:
            self._data.pop(("daemon", daemon_id), None)

    def list_daemons(self):
        with self._lock:
            keys = [k for k in self._data if k[0] == "daemon"]
            return [(k[1], v) for k in keys if (v := self._get(k)) is not None]


class FileStateStore(StateStore):
    """State in a directory, for setups without Redis (`use_redis_locks` off).

    Every daemon and every `sync pause/status` must see the same directory:
    on one host that is automatic; daemons in separate containers need it on a
    shared volume -- the same caveat as the file locks of the jobs
    themselves (`utils/locking.py`, fixed under /tmp).
    """

    def __init__(self, directory: str, clock=time.time):
        self.directory = directory
        self._clock = clock
        os.makedirs(directory, exist_ok=True)

    def _path(self, kind: str, name: str) -> str:
        return os.path.join(self.directory, f"{kind}.{name}.json")

    @contextlib.contextmanager
    def _locked(self, path: str):
        with FileLock(path + ".lock", timeout=10):
            yield

    def _read(self, path: str):
        try:
            with open(path) as f:
                record = json.load(f)
        except (FileNotFoundError, ValueError):
            return None
        expires = record.get("expires")
        if expires is not None and expires <= self._clock():
            return None
        return record.get("value")

    def _write(self, path: str, value, ttl: Optional[float] = None):
        record = {
            "value": value,
            "expires": None if ttl is None else self._clock() + ttl,
        }
        tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
        with open(tmp, "w") as f:
            json.dump(record, f)
        os.replace(tmp, path)  # readers never see a half-written file

    def _delete(self, path: str) -> bool:
        try:
            os.remove(path)
            return True
        except FileNotFoundError:
            return False

    def claim(self, network, owner):
        path = self._path("claim", network)
        with self._locked(path):
            if self._read(path) not in (None, owner):
                return False
            self._write(path, owner, CLAIM_TTL_SECONDS)
            return True

    def release(self, network, owner):
        path = self._path("claim", network)
        with self._locked(path):
            if self._read(path) == owner:
                self._delete(path)

    def claim_owner(self, network):
        return self._read(self._path("claim", network))

    def pause(self, network, reason, by, seconds=None):
        self._write(
            self._path("pause", network), _pause_record(reason, by, seconds), seconds
        )

    def resume(self, network):
        path = self._path("pause", network)
        found = self._read(path) is not None
        self._delete(path)
        return found

    def get_pause(self, network):
        return self._read(self._path("pause", network))

    def write_status(self, network, status):
        self._write(self._path("status", network), dict(status))

    def read_status(self, network):
        return self._read(self._path("status", network))

    def announce_daemon(self, daemon_id, info):
        self._write(self._path("daemon", daemon_id), dict(info), DAEMON_TTL_SECONDS)

    def forget_daemon(self, daemon_id):
        self._delete(self._path("daemon", daemon_id))

    def list_daemons(self):
        out = []
        for path in sorted(glob.glob(self._path("daemon", "*"))):
            info = self._read(path)
            if info is not None:
                name = os.path.basename(path)[len("daemon.") : -len(".json")]
                out.append((name, info))
        return out


# Compare-and-set on the claim value, so a daemon never extends or drops a
# claim another daemon took after ours expired.
_EXTEND_IF_OWNER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
_DELETE_IF_OWNER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class RedisStateStore(StateStore):
    def __init__(self, redis_url: str, env: str):
        import redis

        self._r = redis.from_url(redis_url, decode_responses=True)
        self._prefix = f"graphsense:sync:{env}:"
        self._extend = self._r.register_script(_EXTEND_IF_OWNER)
        self._delete = self._r.register_script(_DELETE_IF_OWNER)

    def _k(self, *parts) -> str:
        return self._prefix + ":".join(parts)

    def claim(self, network, owner):
        key = self._k("claim", network)
        if self._r.set(key, owner, nx=True, ex=CLAIM_TTL_SECONDS):
            return True
        return bool(self._extend(keys=[key], args=[owner, CLAIM_TTL_SECONDS]))

    def release(self, network, owner):
        self._delete(keys=[self._k("claim", network)], args=[owner])

    def claim_owner(self, network):
        return self._r.get(self._k("claim", network))

    def pause(self, network, reason, by, seconds=None):
        self._r.set(
            self._k("pause", network),
            json.dumps(_pause_record(reason, by, seconds)),
            ex=seconds,
        )

    def resume(self, network):
        return bool(self._r.delete(self._k("pause", network)))

    def get_pause(self, network):
        return _loads(self._r.get(self._k("pause", network)))

    def write_status(self, network, status):
        self._r.set(self._k("status", network), json.dumps(status))

    def read_status(self, network):
        return _loads(self._r.get(self._k("status", network)))

    def announce_daemon(self, daemon_id, info):
        self._r.set(
            self._k("daemon", daemon_id), json.dumps(info), ex=DAEMON_TTL_SECONDS
        )

    def forget_daemon(self, daemon_id):
        self._r.delete(self._k("daemon", daemon_id))

    def list_daemons(self):
        prefix = self._k("daemon", "")
        out = []
        for key in self._r.scan_iter(match=prefix + "*"):
            info = _loads(self._r.get(key))
            if info is not None:
                out.append((key[len(prefix) :], info))
        return out


def _pause_record(reason: str, by: str, seconds: Optional[int]) -> dict:
    now = int(time.time())
    return {
        "reason": reason,
        "by": by,
        "since": now,
        "until": None if seconds is None else now + seconds,
    }


def _loads(value: Optional[str]) -> Optional[dict]:
    if value is None:
        return None
    try:
        return json.loads(value)
    except ValueError:
        return None
