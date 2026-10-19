"""Shared state of the sync daemons: who runs which currency, what is paused,
and the last status of every currency.

None of this protects data. Two jobs on one keyspace are already kept apart by
the per-keyspace ingest/delta-update locks (`utils/locking.py`); the claim here
only keeps two daemons from *scheduling* the same currency. That is why a claim
may be a lease with a TTL while the locks deliberately are not: a claim that
expires under a live daemon at worst makes both daemons start a job, and one of
them gets lock contention (exit 911).
"""

import json
import os
import socket
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

# Long enough to survive a slow scheduler tick, short enough that a currency
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
    def claim(self, currency: str, owner: str) -> bool:
        """Take the currency, or extend our claim on it. False if another
        daemon holds it."""

    @abstractmethod
    def release(self, currency: str, owner: str) -> None:
        """Give the currency up, if we hold it."""

    @abstractmethod
    def claim_owner(self, currency: str) -> Optional[str]: ...

    @abstractmethod
    def pause(
        self, currency: str, reason: str, by: str, seconds: Optional[int] = None
    ) -> None: ...

    @abstractmethod
    def resume(self, currency: str) -> bool:
        """Lift a pause. False if the currency was not paused."""

    @abstractmethod
    def get_pause(self, currency: str) -> Optional[dict]: ...

    @abstractmethod
    def write_status(self, currency: str, status: dict) -> None: ...

    @abstractmethod
    def read_status(self, currency: str) -> Optional[dict]: ...

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

    def claim(self, currency, owner):
        with self._lock:
            current = self._get(("claim", currency))
            if current not in (None, owner):
                return False
            self._set(("claim", currency), owner, CLAIM_TTL_SECONDS)
            return True

    def release(self, currency, owner):
        with self._lock:
            if self._get(("claim", currency)) == owner:
                del self._data[("claim", currency)]

    def claim_owner(self, currency):
        with self._lock:
            return self._get(("claim", currency))

    def pause(self, currency, reason, by, seconds=None):
        with self._lock:
            self._set(("pause", currency), _pause_record(reason, by, seconds), seconds)

    def resume(self, currency):
        with self._lock:
            found = self._get(("pause", currency)) is not None
            self._data.pop(("pause", currency), None)
            return found

    def get_pause(self, currency):
        with self._lock:
            return self._get(("pause", currency))

    def write_status(self, currency, status):
        with self._lock:
            self._set(("status", currency), dict(status))

    def read_status(self, currency):
        with self._lock:
            return self._get(("status", currency))

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

    def claim(self, currency, owner):
        key = self._k("claim", currency)
        if self._r.set(key, owner, nx=True, ex=CLAIM_TTL_SECONDS):
            return True
        return bool(self._extend(keys=[key], args=[owner, CLAIM_TTL_SECONDS]))

    def release(self, currency, owner):
        self._delete(keys=[self._k("claim", currency)], args=[owner])

    def claim_owner(self, currency):
        return self._r.get(self._k("claim", currency))

    def pause(self, currency, reason, by, seconds=None):
        self._r.set(
            self._k("pause", currency),
            json.dumps(_pause_record(reason, by, seconds)),
            ex=seconds,
        )

    def resume(self, currency):
        return bool(self._r.delete(self._k("pause", currency)))

    def get_pause(self, currency):
        return _loads(self._r.get(self._k("pause", currency)))

    def write_status(self, currency, status):
        self._r.set(self._k("status", currency), json.dumps(status))

    def read_status(self, currency):
        return _loads(self._r.get(self._k("status", currency)))

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
