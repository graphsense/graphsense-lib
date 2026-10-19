import pytest

from graphsenselib.sync.state import (
    CLAIM_TTL_SECONDS,
    FileStateStore,
    MemoryStateStore,
)


class Clock:
    t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture(params=["memory", "file"])
def store_and_clock(request, tmp_path):
    clock = Clock()
    if request.param == "memory":
        return MemoryStateStore(clock=clock), clock
    return FileStateStore(str(tmp_path / "state"), clock=clock), clock


def test_claims_are_exclusive_and_expire(store_and_clock):
    store, clock = store_and_clock
    assert store.claim("zec", "a")
    assert store.claim("zec", "a")  # extending our own claim
    assert not store.claim("zec", "b")
    store.release("zec", "b")  # not b's to release
    assert store.claim_owner("zec") == "a"

    clock.t += CLAIM_TTL_SECONDS + 1  # daemon a died
    assert store.claim("zec", "b")
    store.release("zec", "b")
    assert store.claim_owner("zec") is None


def test_pause_with_expiry(store_and_clock):
    store, clock = store_and_clock
    store.pause("zec", "maintenance", by="me", seconds=60)
    assert store.get_pause("zec")["reason"] == "maintenance"
    clock.t += 61
    assert store.get_pause("zec") is None
    assert not store.resume("zec")

    store.pause("zec", "until resumed", by="me")
    clock.t += 10**6
    assert store.resume("zec")
    assert store.get_pause("zec") is None


def test_status_and_daemons(store_and_clock):
    store, clock = store_and_clock
    store.write_status("zec", {"raw": 5})
    assert store.read_status("zec") == {"raw": 5}
    store.announce_daemon("abc123", {"channel": "stable"})
    assert store.list_daemons() == [("abc123", {"channel": "stable"})]
    store.forget_daemon("abc123")
    assert store.list_daemons() == []


def test_file_store_is_shared_between_instances(tmp_path):
    a = FileStateStore(str(tmp_path))
    b = FileStateStore(str(tmp_path))
    assert a.claim("zec", "daemon-a")
    assert not b.claim("zec", "daemon-b")
    b.pause("btc", "x", by="me")
    assert a.get_pause("btc")["reason"] == "x"
