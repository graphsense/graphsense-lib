from graphsenselib.sync.state import CLAIM_TTL_SECONDS, MemoryStateStore


class Clock:
    t = 1000.0

    def __call__(self):
        return self.t


def test_claims_are_exclusive_and_expire():
    clock = Clock()
    store = MemoryStateStore(clock=clock)
    assert store.claim("zec", "a")
    assert store.claim("zec", "a")  # extending our own claim
    assert not store.claim("zec", "b")
    store.release("zec", "b")  # not b's to release
    assert store.claim_owner("zec") == "a"

    clock.t += CLAIM_TTL_SECONDS + 1  # daemon a died
    assert store.claim("zec", "b")


def test_pause_with_expiry():
    clock = Clock()
    store = MemoryStateStore(clock=clock)
    store.pause("zec", "maintenance", by="me", seconds=60)
    assert store.get_pause("zec")["reason"] == "maintenance"
    clock.t += 61
    assert store.get_pause("zec") is None
    assert not store.resume("zec")
