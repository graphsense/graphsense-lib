"""SlackBurstHandler: ERROR lines reach Slack without blocking, collapsed.

A burst of the same error (here: Cassandra NoHostAvailable from every
request) used to post one Slack message per request from inside the logging
call, blocking the event loop each time. Now: the first one at once, repeats
counted into one summary per window, and emit() only enqueues.
"""

import logging
import threading
import time

from graphsenselib.utils.slack_aggregation import SlackBurstHandler, event_key

WINDOW = 300
T0 = 1_800_000_000 - (1_800_000_000 % WINDOW)  # start of a window


def _tb(exc="cassandra.cluster.NoHostAvailable"):
    return (
        "Traceback (most recent call last):\n"
        + "".join(f'  File "/app/mod{i}.py", line {i}, in f\n' for i in range(60))
        + f"{exc}: ('Unable to complete the operation against any hosts', {{}})"
    )


def _text(n=0, user="alice", exc="cassandra.cluster.NoHostAvailable"):
    return (
        f"ERROR graphsenselib.web.app: Unhandled exception | URL: "
        f"http://api.example/btc/clusters/{288831162 + n}/links?neighbor={n} "
        f"| User: {user}\n{_tb(exc)}"
    )


def _record(msg, created):
    r = logging.makeLogRecord(
        {"name": "graphsenselib.web.app", "levelno": logging.ERROR, "msg": msg}
    )
    r.levelname = "ERROR"
    r.created = created
    return r


def _handler(sent, now, **kw):
    h = SlackBurstHandler(
        "https://hooks.slack.test/x",
        environment="prod",
        window_s=WINDOW,
        send=sent.append,
        clock=lambda: now[0],
        start=False,
        **kw,
    )
    h.setFormatter(logging.Formatter("%(message)s"))
    return h


def test_same_error_with_other_ids_and_users_is_one_key():
    a, _ = event_key(_text(1, "alice"))
    b, _ = event_key(_text(2, "bob"))
    assert a == b
    assert "NoHostAvailable" in a and "/btc/clusters/{id}/links" in a
    assert "288831" not in a and "alice" not in a and "neighbor" not in a


def test_other_exception_type_is_another_key():
    a, _ = event_key(_text(1))
    b, _ = event_key(_text(1, exc="ValueError"))
    assert a != b


def test_a_burst_gives_one_message_now_and_one_summary_at_window_end():
    sent, now = [], [T0 + 10]
    h = _handler(sent, now)
    for i in range(500):
        h.emit(_record(_text(i, f"user{i % 7}"), T0 + 10 + i * 0.1))
    h.process()
    assert len(sent) == 1, "only the first one is posted at once"
    assert sent[0].startswith("[prod] ")
    assert "NoHostAvailable" in sent[0]

    now[0] = T0 + WINDOW + 1
    h.process()
    assert len(sent) == 2
    assert "499x more" in sent[1]
    assert "NoHostAvailable" in sent[1]


def test_two_different_errors_are_two_messages():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now)
    h.emit(_record(_text(1), T0 + 1))
    h.emit(_record(_text(2, exc="KeyError"), T0 + 2))
    h.emit(_record(_text(3, exc="KeyError"), T0 + 3))
    h.process()
    assert len(sent) == 2
    now[0] = T0 + WINDOW
    h.process()
    assert len(sent) == 3 and "1x more" in sent[2] and "KeyError" in sent[2]


def test_a_single_error_gets_no_summary():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now)
    h.emit(_record(_text(1), T0 + 1))
    h.process()
    now[0] = T0 + 2 * WINDOW
    h.process()
    assert len(sent) == 1


def test_the_traceback_is_shortened_to_its_last_lines():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now, traceback_lines=5)
    h.emit(_record(_text(1), T0 + 1))
    h.process()
    body = sent[0]
    assert "lines skipped" in body
    assert "mod59.py" in body and "mod10.py" not in body
    assert body.rstrip().count("\n") < 15


def test_window_zero_posts_every_line():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now)
    h.window_s = 0
    for i in range(5):
        h.emit(_record(_text(i), T0 + 1))
    h.process()
    assert len(sent) == 5


def test_a_full_queue_is_counted_and_reported():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now, queue_size=3)
    for i in range(10):
        h.emit(_record(_text(i), T0 + 1))
    h.process()
    assert any("7 error log line(s) were not sent" in m for m in sent)


def test_emit_does_not_block_when_slack_hangs():
    release = threading.Event()

    def slow_send(text):
        release.wait(10)  # a webhook that does not answer

    h = SlackBurstHandler("https://hooks.slack.test/x", window_s=WINDOW, send=slow_send)
    h.setFormatter(logging.Formatter("%(message)s"))
    try:
        start = time.perf_counter()
        for i in range(200):
            h.emit(_record(_text(i, exc=f"E{i}Error"), time.time()))
        assert time.perf_counter() - start < 0.5
    finally:
        release.set()
        h.close()


def test_close_posts_pending_summaries():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now)
    for i in range(3):
        h.emit(_record(_text(i), T0 + 1))
    h.process()
    h.close()
    assert len(sent) == 2 and "2x more" in sent[1]


class FakeCoordinator:
    """In-memory stand-in for RedisBurstCoordinator, shared by 'workers'."""

    def __init__(self):
        self.owners, self.counts = set(), {}

    def claim(self, key, bucket, ttl_s):
        if (key, bucket) in self.owners:
            return False
        self.owners.add((key, bucket))
        return True

    def add(self, key, bucket, n, ttl_s):
        self.counts[(key, bucket)] = self.counts.get((key, bucket), 0) + n

    def take(self, key, bucket):
        return self.counts.pop((key, bucket), 0)


def test_workers_share_one_first_message_and_one_total():
    sent, now = [], [T0 + 1]
    coord = FakeCoordinator()
    workers = [_handler(sent, now, coordinator=coord) for _ in range(3)]
    for w, n in zip(workers, (4, 3, 5)):
        for i in range(n):
            w.emit(_record(_text(i), T0 + 1 + i))
        w.process()
    assert len(sent) == 1, "one worker posts the first message"

    now[0] = T0 + WINDOW + 1  # window over: every worker adds its count
    for w in workers:
        w.process()
    assert len(sent) == 1, "the owner waits for the others (grace)"

    now[0] = T0 + WINDOW + 20
    for w in workers:
        w.process()
    assert len(sent) == 2
    assert "11x more" in sent[1]  # 12 in total, one of them already posted


class BrokenCoordinator:
    def claim(self, *a):
        raise ConnectionError("redis down")

    add = take = claim


def test_a_failing_coordinator_falls_back_to_posting_alone():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now, coordinator=BrokenCoordinator())
    for i in range(3):
        h.emit(_record(_text(i), T0 + 1))
    h.process()
    now[0] = T0 + WINDOW + 30
    h.process()
    assert len(sent) == 2 and "2x more" in sent[1]


def test_the_handler_ignores_its_own_warnings():
    sent, now = [], [T0 + 1]
    h = _handler(sent, now)
    r = _record("Slack webhook answered 500", T0 + 1)
    r.name = "graphsenselib.utils.slack_aggregation"
    h.emit(r)
    h.process()
    assert sent == []


class _MiniRedis:
    """Just enough of redis.Redis for RedisBurstCoordinator."""

    def __init__(self):
        self.data, self.ttl = {}, {}

    def set(self, name, value, nx=False, ex=None):
        if nx and name in self.data:
            return None
        self.data[name], self.ttl[name] = value, ex
        return True

    def pipeline(self):
        return _MiniPipe(self)


class _MiniPipe:
    def __init__(self, r):
        self.r, self.ops = r, []

    def __getattr__(self, op):
        return lambda *a: self.ops.append((op, a))

    def execute(self):
        out = []
        for op, a in self.ops:
            d = self.r.data
            if op == "incrby":
                d[a[0]] = int(d.get(a[0], 0)) + a[1]
                out.append(d[a[0]])
            elif op == "expire":
                self.r.ttl[a[0]] = a[1]
                out.append(True)
            elif op == "get":
                out.append(d.get(a[0]))
            elif op == "delete":
                out.append(int(d.pop(a[0], None) is not None))
        return out


def test_redis_coordinator_claims_once_and_sums_counts():
    from graphsenselib.utils.slack_aggregation import RedisBurstCoordinator

    c = RedisBurstCoordinator.__new__(RedisBurstCoordinator)
    mini = _MiniRedis()
    c.redis = mini  # ty: ignore[invalid-assignment]
    c.prefix = "gs:slack:prod"
    assert c.claim("k", 7, 615) is True
    assert c.claim("k", 7, 615) is False
    assert c.claim("k", 8, 615) is True  # next window: a new first message
    c.add("k", 7, 4, 615)
    c.add("k", 7, 0, 615)  # nothing to add: no write
    c.add("k", 7, 3, 615)
    assert c.take("k", 7) == 7
    assert c.take("k", 7) == 0  # reported once
    keys = list(mini.data)
    assert all(k.startswith("gs:slack:prod:") for k in keys)
    assert all(mini.ttl[k] == 615 for k in keys)
