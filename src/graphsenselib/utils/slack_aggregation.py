"""Slack notifications for ERROR log lines: non-blocking and collapsed.

The REST app used to post every ERROR line to Slack with a synchronous
``requests.post`` from inside the logging call. During an outage that meant
hundreds of messages (one per failed request and worker, each with a full
traceback) and, worse, every one of them blocked the worker's event loop for
up to the Slack timeout, exactly when the worker was struggling already.

``SlackBurstHandler`` instead:

- only enqueues in ``emit`` (bounded queue; a full queue is counted, not
  blocking); a sender thread per process does the HTTP calls;
- groups records by an *event key*: the exception type plus the first line
  with ids, addresses, numbers, query strings and the user removed. The first
  record of a key in a time bucket (``window_s``, aligned to the clock) is
  posted at once, with a shortened traceback; repeats in the same bucket are
  only counted and posted as one summary when the bucket ends;
- optionally coordinates the workers of one deployment through Redis
  (``RedisBurstCoordinator``): one worker claims the first message of a
  key and bucket, every worker adds its count, the claiming one posts the
  total. Without Redis (or when Redis fails) each process acts alone.

A long outage thus yields two messages per key and window instead of one per
request.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# (connect, read) timeout for one webhook post; runs in the sender thread.
_POST_TIMEOUT = (3.05, 10)
# Slack shows long messages collapsed anyway; keep them readable.
_MAX_MESSAGE_CHARS = 3500
# The longest Retry-After we honour for a 429 from Slack.
_MAX_RETRY_AFTER_S = 60

_EXC_LINE = re.compile(
    r"^((?:[A-Za-z_]\w*\.)*(?:[A-Z]\w*)?"
    r"(?:Error|Exception|Available|TimedOut|Timeout|Busy|Shutdown|Exit|Interrupt))"
    r"\b"
)
_URL = re.compile(r"\bURL: \S+")
_USER = re.compile(r"\s*\|\s*User: \S*")
_LONG_TOKEN = re.compile(r"\b(?:0x)?[0-9A-Za-z]{16,}\b")
_NUMBER = re.compile(r"\b\d+\b")


def _normalize_path(url: str) -> str:
    """Path of a URL with id-like segments as {id} and no query string."""
    path = re.sub(r"^[a-z]+://[^/]+", "", url).split("?", 1)[0]
    segments = [
        "{id}" if (any(c.isdigit() for c in s) or len(s) >= 20) else s
        for s in path.split("/")
    ]
    return "/".join(segments) or "/"


def event_key(text: str) -> Tuple[str, str]:
    """(key, label) of a formatted log record.

    The key groups records that are "the same error": same exception type
    and same first line once ids, addresses, numbers, the query string and
    the user are taken out. The label is the human-readable form of it.
    """
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return "", "(empty log line)"
    first = lines[0]
    first = _URL.sub(lambda m: "URL: " + _normalize_path(m.group(0)[5:]), first)
    first = _USER.sub("", first)
    first = _LONG_TOKEN.sub("{id}", first)
    first = _NUMBER.sub("N", first).strip()
    exc = ""
    for line in reversed(lines[1:]):
        m = _EXC_LINE.match(line.strip())
        if m:
            exc = m.group(1)
            break
    label = f"{exc} | {first}" if exc else first
    return label, label[:200]


@dataclass
class _Bucket:
    label: str
    first_ts: float
    owner: bool
    local: int = 0  # occurrences counted here and not yet added to the total
    pushed: bool = False


@dataclass
class _Stats:
    dropped: int = 0  # records lost to a full queue since the last report
    last_dropped_report: float = 0.0
    send_failures: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class RedisBurstCoordinator:
    """Shares claims and counts of one deployment's workers through Redis.

    Keys live under ``<prefix>:<environment>:<hash of the event key>:<bucket>``
    and expire on their own. Every method raises on Redis errors; the handler
    then falls back to acting alone.
    """

    def __init__(self, url: str, environment: Optional[str], prefix="gs:slack"):
        import redis  # sync client: runs in the sender thread

        self.redis = redis.Redis.from_url(
            url, socket_timeout=2, socket_connect_timeout=2
        )
        self.prefix = f"{prefix}:{environment or 'default'}"

    def _base(self, key: str, bucket: int) -> str:
        digest = hashlib.sha1(key.encode(), usedforsecurity=False).hexdigest()[:16]
        return f"{self.prefix}:{digest}:{bucket}"

    def claim(self, key: str, bucket: int, ttl_s: int) -> bool:
        """True for the first worker to see this key in this bucket."""
        return bool(
            self.redis.set(
                self._base(key, bucket) + ":owner", os.getpid(), nx=True, ex=ttl_s
            )
        )

    def add(self, key: str, bucket: int, n: int, ttl_s: int) -> None:
        if n <= 0:
            return
        name = self._base(key, bucket) + ":count"
        pipe = self.redis.pipeline()
        pipe.incrby(name, n)
        pipe.expire(name, ttl_s)
        pipe.execute()

    def take(self, key: str, bucket: int) -> int:
        """The total added by all workers, removed so it is reported once."""
        name = self._base(key, bucket) + ":count"
        pipe = self.redis.pipeline()
        pipe.get(name)
        pipe.delete(name)
        value, _ = pipe.execute()
        return int(value or 0)


class SlackBurstHandler(logging.Handler):
    """Posts ERROR records to a Slack webhook without blocking, collapsed.

    ``window_s`` <= 0 posts every record (still from the sender thread).
    ``send``, ``clock`` and ``start=False`` exist for tests: with
    ``start=False`` nothing runs in the background and ``process()`` does one
    round of the sender loop.
    """

    def __init__(
        self,
        webhook_url: str,
        environment: Optional[str] = None,
        window_s: int = 300,
        traceback_lines: int = 30,
        queue_size: int = 1000,
        coordinator: Optional[RedisBurstCoordinator] = None,
        grace_s: int = 15,
        send: Optional[Callable[[str], None]] = None,
        clock: Callable[[], float] = time.time,
        start: bool = True,
    ):
        super().__init__()
        self.webhook_url = webhook_url
        self.environment = environment
        self.window_s = window_s
        self.traceback_lines = traceback_lines
        self.queue_size = queue_size
        self.coordinator = coordinator
        self.grace_s = grace_s
        self._send = send or self._post
        self.clock = clock
        self._buckets: Dict[Tuple[str, int], _Bucket] = {}
        self._stats = _Stats()
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._pid = os.getpid()
        self._start = start
        if start:
            self._start_thread()

    # ------------------------------------------------------------- producer
    def emit(self, record: logging.LogRecord) -> None:
        # never react to our own warnings (a handler on the root logger
        # would otherwise feed its failures back into itself)
        if record.name == logger.name:
            return
        try:
            if self._start and os.getpid() != self._pid:
                self._after_fork()
            text = self.format(record)
            self._queue.put_nowait((record.created, text))
        except queue.Full:
            with self._stats.lock:
                self._stats.dropped += 1
        except Exception:
            self.handleError(record)

    def _after_fork(self) -> None:
        # threads do not survive fork(): start over in the child
        self._pid = os.getpid()
        self._queue = queue.Queue(maxsize=self.queue_size)
        self._buckets = {}
        self._stats = _Stats()
        self._stop = threading.Event()
        self._start_thread()

    # --------------------------------------------------------------- sender
    def _start_thread(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="slack-burst-sender", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self.process(block_s=1.0)
        self.process(block_s=0, final=True)

    def process(self, block_s: float = 0, final: bool = False) -> None:
        """One round: take queued records, then end the expired buckets."""
        try:
            item = self._queue.get(timeout=block_s) if block_s else None
        except queue.Empty:
            item = None
        items = [item] if item else []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except queue.Empty:
                break
        for created, text in items:
            self._safely(self._ingest, created, text)
        self._safely(self._tick, final)

    def _safely(self, fn, *args) -> None:
        try:
            fn(*args)
        except Exception as e:  # the sender thread must survive anything
            logger.warning("Slack notification step failed: %r", e)

    def _ttl(self) -> int:
        return int(self.window_s * 2 + self.grace_s)

    def _ingest(self, created: float, text: str) -> None:
        if self.window_s <= 0:
            self._send(self._first_message(text, None))
            return
        key, label = event_key(text)
        bucket = int(created // self.window_s)
        b = self._buckets.get((key, bucket))
        if b is not None:
            b.local += 1
            return
        owner = True
        if self.coordinator is not None:
            try:
                owner = self.coordinator.claim(key, bucket, self._ttl())
            except Exception as e:
                logger.warning("Slack coordination (claim) failed: %r", e)
        self._buckets[(key, bucket)] = _Bucket(label, created, owner)
        if owner:
            self._send(self._first_message(text, bucket))
        else:
            self._buckets[(key, bucket)].local += 1

    def _tick(self, final: bool) -> None:
        now = self.clock()
        for (key, bucket), b in list(self._buckets.items()):
            end = (bucket + 1) * self.window_s
            if now < end and not final:
                continue
            total = b.local
            if self.coordinator is not None:
                try:
                    if not b.pushed:
                        self.coordinator.add(key, bucket, b.local, self._ttl())
                        b.pushed, b.local = True, 0
                    if not b.owner:
                        del self._buckets[(key, bucket)]
                        continue
                    if now < end + self.grace_s and not final:
                        continue  # let the other workers add their counts
                    total = self.coordinator.take(key, bucket)
                except Exception as e:
                    logger.warning("Slack coordination (count) failed: %r", e)
                    total = b.local  # whatever this worker still knows
            del self._buckets[(key, bucket)]
            if total > 0 and b.owner:
                self._send(self._summary(b, bucket, total))
        self._report_dropped(now, final)

    def _report_dropped(self, now: float, final: bool) -> None:
        with self._stats.lock:
            dropped = self._stats.dropped
            due = final or now - self._stats.last_dropped_report >= max(
                self.window_s, 60
            )
            if not dropped or not due:
                return
            self._stats.dropped = 0
            self._stats.last_dropped_report = now
        self._send(
            f"{self._prefix()}{dropped} error log line(s) were not sent to Slack "
            "(notification queue full); see the logs."
        )

    # ------------------------------------------------------------- messages
    def _prefix(self) -> str:
        return f"[{self.environment}] " if self.environment else ""

    def _first_message(self, text: str, bucket: Optional[int]) -> str:
        lines = text.splitlines()
        head, rest = (lines[0], lines[1:]) if lines else ("", [])
        if len(rest) > self.traceback_lines:
            skipped = len(rest) - self.traceback_lines
            rest = [f"... ({skipped} lines skipped)"] + rest[-self.traceback_lines :]
        body = f"{self._prefix()}{head}"
        if rest:
            body += "\n```\n" + "\n".join(rest) + "\n```"
        if bucket is not None:
            until = datetime.fromtimestamp(
                (bucket + 1) * self.window_s, timezone.utc
            ).strftime("%H:%M")
            body += (
                f"\n_Repeats until {until} UTC are counted and posted as one summary._"
            )
        if len(body) > _MAX_MESSAGE_CHARS:
            body = body[: _MAX_MESSAGE_CHARS - 20] + "\n... (truncated)"
        return body

    def _summary(self, b: _Bucket, bucket: int, total: int) -> str:
        def hm(ts: float) -> str:
            return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M")

        start, end = bucket * self.window_s, (bucket + 1) * self.window_s
        return (
            f"{self._prefix()}{total}x more between {hm(start)} and {hm(end)} "
            f"UTC (first one posted at {hm(b.first_ts)}): {b.label}"
        )

    def _post(self, text: str) -> None:
        for attempt in (1, 2):
            r = requests.post(
                self.webhook_url, json={"text": text}, timeout=_POST_TIMEOUT
            )
            if r.status_code == 429 and attempt == 1:
                try:
                    wait = float(r.headers.get("Retry-After", "1"))
                except ValueError:
                    wait = 1.0
                time.sleep(min(max(wait, 0.0), _MAX_RETRY_AFTER_S))
                continue
            if r.status_code >= 400:
                self._stats.send_failures += 1
                logger.warning("Slack webhook answered %s", r.status_code)
            return

    # -------------------------------------------------------------- closing
    def close(self) -> None:
        """Flush: post the pending summaries, then stop the sender thread."""
        try:
            if self._thread is not None and self._thread.is_alive():
                self._stop.set()
                self._thread.join(timeout=10)
            elif not self._start:
                self.process(final=True)
        finally:
            super().close()
