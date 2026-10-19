"""The continuous sync loop behind `graphsense-cli sync run`.

Per currency, one *cycle* at a time:

    probe node tip / raw / transformed
    -> reorg check (newest raw block hashes vs. the node)
    -> ingest from-node   (bounded slice; with exchange rates once per interval)
    -> delta-update update
    -> consistency check  (optional, periodic)

then wait `poll_seconds` -- or start the next cycle at once while ingest is
still catching up. Currencies run in parallel, each cycle on its own thread;
the steps themselves are child processes (see jobs.py).

Within a currency the steps must run one after the other anyway: ingest and
delta-update take the same raw-keyspace lock.

delta-update gets no --end-block on purpose: an explicit range turns off its
check for a torn bookkeeping write (`auto_resume` in deltaupdater.update), so
only ingest is cut into slices. delta-update cannot run ahead of ingest, so its
run is as long as the raw backlog.
"""

import logging
import signal
import socket
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from .. import __version__
from ..config import (
    Environment,
    KeyspaceConfig,
    SyncConfig,
    SyncCurrencyConfig,
    avg_blocktimes_by_currencies,
    get_reorg_backoff_blocks,
)
from .jobs import EXIT_INCONSISTENT, JobRunner, Outcome, StepResult
from .probe import ChainProbe
from .state import StateStore, new_daemon_id

logger = logging.getLogger(__name__)

# Default ingest slice: about this much chain time per job.
_DEFAULT_SLICE_SECONDS = 2 * 60 * 60
_ALERT_EVERY_SECONDS = 60 * 60
_PAUSE_REMINDER_AFTER_SECONDS = 24 * 60 * 60
_BUSY_ALERT_AFTER_SECONDS = 30 * 60
# Upper bound on threads; how many cycles actually run at once is
# sync.max_parallel_currencies, re-read on every config reload.
_MAX_WORKER_THREADS = 64
_TICK_SECONDS = 1.0

Notifier = Callable[[str, str, str, int], object]


def default_max_blocks(currency: str) -> int:
    return max(1, _DEFAULT_SLICE_SECONDS // avg_blocktimes_by_currencies[currency])


def _default_notifier(topic: str, msg: str, dedup_key: str, every_seconds: int):
    from ..monitoring.notifications import send_msg_to_topic_rate_limited

    return send_msg_to_topic_rate_limited(topic, msg, dedup_key, every_seconds)


@dataclass
class _Runtime:
    """What the scheduler remembers about one currency between cycles."""

    next_due: float = 0.0  # monotonic
    failures: int = 0
    alerted: bool = False
    busy_since: Optional[float] = None  # monotonic
    last_rates_at: float = 0.0  # wall clock
    last_consistency_at: float = 0.0  # wall clock
    future: Optional[Future] = None
    claimed: bool = False
    last_skip_reason: Optional[str] = None


class SyncScheduler:
    def __init__(
        self,
        env: str,
        channel: str,
        store: StateStore,
        load_environment: Callable[[], Environment],
        runner: Optional[JobRunner] = None,
        only: Optional[List[str]] = None,
        notify: Notifier = _default_notifier,
        probe_factory: Callable[[str, str, KeyspaceConfig], ChainProbe] = ChainProbe,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
    ):
        self.env = env
        self.channel = channel
        self.store = store
        self._load_environment = load_environment
        self.runner = runner or JobRunner()
        self.only = set(only) if only else None
        self._notify = notify
        self._probe_factory = probe_factory
        self._clock = clock
        self._wall = wall

        self.daemon_id = new_daemon_id(channel)
        self._daemon_key = uuid.uuid4().hex[:12]
        self._started_at = int(wall())
        self._stop = threading.Event()
        self._reload_requested = threading.Event()
        self._runtime: Dict[str, _Runtime] = {}
        self._probes: Dict[str, ChainProbe] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=_MAX_WORKER_THREADS, thread_name_prefix="sync-cycle"
        )
        self._environment: Optional[Environment] = None
        self._delta_slots_size = 0
        self._delta_slots = threading.BoundedSemaphore(1)
        self._config_error_reported = False
        self._once = False

    # -- lifecycle ---------------------------------------------------------

    def stop(self):
        self._stop.set()

    def request_reload(self):
        self._reload_requested.set()

    def run(self, once: bool = False) -> int:
        """Run until stopped (SIGTERM/SIGINT), or one cycle per currency with
        ``once``. Returns a process exit code."""
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, lambda *_: self.stop())
            signal.signal(signal.SIGINT, lambda *_: self.stop())
            if hasattr(signal, "SIGHUP"):
                signal.signal(signal.SIGHUP, lambda *_: self.request_reload())
        logger.info(
            f"sync daemon {self._daemon_key} started: env {self.env}, "
            f"channel {self.channel}, graphsense-lib {__version__}"
        )
        failed = False
        self._once = once
        try:
            if once:
                self.tick()
                for f in [rt.future for rt in self._runtime.values() if rt.future]:
                    failed |= bool(f.result())
            else:
                while not self._stop.is_set():
                    self.tick()
                    self._stop.wait(_TICK_SECONDS)
        finally:
            self._shutdown()
        return 1 if failed else 0

    def _shutdown(self):
        logger.info("sync daemon stopping, waiting for running jobs")
        self._stop.set()
        self._executor.shutdown(wait=True)
        for currency, rt in self._runtime.items():
            if rt.claimed:
                try:
                    self.store.release(currency, self.daemon_id)
                except Exception as e:
                    logger.warning(f"Could not release claim on {currency}: {e}")
        for probe in self._probes.values():
            probe.close()
        try:
            self.store.forget_daemon(self._daemon_key)
        except Exception as e:
            logger.debug(f"Could not remove daemon heartbeat: {e}")
        logger.info("sync daemon stopped")

    # -- scheduling --------------------------------------------------------

    def _environment_now(self) -> Optional[Environment]:
        try:
            env = self._load_environment()
            self._config_error_reported = False
        except Exception as e:
            # Keep running on the last good config.
            if not self._config_error_reported:
                logger.error(f"Could not reload config, keeping the old one: {e}")
                self._config_error_reported = True
            return self._environment
        self._environment = env
        return env

    def tick(self):
        if self._reload_requested.is_set():
            self._reload_requested.clear()
            logger.info("Reloading config (SIGHUP)")
        env = self._environment_now()
        if env is None or env.sync is None:
            if env is not None:
                logger.warning(f"No sync section in environment {self.env}")
            return
        sync = env.sync
        if sync.max_parallel_delta_updates != self._delta_slots_size:
            # New cycles use the new limit; running ones finish on the old one.
            self._delta_slots = threading.BoundedSemaphore(
                sync.max_parallel_delta_updates
            )
            self._delta_slots_size = sync.max_parallel_delta_updates

        now = self._clock()
        running = sum(1 for rt in self._runtime.values() if _is_running(rt))
        for currency, ccfg in sync.currencies.items():
            rt = self._runtime.setdefault(currency, _Runtime())
            try:
                if self._schedule_one(currency, ccfg, env, sync, rt, now, running):
                    running += 1
            except Exception as e:
                # e.g. Redis unreachable: try again next tick.
                logger.warning(f"Scheduling {currency} failed: {e}")
        self._announce(sync)

    def _schedule_one(
        self,
        currency: str,
        ccfg: SyncCurrencyConfig,
        env: Environment,
        sync: SyncConfig,
        rt: _Runtime,
        now: float,
        running: int,
    ) -> bool:
        """Start a cycle for the currency if it is ours and due. Returns
        whether one was started."""
        mine = (
            ccfg.enabled
            and ccfg.channel == self.channel
            and (self.only is None or currency in self.only)
        )
        if _is_running(rt):
            # Keep the claim alive while the cycle runs, even if the currency
            # was just moved away: the job is still going.
            rt.claimed = self.store.claim(currency, self.daemon_id)
            return False
        rt.future = None
        if not mine:
            if rt.claimed:
                self.store.release(currency, self.daemon_id)
                rt.claimed = False
                logger.info(
                    f"{currency}: no longer on channel {self.channel}, released"
                )
            return False
        if currency not in env.keyspaces:
            self._skip(rt, currency, f"not configured in environment {self.env}")
            return False

        pause = self.store.get_pause(currency)
        if pause is not None:
            self._skip(rt, currency, f"paused: {pause.get('reason')}")
            self._remind_paused(currency, pause, sync)
            if rt.claimed:
                rt.claimed = self.store.claim(currency, self.daemon_id)
            return False

        at_capacity = not self._once and running >= sync.max_parallel_currencies
        if now < rt.next_due or at_capacity:
            if rt.claimed:
                rt.claimed = self.store.claim(currency, self.daemon_id)
            return False

        if not self.store.claim(currency, self.daemon_id):
            rt.claimed = False
            owner = self.store.claim_owner(currency)
            self._skip(rt, currency, f"claimed by another daemon: {owner}")
            rt.next_due = now + (ccfg.poll_seconds or sync.poll_seconds)
            return False
        rt.claimed = True
        rt.last_skip_reason = None
        rt.future = self._executor.submit(
            self._run_cycle, currency, ccfg, env.keyspaces[currency], sync, rt
        )
        return True

    def _skip(self, rt: _Runtime, currency: str, reason: str):
        if rt.last_skip_reason != reason:
            logger.info(f"{currency}: skipped, {reason}")
            rt.last_skip_reason = reason

    def _announce(self, sync: SyncConfig):
        try:
            self.store.announce_daemon(
                self._daemon_key,
                {
                    "channel": self.channel,
                    "host": socket.gethostname(),
                    "version": __version__,
                    "started_at": self._started_at,
                    "seen_at": int(self._wall()),
                    "running": sorted(
                        c for c, rt in self._runtime.items() if _is_running(rt)
                    ),
                },
            )
        except Exception as e:
            logger.debug(f"Daemon heartbeat failed: {e}")

    def _remind_paused(self, currency: str, pause: dict, sync: SyncConfig):
        since = pause.get("since") or 0
        if pause.get("until") is None and self._wall() - since > (
            _PAUSE_REMINDER_AFTER_SECONDS
        ):
            self._alert(
                sync,
                f"sync-paused:{self.env}:{currency}",
                f"sync {self.env}/{currency} has been paused since "
                f"{_fmt_ts(since)} by {pause.get('by')}: {pause.get('reason')}. "
                f"Resume with: graphsense-cli sync resume -e {self.env} -c {currency}",
                every=_PAUSE_REMINDER_AFTER_SECONDS,
            )

    # -- one cycle ---------------------------------------------------------

    def _run_cycle(
        self,
        currency: str,
        ccfg: SyncCurrencyConfig,
        ks: KeyspaceConfig,
        sync: SyncConfig,
        rt: _Runtime,
    ) -> bool:
        """Returns whether the cycle failed."""
        status = {
            "channel": self.channel,
            "owner": self._daemon_key,
            "version": __version__,
            "cycle_started_at": int(self._wall()),
        }
        poll = ccfg.poll_seconds or sync.poll_seconds
        try:
            delay, failed = self._cycle(currency, ccfg, ks, sync, rt, status)
        except Exception as e:
            logger.exception(f"{currency}: cycle crashed")
            probe = self._probes.pop(currency, None)
            if probe is not None:
                probe.close()
            delay, failed = self._failure(currency, sync, rt, status, poll, str(e), [])
        rt.next_due = self._clock() + delay
        status["next_cycle_at"] = int(self._wall() + delay)
        status["failures"] = rt.failures
        try:
            self.store.write_status(currency, status)
        except Exception as e:
            logger.warning(f"{currency}: could not write status: {e}")
        return failed

    def _probe(self, currency: str, ks: KeyspaceConfig) -> ChainProbe:
        probe = self._probes.get(currency)
        if probe is None:
            probe = self._probe_factory(self.env, currency, ks)
            self._probes[currency] = probe
        return probe

    def _cycle(self, currency, ccfg, ks, sync, rt, status):
        poll = ccfg.poll_seconds or sync.poll_seconds
        probe = self._probe(currency, ks)
        try:
            tip = probe.node_tip()
            raw = probe.raw_height()
        except Exception as e:
            probe.reset()
            return self._failure(
                currency, sync, rt, status, poll, f"probe failed: {e}", []
            )
        ic = ks.ingest_config
        backoff = get_reorg_backoff_blocks(
            currency, override=ic.reorg_backoff_blocks if ic else None
        )
        target = tip - backoff
        status.update(node_tip=tip, target=target, raw=raw)
        if raw is None:
            return self._failure(
                currency,
                sync,
                rt,
                status,
                poll,
                "raw keyspace missing or empty; bootstrap it with a manual "
                "`ingest from-node --start-block ...` first",
                [],
            )

        if ccfg.reorg_check_depth > 0:
            mismatch = self._find_reorg(probe, raw, tip, ccfg.reorg_check_depth)
            if mismatch is not None:
                self._pause_for_reorg(currency, sync, mismatch, status)
                return poll, True

        self._check_lag(currency, ccfg, sync, probe, raw)

        # -- ingest
        rates_due = self._wall() - rt.last_rates_at >= sync.rates_every_minutes * 60
        catching_up = False
        if raw < target or rates_due:
            args = [
                "ingest",
                "from-node",
                "-e",
                self.env,
                "-c",
                currency,
                *ccfg.ingest_args,
                "--no-staleness-check",
            ]
            if not rates_due:
                args.append("--no-exchange-rates")
            end = None
            if raw < target:
                end = min(
                    target,
                    raw + (ccfg.max_blocks_per_job or default_max_blocks(currency)),
                )
                args += ["--end-block", str(end)]
            result = self._step(
                currency, "ingest", args, sync.ingest_timeout_minutes, status
            )
            outcome = self._handle(currency, sync, rt, status, poll, result)
            if outcome is not None:
                return outcome
            if rates_due:
                rt.last_rates_at = self._wall()
            catching_up = end is not None and end < target

        # -- delta update
        if ccfg.delta_update and not ks.disable_delta_updates:
            raw = probe.raw_height()
            transformed = probe.transformed_height()
            status.update(raw=raw, transformed=transformed)
            if transformed is None:
                return self._failure(
                    currency,
                    sync,
                    rt,
                    status,
                    poll,
                    "transformed keyspace missing; run the full transformation first",
                    [],
                )
            if raw is not None and transformed < raw:
                if not self._acquire_delta_slot():
                    return 0, False  # stopping
                try:
                    result = self._step(
                        currency,
                        "delta_update",
                        ["delta-update", "update", "-e", self.env, "-c", currency]
                        + ccfg.delta_update_args,
                        sync.delta_update_timeout_minutes,
                        status,
                    )
                finally:
                    self._delta_slots.release()
                outcome = self._handle(currency, sync, rt, status, poll, result)
                if outcome is not None:
                    return outcome
                status["transformed"] = probe.transformed_height()

        # -- consistency check
        every = sync.consistency_check_every_minutes
        if every is not None and self._wall() - rt.last_consistency_at >= every * 60:
            result = self._step(
                currency,
                "consistency",
                ["monitoring", "check-consistency", "-e", self.env, "-c", currency]
                + sync.consistency_check_args,
                sync.delta_update_timeout_minutes,
                status,
            )
            if result.outcome is Outcome.STOPPED:
                return 0, False
            if result.outcome is not Outcome.BUSY:
                rt.last_consistency_at = self._wall()
            if result.exit_code == EXIT_INCONSISTENT:
                self._alert(
                    sync,
                    f"sync-inconsistent:{self.env}:{currency}",
                    f"sync {self.env}/{currency}: consistency check failed.\n"
                    + _fmt_tail(result.tail),
                )
            elif result.failed:
                return self._failure(
                    currency, sync, rt, status, poll, _describe(result), result.tail
                )

        self._success(currency, sync, rt, status)
        return (0.0 if catching_up else poll), False

    def _step(self, currency, step, args, timeout_minutes, status) -> StepResult:
        status["running"] = step
        try:
            self.store.write_status(currency, status)
        except Exception:
            pass
        result = self.runner.run(
            step, args, timeout_minutes * 60, self._stop, label=f"{currency} {step}"
        )
        status.pop("running", None)
        status.setdefault("steps", {})[step] = {
            "outcome": result.outcome.value,
            "exit_code": result.exit_code,
            "seconds": round(result.seconds, 1),
            "at": int(self._wall()),
        }
        return result

    def _handle(self, currency, sync, rt, status, poll, result: StepResult):
        """Turn a step result into an early cycle return, or None to go on."""
        if result.outcome is Outcome.STOPPED:
            return 0, False
        if result.failed:
            return self._failure(
                currency, sync, rt, status, poll, _describe(result), result.tail
            )
        if result.outcome is Outcome.BUSY:
            now = self._clock()
            rt.busy_since = rt.busy_since or now
            if now - rt.busy_since > _BUSY_ALERT_AFTER_SECONDS:
                self._alert(
                    sync,
                    f"sync-busy:{self.env}:{currency}",
                    f"sync {self.env}/{currency}: `{result.step}` has found its "
                    f"lock taken for {int((now - rt.busy_since) // 60)} min. "
                    "Is a manual run going on, or did a killed job leave the "
                    "lock behind? The job log names the holder.\n"
                    + _fmt_tail(result.tail),
                )
            status["last_result"] = f"{result.step}: lock busy"
            return poll, False
        rt.busy_since = None
        return None

    def _acquire_delta_slot(self) -> bool:
        while not self._stop.is_set():
            if self._delta_slots.acquire(timeout=1.0):
                return True
        return False

    def _success(self, currency, sync, rt, status):
        if rt.alerted:
            self._notify(
                sync.alert_topic,
                f"sync {self.env}/{currency} recovered after {rt.failures} "
                "failed cycles.",
                f"sync-recovered:{self.env}:{currency}:{uuid.uuid4().hex}",
                60,
            )
        rt.failures = 0
        rt.alerted = False
        status["last_result"] = "ok"
        status["last_success_at"] = int(self._wall())

    def _failure(self, currency, sync, rt, status, poll, reason, tail):
        rt.failures += 1
        delay = min(poll * 2 ** (rt.failures - 1), sync.max_backoff_minutes * 60)
        logger.error(
            f"{currency}: cycle failed ({rt.failures} in a row), retry in "
            f"{delay:.0f}s: {reason}"
        )
        status["last_result"] = f"failed: {reason}"
        status["last_failure_at"] = int(self._wall())
        if rt.failures >= sync.failure_alert_after:
            self._alert(
                sync,
                f"sync-failure:{self.env}:{currency}",
                f"sync {self.env}/{currency} failed {rt.failures} times in a "
                f"row (channel {self.channel}, graphsense-lib {__version__}). "
                f"Last error: {reason}\n" + _fmt_tail(tail),
            )
            rt.alerted = True
        return delay, True

    def _alert(self, sync, key, msg, every=_ALERT_EVERY_SECONDS):
        logger.warning(msg)
        try:
            self._notify(sync.alert_topic, msg, key, every)
        except Exception as e:
            logger.warning(f"Could not send notification: {e}")

    # -- checks ------------------------------------------------------------

    def _find_reorg(self, probe, raw: int, tip: int, depth: int) -> Optional[dict]:
        """First of the newest `depth` raw blocks whose hash the node no longer
        has at that height, or None."""
        for height in range(raw, max(raw - depth, -1), -1):
            if height > tip:
                continue  # node behind us (restart/resync): nothing to compare
            ours = probe.raw_block_hash(height)
            if ours is None:
                continue
            theirs = probe.node_block_hash(height)
            if theirs is not None and theirs != ours:
                return {"height": height, "raw": ours, "node": theirs}
        return None

    def _pause_for_reorg(self, currency, sync, mismatch, status):
        reason = (
            f"reorg detected: block {mismatch['height']} is {mismatch['raw']} in "
            f"raw but {mismatch['node']} at the node"
        )
        self.store.pause(currency, reason, by=f"sync daemon {self._daemon_key}")
        status["last_result"] = reason
        self._alert(
            sync,
            f"sync-reorg:{self.env}:{currency}",
            f"sync {self.env}/{currency} PAUSED: {reason}. The reorg went "
            "deeper than the reorg backoff, so raw (and possibly transformed) "
            "holds orphaned blocks. Repair them, then resume with: "
            f"graphsense-cli sync resume -e {self.env} -c {currency}",
        )

    def _check_lag(self, currency, ccfg, sync, probe, raw):
        if ccfg.lag_alert_minutes is None:
            return
        try:
            ts = probe.raw_block_timestamp(raw)
        except Exception as e:
            logger.debug(f"{currency}: lag check skipped: {e}")
            return
        if ts is None:
            return
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_min = (
            datetime.fromtimestamp(self._wall(), timezone.utc) - ts
        ).total_seconds() / 60
        if age_min > ccfg.lag_alert_minutes:
            self._alert(
                sync,
                f"sync-lag:{self.env}:{currency}",
                f"sync {self.env}/{currency}: newest raw block {raw} is "
                f"{age_min:.0f} min old (alert above {ccfg.lag_alert_minutes} min).",
            )


def _is_running(rt: _Runtime) -> bool:
    return rt.future is not None and not rt.future.done()


def _describe(result: StepResult) -> str:
    if result.outcome is Outcome.TIMEOUT:
        return f"{result.step} timed out after {result.seconds / 60:.0f} min"
    return f"{result.step} exited with {result.exit_code}"


def _fmt_tail(tail: List[str], n: int = 10) -> str:
    if not tail:
        return ""
    return "```\n" + "\n".join(tail[-n:]) + "\n```"


def _fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
