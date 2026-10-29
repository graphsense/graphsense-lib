"""The continuous sync loop behind `graphsense-cli sync run`.

Per network, one *cycle* at a time:

    probe node tip / raw / transformed
    -> reorg check (newest raw block hashes vs. the node)
    -> ingest from-node   (bounded slice; with exchange rates once per interval)
    -> delta-update update
    -> Delta Lake compaction check (if ingest_config.auto_compact is set)
    -> consistency check  (optional, periodic)

then wait `poll_seconds` -- or start the next cycle at once while ingest is
still catching up. Networks run in parallel, each cycle on its own thread;
the steps themselves are child processes (see jobs.py).

Within a network the steps must run one after the other anyway: ingest and
delta-update take the same raw-keyspace lock.

delta-update gets no --end-block on purpose: an explicit range turns off its
check for a torn bookkeeping write (`auto_resume` in deltaupdater.update), so
only ingest is cut into slices. delta-update cannot run ahead of ingest, so its
run is as long as the raw backlog.

Compaction is a step of its own rather than `--auto-compact` on the ingest:
a compaction can take hours, far beyond the ingest timeout.

Beside the networks, `sync.tasks` are jobs started at a time of day (e.g. the
cross-network pubkey update). A task runs on one daemon at a time: the one
that claims it, among those on its channel without -c. Its last run is kept in
the state store, so a restart neither repeats nor drops it.
"""

import logging
import signal
import socket
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple

from .. import __version__
from ..config import (
    Environment,
    KeyspaceConfig,
    SyncConfig,
    SyncNetworkConfig,
    SyncTaskConfig,
    avg_blocktimes_by_currencies,
    get_config,
    get_reorg_backoff_blocks,
)
from ..utils.date import weekday_number
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
# sync.max_parallel_networks, re-read on every config reload.
_MAX_WORKER_THREADS = 64
_TICK_SECONDS = 1.0

Notifier = Callable[[str, str, str, int], object]


def default_max_blocks(network: str) -> int:
    return max(1, _DEFAULT_SLICE_SECONDS // avg_blocktimes_by_currencies[network])


def task_key(name: str) -> str:
    """State-store name of a task (claim, pause, status), apart from networks."""
    return f"task.{name}"


def task_slots(task: SyncTaskConfig, now: float) -> Tuple[float, float]:
    """Epoch seconds of the task's latest scheduled time at or before ``now``,
    and of the one after it."""
    current = datetime.fromtimestamp(now, timezone.utc)
    hours, minutes = (int(x) for x in task.at.split(":"))
    slot = current.replace(hour=hours, minute=minutes, second=0, microsecond=0)
    if task.weekday is None:
        period = timedelta(days=1)
    else:
        period = timedelta(days=7)
        slot -= timedelta(days=(current.weekday() - weekday_number(task.weekday)) % 7)
    if slot > current:
        slot -= period
    return slot.timestamp(), (slot + period).timestamp()


def _default_notifier(topic: str, msg: str, dedup_key: str, every_seconds: int):
    from ..monitoring.notifications import send_msg_to_topic_rate_limited

    return send_msg_to_topic_rate_limited(topic, msg, dedup_key, every_seconds)


@dataclass
class _Runtime:
    """What the scheduler remembers about one network between cycles."""

    next_due: float = 0.0  # monotonic
    failures: int = 0
    alerted: bool = False
    busy_since: Optional[float] = None  # monotonic
    last_rates_at: float = 0.0  # wall clock
    last_consistency_at: float = 0.0  # wall clock
    last_compaction_at: float = 0.0  # wall clock
    future: Optional[Future] = None
    claimed: bool = False
    last_skip_reason: Optional[str] = None


@dataclass
class _TaskRuntime:
    future: Optional[Future] = None
    claimed: bool = False
    last_skip_reason: Optional[str] = None


class _Slots:
    """Limits how many jobs of one kind run at once, across networks. The
    limit follows config reloads; a job holding a slot of the old limit gives
    it back there."""

    def __init__(self):
        self.size = 0
        self._sem = threading.BoundedSemaphore(1)

    def resize(self, size: int):
        if size != self.size:
            self._sem = threading.BoundedSemaphore(size)
            self.size = size

    def acquire(self, stop: threading.Event) -> Optional[threading.BoundedSemaphore]:
        """Wait for a slot; returns what to release() it on, or None when
        stopping."""
        sem = self._sem
        while not stop.is_set():
            if sem.acquire(timeout=1.0):
                return sem
            sem = self._sem
        return None


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
        self._tasks: Dict[str, _TaskRuntime] = {}
        self._probes: Dict[str, ChainProbe] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=_MAX_WORKER_THREADS, thread_name_prefix="sync-cycle"
        )
        self._environment: Optional[Environment] = None
        self._delta_slots = _Slots()
        self._compaction_slots = _Slots()
        self._config_error_reported = False
        # What is missing from the config (the sync section, -c networks),
        # warned about once until it appears, not on every tick.
        self._missing: set = set()
        self._once = False
        self._signals = 0

    # -- lifecycle ---------------------------------------------------------

    def stop(self):
        self._stop.set()

    def _on_signal(self, signum, frame):
        """SIGINT/SIGTERM. First: stop starting jobs and let running ones stop
        after their current chunk. Second: say what we are waiting for. Third:
        SIGKILL the jobs.

        Killing is crash-safe for the data (ingest writes `block` last and
        cleans up after itself, delta-update replays its WAL), but a killed
        job leaves its Redis lock behind, and the next run of that network
        stays blocked until someone deletes it (`redis-cli DEL`, see
        utils/locking.py). File locks are released by the OS.
        """
        self._signals += 1
        name = signal.Signals(signum).name
        if self._signals == 1:
            logger.warning(
                f"{name}: stopping. Running jobs stop after their current "
                "chunk; send it twice more to kill them."
            )
            self.stop()
        elif self._signals == 2:
            running = ", ".join(
                f"{label} ({secs / 60:.0f} min)"
                for label, secs in self.runner.running().items()
            )
            logger.warning(
                f"{name}: still waiting for: {running or 'nothing'}. One more "
                "kills them"
                + (
                    "; their Redis locks then stay behind."
                    if get_config().use_redis_locks
                    else "."
                )
            )
        else:
            logger.error(f"{name}: killing running jobs")
            self.runner.kill_now.set()

    def request_reload(self):
        self._reload_requested.set()

    def run(self, once: bool = False) -> int:
        """Run until stopped (SIGTERM/SIGINT), or one cycle per network with
        ``once``. Returns a process exit code."""
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, self._on_signal)
            signal.signal(signal.SIGINT, self._on_signal)
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
                runtimes = [*self._runtime.values(), *self._tasks.values()]
                for f in [rt.future for rt in runtimes if rt.future]:
                    failed |= bool(f.result())
                if self._missing:
                    return 2
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
        for network, rt in self._runtime.items():
            if rt.claimed:
                try:
                    self.store.release(network, self.daemon_id)
                except Exception as e:
                    logger.warning(f"Could not release claim on {network}: {e}")
        for name, trt in self._tasks.items():
            if trt.claimed:
                try:
                    self.store.release(task_key(name), self.daemon_id)
                except Exception as e:
                    logger.warning(f"Could not release claim on task {name}: {e}")
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
        if env is None:
            return
        missing = set()
        if env.sync is None:
            missing.add(f"the sync section of environment {self.env}")
        elif self.only:
            missing |= {
                f"sync.networks.{n} (environment {self.env})"
                for n in self.only - set(env.sync.networks)
            }
        for what in sorted(missing - self._missing):
            logger.warning(f"Not configured, nothing to run: {what}")
        self._missing = missing
        if env.sync is None:
            return
        sync = env.sync
        self._delta_slots.resize(sync.max_parallel_delta_updates)
        self.runner.stop_grace_seconds = sync.stop_grace_seconds
        self._compaction_slots.resize(sync.max_parallel_compactions)

        now = self._clock()
        running = sum(1 for rt in self._runtime.values() if _is_running(rt))
        for network, ccfg in sync.networks.items():
            rt = self._runtime.setdefault(network, _Runtime())
            try:
                if self._schedule_one(network, ccfg, env, sync, rt, now, running):
                    running += 1
            except Exception as e:
                # e.g. Redis unreachable: try again next tick.
                logger.warning(f"Scheduling {network} failed: {e}")
        for name, task in sync.tasks.items():
            trt = self._tasks.setdefault(name, _TaskRuntime())
            try:
                self._schedule_task(name, task, sync, trt)
            except Exception as e:
                logger.warning(f"Scheduling task {name} failed: {e}")
        self._announce(sync)

    def _schedule_one(
        self,
        network: str,
        ccfg: SyncNetworkConfig,
        env: Environment,
        sync: SyncConfig,
        rt: _Runtime,
        now: float,
        running: int,
    ) -> bool:
        """Start a cycle for the network if it is ours and due. Returns
        whether one was started."""
        mine = (
            ccfg.enabled
            and ccfg.channel == self.channel
            and (self.only is None or network in self.only)
        )
        if _is_running(rt):
            # Keep the claim alive while the cycle runs, even if the network
            # was just moved away: the job is still going.
            rt.claimed = self.store.claim(network, self.daemon_id)
            return False
        rt.future = None
        if not mine:
            if rt.claimed:
                self.store.release(network, self.daemon_id)
                rt.claimed = False
                logger.info(f"{network}: no longer on channel {self.channel}, released")
            return False
        if network not in env.keyspaces:
            self._skip(rt, network, f"not configured in environment {self.env}")
            return False

        pause = self.store.get_pause(network)
        if pause is not None:
            self._skip(rt, network, f"paused: {pause.get('reason')}")
            self._remind_paused(network, pause, sync)
            if rt.claimed:
                rt.claimed = self.store.claim(network, self.daemon_id)
            return False

        at_capacity = not self._once and running >= sync.max_parallel_networks
        if now < rt.next_due or at_capacity:
            if rt.claimed:
                rt.claimed = self.store.claim(network, self.daemon_id)
            return False

        if not self.store.claim(network, self.daemon_id):
            rt.claimed = False
            owner = self.store.claim_owner(network)
            self._skip(rt, network, f"claimed by another daemon: {owner}")
            rt.next_due = now + (ccfg.poll_seconds or sync.poll_seconds)
            return False
        rt.claimed = True
        rt.last_skip_reason = None
        rt.future = self._executor.submit(
            self._run_cycle, network, ccfg, env.keyspaces[network], sync, rt
        )
        return True

    def _skip(self, rt: _Runtime, network: str, reason: str):
        if rt.last_skip_reason != reason:
            logger.info(f"{network}: skipped, {reason}")
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
                    )
                    + sorted(
                        task_key(n) for n, t in self._tasks.items() if _is_running(t)
                    ),
                    "networks": sorted(
                        c for c, rt in self._runtime.items() if rt.claimed
                    ),
                },
            )
        except Exception as e:
            logger.debug(f"Daemon heartbeat failed: {e}")

    def _remind_paused(self, network: str, pause: dict, sync: SyncConfig):
        since = pause.get("since") or 0
        if pause.get("until") is None and self._wall() - since > (
            _PAUSE_REMINDER_AFTER_SECONDS
        ):
            self._alert(
                sync,
                f"sync-paused:{self.env}:{network}",
                f"sync {self.env}/{network} has been paused since "
                f"{_fmt_ts(since)} by {pause.get('by')}: {pause.get('reason')}. "
                f"Resume with: graphsense-cli sync resume -e {self.env} -c {network}",
                every=_PAUSE_REMINDER_AFTER_SECONDS,
            )

    # -- tasks -------------------------------------------------------------

    def _schedule_task(
        self, name: str, task: SyncTaskConfig, sync: SyncConfig, trt: _TaskRuntime
    ):
        key = task_key(name)
        if _is_running(trt):
            self.store.claim(key, self.daemon_id)
            return
        trt.future = None
        # A daemon started with -c runs only those networks.
        if not (task.enabled and task.channel == self.channel and self.only is None):
            return
        pause = self.store.get_pause(key)
        if pause is not None:
            self._skip(trt, key, f"paused: {pause.get('reason')}")
            return
        trt.last_skip_reason = None
        if not self._task_due(task, self.store.read_status(key)):
            return
        if not self.store.claim(key, self.daemon_id):
            return  # another daemon runs it
        # It may just have finished on the daemon whose claim we now took.
        if not self._task_due(task, self.store.read_status(key)):
            self.store.release(key, self.daemon_id)
            return
        trt.claimed = True
        trt.future = self._executor.submit(self._run_task, name, task, sync, trt)

    def _task_due(self, task: SyncTaskConfig, status: Optional[dict]) -> bool:
        st = status or {}
        now = self._wall()
        slot, _ = task_slots(task, now)
        if (st.get("last_success_at") or 0) >= slot:
            return False
        attempt = st.get("last_attempt_at") or 0
        return attempt < slot or now - attempt >= task.retry_minutes * 60

    def _run_task(
        self, name: str, task: SyncTaskConfig, sync: SyncConfig, trt: _TaskRuntime
    ) -> bool:
        """Returns whether the task failed."""
        key = task_key(name)
        started = int(self._wall())
        status = dict(self.store.read_status(key) or {})
        status.update(
            owner=self._daemon_key,
            version=__version__,
            running="task",
            started_at=started,
        )
        failed = False
        try:
            self.store.write_status(key, status)
            result = self.runner.run(
                name,
                [*task.args, "-e", self.env],
                task.timeout_minutes * 60,
                self._stop,
                label=f"task {name}",
            )
            if result.outcome is Outcome.STOPPED:
                # Not an attempt: the next daemon starts it again.
                status["last_result"] = "stopped"
            elif result.outcome is Outcome.BUSY:
                status.update(last_attempt_at=started, last_result="lock busy")
            elif result.failed:
                failed = True
                status.update(
                    last_attempt_at=started,
                    last_failure_at=int(self._wall()),
                    last_result=f"failed: {_describe(result)}",
                    failures=status.get("failures", 0) + 1,
                )
                self._alert(
                    sync,
                    f"sync-task-failure:{self.env}:{name}",
                    f"sync {self.env} task {name} failed: {_describe(result)}. "
                    f"Retry in {task.retry_minutes} min.\n" + _fmt_tail(result.tail),
                )
            else:
                status.update(
                    last_attempt_at=started,
                    last_success_at=started,
                    last_result="ok",
                    failures=0,
                    seconds=round(result.seconds),
                )
                if task.success_topic:
                    self._notify_task_success(task, name, result)
        except Exception as e:
            logger.exception(f"task {name} crashed")
            failed = True
            status.update(last_attempt_at=started, last_result=f"failed: {e}")
        finally:
            status.pop("running", None)
            try:
                self.store.write_status(key, status)
            except Exception as e:
                logger.warning(f"task {name}: could not write status: {e}")
            # Only after the status: a daemon that takes the claim next sees
            # this run.
            try:
                self.store.release(key, self.daemon_id)
            except Exception as e:
                logger.warning(f"task {name}: could not release claim: {e}")
            trt.claimed = False
        return failed

    def _notify_task_success(self, task: SyncTaskConfig, name: str, result):
        try:
            self._notify(
                task.success_topic,
                f"sync {self.env} task {name} done in {result.seconds / 60:.0f} min.",
                f"sync-task-ok:{self.env}:{name}:{uuid.uuid4().hex}",
                60,
            )
        except Exception as e:
            logger.warning(f"Could not send notification: {e}")

    # -- one cycle ---------------------------------------------------------

    def _run_cycle(
        self,
        network: str,
        ccfg: SyncNetworkConfig,
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
            delay, failed = self._cycle(network, ccfg, ks, sync, rt, status)
        except Exception as e:
            logger.exception(f"{network}: cycle crashed")
            probe = self._probes.pop(network, None)
            if probe is not None:
                probe.close()
            delay, failed = self._failure(network, sync, rt, status, poll, str(e), [])
        rt.next_due = self._clock() + delay
        status["next_cycle_at"] = int(self._wall() + delay)
        status["failures"] = rt.failures
        try:
            self.store.write_status(network, status)
        except Exception as e:
            logger.warning(f"{network}: could not write status: {e}")
        return failed

    def _probe(self, network: str, ks: KeyspaceConfig) -> ChainProbe:
        probe = self._probes.get(network)
        if probe is None:
            probe = self._probe_factory(self.env, network, ks)
            self._probes[network] = probe
        return probe

    def _cycle(self, network, ccfg, ks, sync, rt, status):
        poll = ccfg.poll_seconds or sync.poll_seconds
        probe = self._probe(network, ks)
        try:
            tip = probe.node_tip()
            raw = probe.raw_height()
        except Exception as e:
            probe.reset()
            return self._failure(
                network, sync, rt, status, poll, f"probe failed: {e}", []
            )
        ic = ks.ingest_config
        backoff = get_reorg_backoff_blocks(
            network, override=ic.reorg_backoff_blocks if ic else None
        )
        target = tip - backoff
        status.update(node_tip=tip, target=target, raw=raw)
        if raw is None:
            return self._failure(
                network,
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
                self._pause_for_reorg(network, sync, mismatch, status)
                return poll, True

        self._check_lag(network, ccfg, sync, probe, raw)

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
                network,
                *ccfg.ingest_args,
                "--no-staleness-check",
                "--no-auto-compact",
            ]
            if not rates_due:
                args.append("--no-exchange-rates")
            end = None
            if raw < target:
                end = min(
                    target,
                    raw + (ccfg.max_blocks_per_job or default_max_blocks(network)),
                )
                args += ["--end-block", str(end)]
            result = self._step(
                network, "ingest", args, sync.ingest_timeout_minutes, status
            )
            outcome = self._handle(network, sync, rt, status, poll, result)
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
                    network,
                    sync,
                    rt,
                    status,
                    poll,
                    "transformed keyspace missing; run the full transformation first",
                    [],
                )
            if raw is not None and transformed < raw:
                result = self._step_in_slot(
                    self._delta_slots,
                    network,
                    "delta_update",
                    ["delta-update", "update", "-e", self.env, "-c", network]
                    + ccfg.delta_update_args,
                    sync.delta_update_timeout_minutes,
                    status,
                )
                if result is None:
                    return 0, False  # stopping
                outcome = self._handle(network, sync, rt, status, poll, result)
                if outcome is not None:
                    return outcome
                status["transformed"] = probe.transformed_height()

        # -- compaction
        if self._compaction_due(ks, sync, rt):
            result = self._step_in_slot(
                self._compaction_slots,
                network,
                "compact",
                [
                    "ingest",
                    "delta-lake",
                    "auto-compact",
                    "-e",
                    self.env,
                    "-c",
                    network,
                ],
                sync.compaction_timeout_minutes,
                status,
            )
            if result is None:
                return 0, False  # stopping
            if result.outcome not in (Outcome.BUSY, Outcome.STOPPED):
                # A failed compaction is retried at the next check, not on
                # every cycle: ingest and delta-update go on meanwhile.
                rt.last_compaction_at = self._wall()
            outcome = self._handle(network, sync, rt, status, poll, result)
            if outcome is not None:
                return outcome

        # -- consistency check
        every = sync.consistency_check_every_minutes
        if every is not None and self._wall() - rt.last_consistency_at >= every * 60:
            result = self._step(
                network,
                "consistency",
                ["monitoring", "check-consistency", "-e", self.env, "-c", network]
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
                    f"sync-inconsistent:{self.env}:{network}",
                    f"sync {self.env}/{network}: consistency check failed.\n"
                    + _fmt_tail(result.tail),
                )
            elif result.failed:
                return self._failure(
                    network, sync, rt, status, poll, _describe(result), result.tail
                )

        self._success(network, sync, rt, status)
        return (0.0 if catching_up else poll), False

    def _step(self, network, step, args, timeout_minutes, status) -> StepResult:
        status["running"] = step
        try:
            self.store.write_status(network, status)
        except Exception:
            pass
        result = self.runner.run(
            step, args, timeout_minutes * 60, self._stop, label=f"{network} {step}"
        )
        status.pop("running", None)
        status.setdefault("steps", {})[step] = {
            "outcome": result.outcome.value,
            "exit_code": result.exit_code,
            "seconds": round(result.seconds, 1),
            "at": int(self._wall()),
        }
        return result

    def _step_in_slot(
        self, slots: _Slots, network, step, args, timeout_minutes, status
    ) -> Optional[StepResult]:
        """_step once a slot is free; None if we are stopping meanwhile."""
        sem = slots.acquire(self._stop)
        if sem is None:
            return None
        try:
            return self._step(network, step, args, timeout_minutes, status)
        finally:
            sem.release()

    def _compaction_due(self, ks: KeyspaceConfig, sync: SyncConfig, rt) -> bool:
        ic = ks.ingest_config
        return (
            ic is not None
            and bool(ic.auto_compact)
            and "delta" in ic.raw_keyspace_file_sinks
            and self._wall() - rt.last_compaction_at
            >= sync.compaction_check_every_minutes * 60
        )

    def _handle(self, network, sync, rt, status, poll, result: StepResult):
        """Turn a step result into an early cycle return, or None to go on."""
        if result.outcome is Outcome.STOPPED:
            return 0, False
        if result.failed:
            return self._failure(
                network, sync, rt, status, poll, _describe(result), result.tail
            )
        if result.outcome is Outcome.BUSY:
            now = self._clock()
            rt.busy_since = rt.busy_since or now
            if now - rt.busy_since > _BUSY_ALERT_AFTER_SECONDS:
                self._alert(
                    sync,
                    f"sync-busy:{self.env}:{network}",
                    f"sync {self.env}/{network}: `{result.step}` has found its "
                    f"lock taken for {int((now - rt.busy_since) // 60)} min. "
                    "Is a manual run going on, or did a killed job leave the "
                    "lock behind? The job log names the holder.\n"
                    + _fmt_tail(result.tail),
                )
            status["last_result"] = f"{result.step}: lock busy"
            return poll, False
        rt.busy_since = None
        return None

    def _success(self, network, sync, rt, status):
        if rt.alerted:
            self._notify(
                sync.alert_topic,
                f"sync {self.env}/{network} recovered after {rt.failures} "
                "failed cycles.",
                f"sync-recovered:{self.env}:{network}:{uuid.uuid4().hex}",
                60,
            )
        rt.failures = 0
        rt.alerted = False
        status["last_result"] = "ok"
        status["last_success_at"] = int(self._wall())

    def _failure(self, network, sync, rt, status, poll, reason, tail):
        rt.failures += 1
        delay = min(poll * 2 ** (rt.failures - 1), sync.max_backoff_minutes * 60)
        logger.error(
            f"{network}: cycle failed ({rt.failures} in a row), retry in "
            f"{delay:.0f}s: {reason}"
        )
        status["last_result"] = f"failed: {reason}"
        status["last_failure_at"] = int(self._wall())
        if rt.failures >= sync.failure_alert_after:
            self._alert(
                sync,
                f"sync-failure:{self.env}:{network}",
                f"sync {self.env}/{network} failed {rt.failures} times in a "
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

    def _pause_for_reorg(self, network, sync, mismatch, status):
        reason = (
            f"reorg detected: block {mismatch['height']} is {mismatch['raw']} in "
            f"raw but {mismatch['node']} at the node"
        )
        self.store.pause(network, reason, by=f"sync daemon {self._daemon_key}")
        status["last_result"] = reason
        self._alert(
            sync,
            f"sync-reorg:{self.env}:{network}",
            f"sync {self.env}/{network} PAUSED: {reason}. The reorg went "
            "deeper than the reorg backoff, so raw (and possibly transformed) "
            "holds orphaned blocks. Repair them, then resume with: "
            f"graphsense-cli sync resume -e {self.env} -c {network}",
        )

    def _check_lag(self, network, ccfg, sync, probe, raw):
        if ccfg.lag_alert_minutes is None:
            return
        try:
            ts = probe.raw_block_timestamp(raw)
        except Exception as e:
            logger.debug(f"{network}: lag check skipped: {e}")
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
                f"sync-lag:{self.env}:{network}",
                f"sync {self.env}/{network}: newest raw block {raw} is "
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
