"""Run one sync step (`ingest from-node`, `delta-update update`, ...) as a
child process.

A child process rather than a call into the same interpreter, on purpose: every
job starts from a fresh process, as the cron jobs did, so leaked memory, a
wedged HTTP connection or a stuck Cassandra session dies with the job; and a
job that hangs can be killed without killing the scheduler.
"""

import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Set in the environment of every job. The CLI then skips its own Slack
# exception digest: the scheduler retries failures with backoff and reports
# them itself, rate-limited, with the job's last log lines.
SYNC_JOB_ENV_VAR = "GRAPHSENSE_SYNC_JOB"

# Exit codes of the commands we run (see ingest/cli.py, deltaupdate/,
# monitoring/cli.py).
EXIT_NOTHING_TO_INGEST = 12
EXIT_LOCK_CONTENDED = 911
# Process exit statuses are 8 bit: the CLI's sys.exit(911) reaches its parent
# as 911 % 256 == 143. (A job killed by SIGTERM shows up as -15 from Popen,
# never as 143, so the two cannot be confused.)
_EXIT_LOCK_CONTENDED_STATUS = EXIT_LOCK_CONTENDED % 256
EXIT_INCONSISTENT = 92

# How long a job gets to stop cleanly after SIGTERM before SIGKILL. Ingest and
# delta-update stop after the current chunk/batch (graceful_ctlc_shutdown).
# This one is the floor; when stop_grace_seconds (sync.stop_grace_seconds) is
# longer it applies to a timed-out job too, since a delta-update batch killed
# midway leaves its locks and a WAL record whether the daemon stops or not.
TERMINATE_GRACE_SECONDS = 120
_TAIL_LINES = 30


class Outcome(Enum):
    OK = "ok"
    NOTHING = "nothing"  # ran fine, nothing new to do
    BUSY = "busy"  # lock held by another process
    FAILED = "failed"
    TIMEOUT = "timeout"
    STOPPED = "stopped"  # we were shutting down


@dataclass
class StepResult:
    step: str
    outcome: Outcome
    exit_code: Optional[int]
    seconds: float
    tail: List[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.outcome in (Outcome.FAILED, Outcome.TIMEOUT)


def classify(exit_code: int) -> Outcome:
    if exit_code == 0:
        return Outcome.OK
    if exit_code == EXIT_NOTHING_TO_INGEST:
        return Outcome.NOTHING
    if exit_code in (EXIT_LOCK_CONTENDED, _EXIT_LOCK_CONTENDED_STATUS):
        return Outcome.BUSY
    return Outcome.FAILED


def default_cli_command() -> List[str]:
    """The `graphsense-cli` of the running interpreter: a child must run the
    same graphsense-lib version as its daemon."""
    candidate = Path(sys.executable).parent / "graphsense-cli"
    if candidate.exists():
        return [str(candidate)]
    found = shutil.which("graphsense-cli")
    if found:
        return [found]
    return [
        sys.executable,
        "-c",
        "from graphsenselib.cli.main import main; main()",
    ]


class JobRunner:
    def __init__(
        self,
        cli_command: Optional[List[str]] = None,
        global_args: Optional[List[str]] = None,
        terminate_grace_seconds: int = TERMINATE_GRACE_SECONDS,
        stop_grace_seconds: Optional[int] = None,
    ):
        self.cli_command = cli_command or default_cli_command()
        # e.g. ["-vv", "--config-file", "..."]: go before the subcommand.
        self.global_args = global_args or []
        self.terminate_grace_seconds = terminate_grace_seconds
        # Grace when the daemon stops (docker stop, a new image): long enough
        # for a delta-update batch to finish, so no WAL record is left behind
        # for the next version, which refuses to replay another version's.
        # The scheduler keeps it in line with sync.stop_grace_seconds.
        self.stop_grace_seconds = stop_grace_seconds
        # Set to cut the grace period short: SIGKILL instead of waiting for a
        # clean stop (the daemon's third Ctrl-C).
        self.kill_now = threading.Event()
        self._running: Dict[str, float] = {}
        self._running_lock = threading.Lock()

    def running(self) -> Dict[str, float]:
        """Label -> seconds running, for every job still alive."""
        now = time.monotonic()
        with self._running_lock:
            return {k: now - v for k, v in self._running.items()}

    def run(
        self,
        step: str,
        args: List[str],
        timeout_seconds: float,
        stop: threading.Event,
        label: str,
    ) -> StepResult:
        cmd = self.cli_command + self.global_args + args
        logger.info(f"[{label}] starting: {' '.join(args)}")
        started = time.monotonic()
        tail: deque = deque(maxlen=_TAIL_LINES)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            text=True,
            errors="replace",
            # Own process group: a Ctrl-C on the daemon's terminal must not
            # reach the jobs directly, and on timeout we signal the whole
            # group, including anything the job spawned.
            start_new_session=True,
            env={**os.environ, SYNC_JOB_ENV_VAR: "1"},
        )
        reader = threading.Thread(
            target=_pump_output,
            args=(proc, tail, label),
            name=f"sync-output-{label}",
            daemon=True,
        )
        reader.start()
        with self._running_lock:
            self._running[label] = started

        timed_out = stopped = False
        while proc.poll() is None:
            if time.monotonic() - started > timeout_seconds:
                timed_out = True
                break
            if stop.is_set():
                stopped = True
                break
            try:
                proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass

        if timed_out or stopped:
            why = "timed out" if timed_out else "stopping"
            grace = max(self.terminate_grace_seconds, self.stop_grace_seconds or 0)
            logger.warning(f"[{label}] {why}, sending SIGTERM (SIGKILL after {grace}s)")
            self._terminate(proc, label, grace)

        with self._running_lock:
            self._running.pop(label, None)
        reader.join(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()
        seconds = time.monotonic() - started
        code = proc.returncode
        if timed_out:
            outcome = Outcome.TIMEOUT
        elif stopped:
            outcome = Outcome.STOPPED
        else:
            outcome = classify(code)
        logger.info(
            f"[{label}] finished: {outcome.value} (exit {code}, {seconds:.0f}s)"
        )
        return StepResult(step, outcome, code, seconds, list(tail))

    def _terminate(self, proc: subprocess.Popen, label: str, grace: float):
        _signal_group(proc, signal.SIGTERM)
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline and not self.kill_now.is_set():
            try:
                proc.wait(timeout=1.0)
                return
            except subprocess.TimeoutExpired:
                pass
        why = (
            "kill requested"
            if self.kill_now.is_set()
            else f"still running {grace:.0f}s after SIGTERM"
        )
        logger.error(f"[{label}] {why}, sending SIGKILL")
        _signal_group(proc, signal.SIGKILL)
        proc.wait()


def _signal_group(proc: subprocess.Popen, sig):
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        pass


def _pump_output(proc: subprocess.Popen, tail: deque, label: str):
    """Forward the job's output to our log, keeping the last lines for alerts."""
    assert proc.stdout is not None
    job_logger = logging.getLogger("graphsenselib.sync.job")
    try:
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                tail.append(line)
                job_logger.info(f"[{label}] {line}")
    except (ValueError, OSError):
        # The pipe was closed under us: something the job spawned kept it
        # open after the job exited, and run() gave up waiting for EOF.
        pass
