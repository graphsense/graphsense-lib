import sys
import threading

from graphsenselib.sync.jobs import SYNC_JOB_ENV_VAR, JobRunner, Outcome

PY = [sys.executable, "-c"]


def run(code, timeout=30, stop=None, grace=5):
    runner = JobRunner(cli_command=PY, terminate_grace_seconds=grace)
    return runner.run("step", [code], timeout, stop or threading.Event(), "test")


def test_exit_codes_map_to_outcomes():
    assert run("pass").outcome is Outcome.OK
    assert run("raise SystemExit(12)").outcome is Outcome.NOTHING
    assert run("raise SystemExit(911)").outcome is Outcome.BUSY
    r = run("raise SystemExit(3)")
    assert r.outcome is Outcome.FAILED and r.exit_code == 3 and r.failed


def test_output_tail_and_job_env():
    r = run(
        "import os; print('hello'); print(os.environ.get(%r)); "
        "raise SystemExit(1)" % SYNC_JOB_ENV_VAR
    )
    assert r.tail[-2:] == ["hello", "1"]


def test_timeout_sends_sigterm():
    # the job handles SIGTERM like graceful_ctlc_shutdown: finish, exit 0
    code = (
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, lambda *a: (print('stopping'), sys.exit(0)))\n"
        "print('started', flush=True)\n"
        "time.sleep(60)\n"
    )
    r = run(code, timeout=1)
    assert r.outcome is Outcome.TIMEOUT
    assert "stopping" in r.tail
    assert r.seconds < 10


def test_sigkill_after_grace():
    code = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print('started', flush=True)\n"
        "time.sleep(60)\n"
    )
    r = run(code, timeout=1, grace=1)
    assert r.outcome is Outcome.TIMEOUT
    assert r.exit_code == -9
    assert r.seconds < 10


def test_stop_event_stops_job():
    stop = threading.Event()
    stop.set()
    r = run("import time; time.sleep(60)", stop=stop)
    assert r.outcome is Outcome.STOPPED
