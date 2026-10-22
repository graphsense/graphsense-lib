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


def test_kill_now_cuts_grace_short():
    runner = JobRunner(cli_command=PY, terminate_grace_seconds=60)
    stop = threading.Event()
    code = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print('started', flush=True)\n"
        "time.sleep(60)\n"
    )
    t = threading.Timer(0.5, stop.set)
    k = threading.Timer(2.0, runner.kill_now.set)
    t.start()
    k.start()
    r = runner.run("step", [code], 30, stop, "test")
    assert r.outcome is Outcome.STOPPED
    assert r.exit_code == -9
    assert r.seconds < 10


def test_stop_uses_stop_grace_not_timeout_grace():
    # a job that needs ~2s to finish its chunk after SIGTERM
    code = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, lambda *a: None)\n"
        "print('started', flush=True)\n"
        "time.sleep(0.5)\n"
        "time.sleep(2)\n"
    )
    runner = JobRunner(cli_command=PY, terminate_grace_seconds=1, stop_grace_seconds=10)
    stop = threading.Event()
    threading.Timer(0.3, stop.set).start()
    r = runner.run("step", [code], 30, stop, "test")
    assert r.outcome is Outcome.STOPPED
    assert r.exit_code == 0  # finished, not killed after the 1s timeout grace


SIGTERM_CLEANUP_JOB = (
    "import time\n"
    "from graphsenselib.utils.signals import install_sigterm_as_interrupt\n"
    "install_sigterm_as_interrupt()\n"
    "try:\n"
    "    print('lock taken', flush=True)\n"
    "    time.sleep(60)\n"
    "finally:\n"
    "    print('lock released', flush=True)\n"
)


def test_sigterm_outside_graceful_section_still_runs_cleanup():
    # what releases a Redis lock that is held outside graceful_ctlc_shutdown
    stop = threading.Event()
    threading.Timer(1.0, stop.set).start()
    r = run(SIGTERM_CLEANUP_JOB, stop=stop)
    assert r.outcome is Outcome.STOPPED
    assert "lock released" in r.tail
    assert r.exit_code != -9
