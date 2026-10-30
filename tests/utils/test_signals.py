import subprocess
import sys
import textwrap

# Run in a child: the handlers are process-wide and pytest must keep its own.
SCRIPT = textwrap.dedent(
    """
    import os, signal
    from graphsenselib.utils import signals

    signals.install_sigterm_as_interrupt()

    def term():
        os.kill(os.getpid(), signal.SIGTERM)

    # outside a graceful section: unwinds like Ctrl-C, finally blocks run
    try:
        try:
            term()
        finally:
            print("cleanup ran")
    except KeyboardInterrupt:
        print("interrupted")

    # inside: only sets the flag, the loop stops at a safe spot
    with signals.graceful_ctlc_shutdown() as shutdown_initialized:
        term()
        print("flag", shutdown_initialized())

    # after leaving: the interrupt handler is back, not SIG_DFL
    print("restored", signal.getsignal(signal.SIGTERM) is signals._interrupt_on_sigterm)
    try:
        term()
    except KeyboardInterrupt:
        print("interrupted again")
    """
)


def test_sigterm_handling_around_graceful_sections():
    out = subprocess.run(
        [sys.executable, "-c", SCRIPT], capture_output=True, text=True, timeout=30
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.split("\n")[:-1] == [
        "cleanup ran",
        "interrupted",
        "flag True",
        "restored True",
        "interrupted again",
    ]
