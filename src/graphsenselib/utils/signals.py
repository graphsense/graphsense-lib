import logging
import signal
import threading
from contextlib import contextmanager

logger = logging.getLogger(__name__)

SHUTDOWN = False
SHUTDOWN_SIGNAL_CNT = 0
# Handlers in force before register_ctrlc_shutdown_handler, put back by
# unregister_ctrlc_shutdown_handler.
_PREVIOUS_HANDLERS = {}


def _interrupt_on_sigterm(signum, frame):
    raise KeyboardInterrupt(f"Received {signal.Signals(signum).name}")


def install_sigterm_as_interrupt():
    """Make SIGTERM behave like Ctrl-C (KeyboardInterrupt) outside the graceful
    sections, instead of Python's default of dying on the spot.

    The default skips every finally block and context exit, so a job stopped
    between taking its Redis lock and entering graceful_ctlc_shutdown (or just
    after leaving it) never released the lock -- which has no TTL, so the
    network stayed blocked until someone ran redis-cli DEL. As an exception the
    stop unwinds and the lock is released. A no-op outside the main thread,
    where signal.signal is not allowed.
    """
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _interrupt_on_sigterm)


def shutdown_handler(signum, frame):
    global SHUTDOWN, SHUTDOWN_SIGNAL_CNT
    sig_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
    logger.warning(f"Received {sig_name}, will shutdown when reaching a safe spot.")
    SHUTDOWN = True
    SHUTDOWN_SIGNAL_CNT += 1
    if SHUTDOWN_SIGNAL_CNT > 10:
        logger.warning(
            "Received shutdown signal more than 10 times. "
            "Disabling shutdown handler. "
            "The next signal will interrupt the process. "
            "Be careful, you are on your own now."
        )
        unregister_ctrlc_shutdown_handler()


def shutdown_initialized():
    return SHUTDOWN


def register_ctrlc_shutdown_handler():
    logger.info("Registered graceful shutdown handler.")
    for sig in (signal.SIGINT, signal.SIGTERM):
        _PREVIOUS_HANDLERS[sig] = signal.signal(sig, shutdown_handler)


def unregister_ctrlc_shutdown_handler():
    """Put back what was in force before register_ctrlc_shutdown_handler (for
    SIGTERM under graphsense-cli: install_sigterm_as_interrupt), not SIG_DFL,
    which kills without running the cleanup that releases locks."""
    logger.info("Unregistered graceful shutdown handler.")
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous = _PREVIOUS_HANDLERS.pop(sig, None)
        if previous is None:
            # set from outside Python, or never registered
            previous = (
                signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL
            )
        signal.signal(sig, previous)


@contextmanager
def graceful_ctlc_shutdown(*args, **kwds):
    register_ctrlc_shutdown_handler()
    try:
        yield shutdown_initialized
    finally:
        unregister_ctrlc_shutdown_handler()
