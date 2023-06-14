import logging
import signal
from contextlib import contextmanager

logger = logging.getLogger(__name__)

SHUTDOWN = False
SHUTDOWN_SIGNAL_CNT = 0


def shutdown_handler(signum, frame):
    global SHUTDOWN, SHUTDOWN_SIGNAL_CNT
    logger.warning("Received STRG-C, will shutdown when reaching a safe spot.")
    SHUTDOWN = True
    SHUTDOWN_SIGNAL_CNT += 1
    if SHUTDOWN_SIGNAL_CNT > 10:
        logger.warning(
            "Received STRG-C more than 10 times. "
            "Disabling shutdown handler. "
            "The next STRG-C will terminate the process. "
            "Be careful, you are on you own now."
        )
        unregister_ctrlc_shutdown_handler()


def shutdown_initialized():
    return SHUTDOWN


def register_ctrlc_shutdown_handler():
    logger.info("Registered graceful ctrlc handler.")
    signal.signal(signal.SIGINT, shutdown_handler)


def unregister_ctrlc_shutdown_handler():
    logger.info("Unregistered graceful ctrlc handler.")
    signal.signal(signal.SIGINT, signal.SIG_DFL)


@contextmanager
def graceful_ctlc_shutdown(*args, **kwds):
    register_ctrlc_shutdown_handler()
    try:
        yield shutdown_initialized
    finally:
        unregister_ctrlc_shutdown_handler()
