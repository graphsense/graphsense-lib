import logging
import signal
from contextlib import contextmanager

logger = logging.getLogger(__name__)

SHUTDOWN = False


def shutdown_handler(signum, frame):
    global SHUTDOWN
    logger.warning("Received STRG-C, will shutdown after batch is written.")
    SHUTDOWN = True


def shutdown_initialized():
    return SHUTDOWN


def register_ctrlc_shutdown_handler():
    logger.info("Registered gracefull ctrlc handler.")
    signal.signal(signal.SIGINT, shutdown_handler)


def unregister_ctrlc_shutdown_handler():
    logger.info("Unregistered gracefull ctrlc handler.")
    signal.signal(signal.SIGINT, signal.SIG_DFL)


@contextmanager
def gracefull_ctlc_shutdown(*args, **kwds):
    register_ctrlc_shutdown_handler()
    try:
        yield shutdown_initialized
    finally:
        unregister_ctrlc_shutdown_handler()
