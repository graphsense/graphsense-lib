import logging
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install as install_rich_traceback

from ..config import (
    GRAPHSENSE_DEFAULT_DATETIME_FORMAT,
    GRAPHSENSE_VERBOSE_DATETIME_FORMAT,
)

# create logger
logger = logging.getLogger(__name__)


@contextmanager
def suppress_log_level(loglevel: int):
    logging.disable(loglevel)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)


class NoETLBatchExecSpamFilter(logging.Filter):
    def filter(self, record):
        # only filter for log level error
        if record.levelno != logging.ERROR:
            return True
        return not record.getMessage().startswith(
            "An exception occurred while executing work_handler"
        ) and not record.getMessage().startswith(
            "An exception occurred while executing execute_with_retries"
        )


class MicrosecondFormatter(logging.Formatter):
    """Custom formatter that supports %f (microseconds) in datefmt.

    The standard logging.Formatter uses time.strftime() which doesn't support %f.
    This formatter uses datetime.strftime() instead.
    """

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime("%Y-%m-%d %H:%M:%S")


def configure_logging(loglevel):
    log_format = "| %(subsystem)s | %(message)s"
    datefmt = GRAPHSENSE_DEFAULT_DATETIME_FORMAT

    def addSubsys(record: logging.LogRecord):
        try:
            subsys = record.name.split(".")
            record.subsystem = (subsys[1:2] or ("",))[0]
        except Exception:
            record.subsystem = "?"
        return record

    if loglevel < 10:
        # this means the value passed is
        # not a valid log level in python
        if loglevel == 0:
            loglevel = logging.WARNING
        elif loglevel == 1:
            loglevel = logging.INFO
        elif loglevel >= 2:
            loglevel = logging.DEBUG
            log_format = " | %(name)s | %(thread)d | %(message)s"
            datefmt = GRAPHSENSE_VERBOSE_DATETIME_FORMAT

    """ RichHandler colorizes the logs for terminal, plain handler for file """
    c = Console(width=220)
    if c.is_terminal:
        rh = RichHandler(
            rich_tracebacks=True, tracebacks_suppress=[click], log_time_format=datefmt
        )
        rh.addFilter(addSubsys)
        rh.addFilter(NoETLBatchExecSpamFilter())
        handlers = [rh]
    else:
        # For file output: plain StreamHandler (no wrapping) + rich tracebacks for exceptions
        # Use MicrosecondFormatter to support %f in datefmt
        sh = logging.StreamHandler()
        sh.setFormatter(
            MicrosecondFormatter(
                fmt=f"%(asctime)s %(levelname)-8s{log_format}",
                datefmt=datefmt,
            )
        )
        sh.addFilter(addSubsys)
        sh.addFilter(NoETLBatchExecSpamFilter())
        handlers = [sh]
        # Install rich tracebacks for uncaught exceptions (shows local variables)
        install_rich_traceback(
            show_locals=True,
            suppress=[click],
            console=Console(file=sys.stderr, width=220),
        )

    logging.basicConfig(
        format=log_format,
        level=loglevel,
        datefmt=datefmt,
        handlers=handlers,
    )

    if loglevel <= logging.DEBUG:
        logger.debug("Logging set to verbose mode.")
        # Suppress cassandra driver logs to avoid interleaving with performance logs
        logging.getLogger("cassandra").setLevel(logging.WARNING)
        logging.getLogger("ethereumetl").setLevel(logging.WARNING)
        logging.getLogger("web3").setLevel(logging.WARNING)
        logging.getLogger("Cluster").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("ProgressLogger").setLevel(logging.ERROR)
        logging.getLogger("BatchWorkExecutor").setLevel(logging.ERROR)
    else:
        logging.getLogger("cassandra").setLevel(logging.ERROR)
        logging.getLogger("ethereumetl").setLevel(logging.WARNING)
        logging.getLogger("Cluster").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("ProgressLogger").setLevel(logging.ERROR)
        logging.getLogger("BatchWorkExecutor").setLevel(logging.ERROR)


class IndentLogger(logging.LoggerAdapter):
    _ident = 0

    def __init__(self, logger):
        super().__init__(logger, extra={})

    def set_ident(self, level):
        self._ident = level
        return self

    def process(self, msg, kwargs):
        return "{i}{m}".format(i=" " * self._ident, m=msg), kwargs


class LoggerScope:
    _active_group = threading.local()

    @classmethod
    def debug(Cls, logger, msg=None):
        return Cls(logger, msg=msg, level=logging.DEBUG)

    @classmethod
    def info(Cls, logger, msg=None):
        return Cls(logger, msg=msg, level=logging.INFO)

    @classmethod
    def error(Cls, logger, msg=None):
        return Cls(logger, msg=msg, level=logging.CRITICAL)

    @staticmethod
    def get_stack():
        if not hasattr(LoggerScope._active_group, "current"):
            LoggerScope._active_group.current = []
        return LoggerScope._active_group.current

    @staticmethod
    def get_current_scope():
        stk = LoggerScope.get_stack()
        return stk[0] if len(stk) > 0 else None

    @staticmethod
    def get_indent_logger(logger):
        return IndentLogger(logger).set_ident(len(LoggerScope.get_stack()) + 1)

    def __init__(self, logger, msg=None, level=logging.INFO):
        self._logger = logger
        self._level = level
        self._msg = msg

    def __enter__(self):
        currentstack = self.get_stack()
        self._start_time = time.time()
        if self._msg is not None:
            IndentLogger(self._logger).set_ident(len(currentstack)).log(
                self._level, f"B - {self._msg}"
            )
        currentstack.append(self)
        logger = IndentLogger(self._logger)
        logger.set_ident(len(currentstack) + 1)
        return logger

    def __exit__(self, exc_type, exc_value, exc_traceback):
        last = LoggerScope._active_group.current.pop()
        currentstacklen = len(self.get_stack())
        assert last == self, "Logger context exited out of order"
        self.elapsed_seconds = time.time() - self._start_time
        if self._msg is not None:
            IndentLogger(self._logger).set_ident(currentstacklen).log(
                self._level, f"E - {self._msg} - took {self.elapsed_seconds:.3f}s"
            )
