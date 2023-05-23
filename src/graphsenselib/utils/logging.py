import logging
import threading
import time
from contextlib import contextmanager

import click
from rich.console import Console
from rich.logging import RichHandler

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT


@contextmanager
def suppress_log_level(loglevel: int):
    logging.disable(loglevel)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)


def configure_logging(loglevel):
    log_format = " | %(message)s"

    if loglevel == 0:
        loglevel = logging.WARNING
    elif loglevel == 1:
        loglevel = logging.INFO
    elif loglevel >= 2:
        loglevel = logging.DEBUG

    """ RichHandler colorizes the logs """
    c = Console(width=220)
    if c.is_terminal:
        rh = RichHandler(rich_tracebacks=True, tracebacks_suppress=[click])
    else:
        # if file redirect set terminal width to 220
        rh = RichHandler(
            rich_tracebacks=True,
            tracebacks_suppress=[click],
            console=c,
            show_path=False,
        )
    logging.basicConfig(
        format=log_format,
        level=loglevel,
        datefmt=GRAPHSENSE_DEFAULT_DATETIME_FORMAT,
        handlers=[rh],
    )

    logging.getLogger("cassandra").setLevel(logging.ERROR)
    logging.getLogger("ethereumetl").setLevel(logging.ERROR)
    logging.getLogger("Cluster").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)


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
