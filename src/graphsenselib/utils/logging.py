import logging
import threading
import time


class IndentLogger(logging.LoggerAdapter):
    _ident = 0

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
        return IndentLogger(logger).set_ident(len(LoggerScope.get_stack()))

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
