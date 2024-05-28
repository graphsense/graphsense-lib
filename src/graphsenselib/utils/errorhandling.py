import json
import os
from contextlib import contextmanager


@contextmanager
def cr_critical_section(recovery_hint: dict, crash_recoverer):
    try:
        yield
    except Exception as e:
        recovery_hint["exception"] = str(e)
        recovery_hint["exception_type"] = type(e).__name__
        crash_recoverer.enter_recovery_mode(recovery_hint)
        raise e


def get_exception_digest(ex):
    return f"{type(ex).__name__}: {str(ex)}"


class CrashRecoverer:
    """
    Class permanently stores (in a file) a crash hint if a program crashes in a critical
    If the class is reinstantiated with the same identifier and a crash hint is present
    the class does not allow the program to enter the critical section again until
    the crash and the underlying inconsistency is resolved
    (e.g. by calling leave_recover_mode) or by deleting the crash hint file.

    Crash hints are user specified and in the optimal case should contain enough
    information for the program to recover from crashes automatically on the
    next run.
    """

    def __init__(self, crashfile_name: str):
        self._crashfile = os.path.expanduser(crashfile_name)

    def is_in_recovery_mode(self) -> bool:
        return os.path.exists(self._crashfile)

    def enter_critical_section(self, recovery_hint: dict):
        """
        test if recovery hint is json serializable
        to avoid that serialisation fails in case of an error
        """
        json.dumps(recovery_hint)
        if self.is_in_recovery_mode():
            raise ValueError("We are already in recovery mode.")
        return cr_critical_section(recovery_hint, self)

    def get_recovery_hint(self) -> dict:
        with open(self._crashfile, "r") as f:
            return json.load(f)

    def get_recovery_hint_filename(self) -> str:
        return self._crashfile

    def enter_recovery_mode(self, recovery_hint: dict):
        if self.is_in_recovery_mode():
            raise ValueError("We are already in recovery mode.")
        with open(os.path.expanduser(self._crashfile), "w") as f:
            json.dump(recovery_hint, f)

    def leave_recovery_mode(self):
        if self.is_in_recovery_mode():
            os.remove(self._crashfile)
