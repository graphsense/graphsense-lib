import os

from graphsenselib.utils import batch, bytes_to_hex, strip_0x, subkey_exists, to_int
from graphsenselib.utils.errorhandling import CrashRecoverer


def test_btoh_works(capsys):
    assert bytes_to_hex(b"") is None
    assert bytes_to_hex(b"asdfasdf") == "6173646661736466"


def test_strip_0x_works1():
    assert strip_0x("0xa9059cbb") == "a9059cbb"


def test_strip_0x_works2():
    assert strip_0x("a9059cbb") == "a9059cbb"


def test_to_int1():
    assert to_int(0) == 0
    assert to_int("1111") == 1111
    assert to_int("0xf") == 15
    assert to_int("0x10") == 16


def test_crash_recoverer():
    file = "/tmp/test_graphsense_lib_crashrecoverer.err"
    if os.path.exists(file):
        os.remove(file)
    cr = CrashRecoverer(file)

    try:
        with cr.enter_critical_section({"int": 1}):
            raise NotImplementedError("test")
    except NotImplementedError:
        assert cr.is_in_recovery_mode()
        assert cr.get_recovery_hint() == {
            "int": 1,
            "exception": "test",
            "exception_type": "NotImplementedError",
        }

    cr = CrashRecoverer(file)
    assert cr.is_in_recovery_mode()
    assert cr.get_recovery_hint() == {
        "int": 1,
        "exception": "test",
        "exception_type": "NotImplementedError",
    }

    try:
        with cr.enter_critical_section({}):
            pass
    except ValueError:
        pass

    assert cr.get_recovery_hint() == {
        "int": 1,
        "exception": "test",
        "exception_type": "NotImplementedError",
    }

    cr.leave_recovery_mode()
    assert not cr.is_in_recovery_mode()
    assert not os.path.exists(file)


def test_batch_works():
    assert [list(b) for b in batch(range(763638, 763639 + 1), n=1)] == [
        [763638],
        [763639],
    ]
    assert [list(b) for b in batch(range(763638, 763639 + 1), n=2)] == [
        [763638, 763639]
    ]
    assert [list(b) for b in batch(range(763638, 763640 + 1), n=2)] == [
        [763638, 763639],
        [763640],
    ]


def test_subkey_exists():
    assert subkey_exists({}, ["abc"]) is False
    assert subkey_exists({"abc": None}, ["abc"]) is True
    assert subkey_exists({"abc": []}, ["abc", "cbd"]) is False
    assert subkey_exists({"abc": {"cbd": 1}}, ["abc", "cbd"]) is True
    assert subkey_exists({"abc": {"cbd": None}}, ["abc", "cbd"]) is True
