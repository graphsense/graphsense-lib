import os

from graphsenselib.utils import (
    batch,
    batch_date,
    bytes_to_hex,
    first_or_default,
    generate_date_range_days,
    remove_prefix,
    strip_0x,
    subkey_exists,
    subkey_get,
    to_int,
    truncateI32,
)
from graphsenselib.utils.errorhandling import CrashRecoverer
from graphsenselib.utils.generic import dict_to_dataobject


def test_dict_to_dataobject():
    d = {"a": 1}

    class Mock:
        a = 1

    obj2 = Mock()
    obj = dict_to_dataobject(d)

    assert obj.a == 1
    assert obj2.a == 1
    assert dict_to_dataobject(obj2).a == 1


def test_btoh_works(capsys):
    assert bytes_to_hex(b"") is None
    assert bytes_to_hex(b"asdfasdf") == "6173646661736466"


def test_strip_0x_works1():
    assert strip_0x("0xa9059cbb") == "a9059cbb"


def test_strip_0x_works2():
    assert strip_0x("a9059cbb") == "a9059cbb"
    assert strip_0x(None) is None


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


def test_subkey_get():
    assert subkey_get({}, ["abc"]) is None
    assert subkey_get({"abc": None}, ["abc"]) is None
    assert subkey_get({"abc": []}, ["abc", "cbd"]) is None
    assert subkey_get({"abc": {"cbd": 1}}, ["abc", "cbd"]) == 1
    assert subkey_get({"abc": {"cbd": "string"}}, ["abc", "cbd"]) == "string"
    assert (
        subkey_get({"abc": {"cbd": ["a", "b"], "bbb": [1, 2, 3]}}, ["abc", "cbd", "1"])
        == "b"
    )
    assert subkey_get(
        {"abc": {"cbd": ["a", "b"], "bbb": [1, 2, 3]}}, ["abc", "bbb"]
    ) == [1, 2, 3]


def test_first_or_default():
    assert first_or_default([1, 2, 3], lambda x: x > 2, default=10) == 3
    assert first_or_default([1, 2, 3], lambda x: x > 5, default=10) == 10
    assert first_or_default([1, 2, 3], lambda x: x > 5, default=None) is None


def test_remove_prefix():
    assert remove_prefix("0xa9059cbb", "0x") == "a9059cbb"
    assert remove_prefix("0xa9059cbb", "a0x") == "0xa9059cbb"


def test_date_range_works():
    from datetime import date

    a = date.fromisoformat("2010-03-12")
    b = date.fromisoformat("2024-05-19")

    batches = list(batch_date(a, b, days=180))

    deltas = [y - x for x, y in batches]

    assert all(d.days == 180 for d in deltas[:-1])

    assert batches[0][0] == a
    assert batches[-1][1] == b
    assert (sum(d.days for d in deltas) + len(deltas) - 1) == (b - a).days

    gen_dates = list(generate_date_range_days(a, b))

    assert gen_dates[0] == a
    assert gen_dates[-1] == b

    assert len(gen_dates) == (b - a).days + 1


def testTruncateI32():
    for i in range(0, 2147483647, 1000):
        assert truncateI32(i) == i

    assert truncateI32(2147483647) == 2147483647

    assert truncateI32(2147483648) == -2147483648
