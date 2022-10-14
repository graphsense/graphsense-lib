from graphsenselib.utils import bytes_to_hex, strip_0x, to_int


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
