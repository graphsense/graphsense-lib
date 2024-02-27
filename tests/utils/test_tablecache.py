import pytest
from diskcache import Cache

from graphsenselib.utils.cache import TableBasedCache


def test_cache():
    c = TableBasedCache(Cache())

    c[("abc", 1)] = 1
    c[("abc", 2)] = 2

    assert c[("abc", 1)] == 1
    assert c[("abc", 2)] == 2

    c.put_items("abc", [(1, 3), (2, 4)])

    assert c[("abc", 1)] == 3
    assert c[("abc", 2)] == 4

    data = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 2},
        {"a": 1, "b": 3},
    ]

    c.put_items_keyed_by("abc", data, key="a")

    assert len(c.get_item("abc", 1)) == 2
    assert c.get_item("abc", 1) == [{"a": 1, "b": 2}, {"a": 1, "b": 3}]
    assert len(c.get_item("abc", 3)) == 1
    assert c.get_item("abc", 2) == 4

    c.delete_item("abc", 1)
    with pytest.raises(KeyError):
        assert len(c.get_item("abc", 1)) == 2

    assert c.get_item("abc", 2) == 4

    del c[("abc", 2)]

    with pytest.raises(KeyError):
        c.get_item("abc", 2)

    assert c.get(("abc", 2), None) is None
    assert c.get(("abc", 2), []) == []
