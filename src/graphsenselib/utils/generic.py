import bisect
import itertools
from datetime import timedelta
from typing import Iterable, Optional, Sequence

import pandas as pd


class DataObject:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def as_dict(self):
        return self.__dict__

    def __repr__(self):
        keys = sorted(self.__dict__)
        items = ("{}={!r}".format(k, self.__dict__[k]) for k in keys)
        return "{}({})".format(type(self).__name__, ", ".join(items))

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def dict_to_dataobject(d: dict) -> DataObject:
    if isinstance(d, dict):
        obj = DataObject(**d)
        for k, v in obj.__dict__.items():
            obj.__dict__[k] = dict_to_dataobject(v)
        return obj
    if isinstance(d, list):
        return [dict_to_dataobject(x) for x in d]
    else:
        return d


class GenericArrayFacade:
    def __init__(self, getter_fun):
        self.getter = getter_fun

    def __getitem__(self, key):
        return self.getter(key)


def remove_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix) :]
    else:
        return s


def group_by(lst: Iterable, key) -> dict:
    groups = itertools.groupby(sorted(lst, key=key), key=key)
    return {k: list(v) for k, v in groups}


def groupby_property(
    lst: Iterable, property_name: str, sort_by: Optional[str] = None
) -> dict:
    def keyfun(x):
        return getattr(x, property_name)

    groups = group_by(lst, key=keyfun)

    return {
        k: (v if sort_by is None else sorted(v, key=lambda x: getattr(x, sort_by)))
        for k, v in groups.items()
    }


def camel_to_snake_case(camel_str: str) -> str:
    return "".join([f"_{c.lower()}" if c.isupper() else c for c in camel_str])


def dict_with_snake_keys(d) -> dict:
    if not isinstance(d, dict):
        return d
    return {camel_to_snake_case(k): dict_with_snake_keys(v) for k, v in d.items()}


def get_cassandra_result_as_dateframe(result):
    df = pd.DataFrame(result)
    return df


def fallback(*args):
    for arg in args:
        if arg is not None:
            return arg


def split_list_on_condition(lst, condition):
    return [x for x in lst if condition(x)], [x for x in lst if not condition(x)]


def remove_multi_whitespace(string):
    return " ".join(string.split())


def no_nones(lst):
    return [item for item in lst if item is not None]


def flatten(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]


def batch(iterable, n=1):
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx : min(ndx + n, length)]


def binary_search(L, x, lo, hi):
    i = bisect.bisect_left(L, x, lo=lo, hi=hi)
    if i == hi or L[i] != x:
        return -1
    return i


def pandas_row_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def subkey_exists(item, key_list) -> bool:
    if item is None and len(key_list) > 0:
        return False
    if not key_list:
        return True
    h, *rest = key_list
    if h in item:
        return subkey_exists(item[h], rest)
    else:
        return False


def subkey_get(item, key_list) -> bool:
    if item is None and len(key_list) > 0:
        return item
    if not key_list:
        return item
    h, *rest = key_list
    if h.isdigit() and isinstance(item, list) and int(h) < len(item):
        return subkey_get(item[int(h)], rest)
    elif h in item:
        return subkey_get(item[h], rest)
    else:
        return None


def first_or_default(seq: Sequence[object], pred, default=None):
    return next(filter(pred, seq), default)


def batch_date(da, db, **kwargs):
    step = timedelta(**kwargs)
    while da <= db:
        start_old = da
        da += step
        if da > db:
            da = db
        yield (start_old, da)
        da += timedelta(days=1)


def generate_date_range_days(da, db):
    return itertools.takewhile(
        lambda x: x <= db,
        (da + timedelta(days=delta) for delta in itertools.count(start=0, step=1)),
    )


def truncateI32(number):
    result = number & 0xFFFFFFFF
    if result & (1 << 31):  # negative value
        result -= 1 << 32
    return result
