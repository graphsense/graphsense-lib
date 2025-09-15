import bisect
import itertools
from datetime import timedelta
from typing import Iterable, Optional, Sequence, Any, Union, List

import pandas as pd
import base64
import sys

max_int64 = 2**63 - 1


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


def dict_to_dataobject(d: dict) -> Union[DataObject, List[DataObject]]:
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
    if not camel_str:
        return camel_str

    result = []
    for i, c in enumerate(camel_str):
        # Add underscore before uppercase letter if:
        # 1. Not the first character AND
        # 2. Previous character was lowercase OR
        # 3. Previous character was a digit OR
        # 4. Next character is lowercase (handles acronyms like "XMLHttp")
        if c.isupper() and i > 0:
            prev_char = camel_str[i - 1]
            next_char = camel_str[i + 1] if i + 1 < len(camel_str) else ""

            if (
                prev_char.islower()
                or prev_char.isdigit()
                or (next_char and next_char.islower())
            ):
                result.append("_")

        result.append(c.lower())

    return "".join(result)


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


def subkey_get(item, key_list) -> Optional[Any]:
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


class RangeManager:
    def __init__(self):
        self.ranges = []

    def add_index(self, index):
        """Add an index to the manager and merge with existing ranges if possible."""
        new_range = [index, index]
        merged = []
        i = 0
        n = len(self.ranges)

        while i < n:
            current_start, current_end = self.ranges[i]

            if new_range[0] <= current_end + 1 and current_start <= new_range[1] + 1:
                new_range[0] = min(new_range[0], current_start)
                new_range[1] = max(new_range[1], current_end)
            else:
                merged.append(self.ranges[i])
            i += 1

        merged.append(new_range)
        merged.sort()
        self.ranges = merged

    def add_range(self, start, end):
        """Add a range of indices to the manager and merge with existing ranges if possible."""
        new_range = [start, end]
        merged = []
        i = 0
        n = len(self.ranges)

        while i < n:
            current_start, current_end = self.ranges[i]

            if new_range[0] <= current_end + 1 and current_start <= new_range[1] + 1:
                new_range[0] = min(new_range[0], current_start)
                new_range[1] = max(new_range[1], current_end)
            else:
                merged.append(self.ranges[i])
            i += 1

        merged.append(new_range)
        merged.sort()
        self.ranges = merged

    def is_processed(self, index):
        """Check if an index is covered by any of the ranges."""
        for start, end in self.ranges:
            if start <= index <= end:
                return True
        return False

    def get_ranges(self):
        """Return the current list of ranges."""
        return self.ranges

    def find_smallest_unprocessed(self):
        """Find the smallest integer not yet processed."""
        smallest_unprocessed = 0
        for start, end in self.ranges:
            if start > smallest_unprocessed:
                return smallest_unprocessed
            smallest_unprocessed = end + 1
        return smallest_unprocessed

    def serialize(self):
        """Serialize the RangeManager object to bytes."""
        flat_list = []
        for start, end in self.ranges:
            flat_list.append(start)
            flat_list.append(end)

        byte_data = bytearray()
        for num in flat_list:
            byte_data.extend(num.to_bytes(4, byteorder="big"))

        return bytes(byte_data)

    @classmethod
    def deserialize(cls, data):
        """Deserialize bytes back to a RangeManager object."""
        range_manager = cls()

        flat_list = []
        for i in range(0, len(data), 4):
            num = int.from_bytes(data[i : i + 4], byteorder="big")
            flat_list.append(num)

        ranges = []
        for i in range(0, len(flat_list), 2):
            ranges.append([flat_list[i], flat_list[i + 1]])

        range_manager.ranges = ranges
        return range_manager


def custom_json_encoder(obj):
    if isinstance(obj, bytes):
        return {"type": "bytes", "value": base64.b64encode(obj).decode("utf-8")}
    if isinstance(obj, int) and (obj > max_int64 or obj < -max_int64):
        return {"type": "bytes", "value": str(obj)}
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def custom_json_decoder(dct):
    if "type" in dct and dct["type"] == "bytes" and "value" in dct:
        return base64.b64decode(dct["value"].encode("utf-8"))
    if "type" in dct and dct["type"] == "int" and "value" in dct:
        return int(dct["value"])
    return dct


def filter_sensitive_sys_argv(
    argv: Optional[List[str]] = None, sensitive_keys: Optional[List[str]] = None
) -> List[str]:
    """
    Returns a filtered version of sys.argv with sensitive parameters masked.

    Args:
        argv (list): List of command-line arguments (defaults to sys.argv).
        sensitive_keys (list): List of parameter names to filter (case-insensitive).

    Returns:
        List of arguments with sensitive values replaced by '***'.
    """
    if argv is None:
        argv = sys.argv
    if sensitive_keys is None:
        sensitive_keys = ["password", "token", "secret", "apikey", "api_key", "-u"]

    filtered = []
    skip_next = False
    for i, arg in enumerate(argv):
        if skip_next:
            filtered.append("***")
            skip_next = False
            continue
        lower_arg = arg.lower()
        # Match --key=value or --key value
        if any(key in lower_arg for key in sensitive_keys):
            if "=" in arg:
                key, _ = arg.split("=", 1)
                filtered.append(f"{key}=***")
            else:
                filtered.append(arg)
                skip_next = True  # Mask next value
        else:
            filtered.append(arg)
    return filtered


# Example usage:
# safe_argv = filter_sensitive_sys_argv()
