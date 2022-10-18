import bisect
import logging

import pandas as pd


class GenericArrayFacade:
    def __init__(self, getter_fun):
        self.getter = getter_fun

    def __getitem__(self, key):
        return self.getter(key)


def get_cassandra_result_as_dateframe(result):
    df = pd.DataFrame(result)
    return df


def split_list_on_condition(list, condition):
    return [x for x in list if condition(x)], [x for x in list if not condition(x)]


def remove_mulit_whitespace(string):
    return " ".join(string.split())


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


def configure_logging(loglevel):
    log_format = (
        "%(asctime)s %(name)40s %(levelname)-8s | %(message)s"
        if loglevel > 1
        else "%(asctime)s %(levelname)-8s | %(message)s"
    )

    if loglevel == 0:
        loglevel = logging.WARNING
    elif loglevel == 1:
        loglevel = logging.INFO
    elif loglevel >= 2:
        loglevel = logging.DEBUG

    logging.basicConfig(
        format=log_format,
        level=loglevel,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.getLogger("cassandra").setLevel(logging.ERROR)
    logging.getLogger("Cluster").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)


def pandas_row_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
