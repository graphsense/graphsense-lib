# Pickle here mirrors multiprocessing's own IPC transport: PlainRow objects
# only ever cross the pipe between this process and worker processes it
# spawned itself — never data from an untrusted source.
import pickle
from collections import namedtuple

import pytest

from graphsenselib.db.parallel import PlainRow, flatten_value

# Simulates a driver row from named_tuple_factory.
AddressRow = namedtuple(
    "AddressRow", ["address_id", "address", "total_received", "token_values"]
)
# Simulates a dynamically generated driver UDT value (namedtuple-based).
CurrencyUdt = namedtuple("CurrencyUdt", ["value", "fiat_values"])


def test_flatten_namedtuple_row_gives_attribute_access():
    row = AddressRow(
        address_id=7, address=b"\x01\x02", total_received=None, token_values=None
    )
    flat = flatten_value(row)
    assert isinstance(flat, PlainRow)
    assert flat.address_id == 7
    assert flat.address == b"\x01\x02"
    assert flat.total_received is None


def test_flatten_nested_udt_value():
    row = AddressRow(
        address_id=1,
        address=b"\x00",
        total_received=CurrencyUdt(value=100, fiat_values=[1.5, 2.5]),
        token_values=None,
    )
    flat = flatten_value(row)
    assert flat.total_received.value == 100
    assert flat.total_received.fiat_values == [1.5, 2.5]


def test_flatten_map_of_udts_preserves_dict_and_flattens_values():
    row = AddressRow(
        address_id=1,
        address=b"\x00",
        total_received=None,
        token_values={"USDT": CurrencyUdt(value=5, fiat_values=[0.1, 0.2])},
    )
    flat = flatten_value(row)
    assert isinstance(flat.token_values, dict)
    assert flat.token_values["USDT"].value == 5
    assert flat.token_values["USDT"].fiat_values == [0.1, 0.2]


def test_plain_scalars_pass_through():
    for v in (42, "x", b"\xff", None, True, 1.5):
        assert flatten_value(v) == v or (v is None and flatten_value(v) is None)


def test_flattened_row_has_no_item_access():
    # The driver's UDT serializer tries val[i] first and falls back to
    # getattr only on TypeError, so item access must raise TypeError.
    flat = flatten_value(CurrencyUdt(value=1, fiat_values=[0.0]))
    with pytest.raises(TypeError):
        flat[0]


def test_flattened_udt_serializes_via_getattr_fallback():
    # Replicates cassandra.cqltypes.UserType.serialize_safe access order.
    flat = flatten_value(CurrencyUdt(value=9, fiat_values=[3.0]))
    extracted = []
    for i, fieldname in enumerate(["value", "fiat_values"]):
        try:
            item = flat[i]
        except TypeError:
            item = getattr(flat, fieldname, None)
        extracted.append(item)
    assert extracted == [9, [3.0]]


def test_missing_attribute_raises_attributeerror():
    flat = flatten_value(CurrencyUdt(value=1, fiat_values=[]))
    with pytest.raises(AttributeError):
        _ = flat.does_not_exist
    assert not hasattr(flat, "does_not_exist")


def test_pickle_roundtrip_preserves_structure():
    row = AddressRow(
        address_id=3,
        address=b"\xaa",
        total_received=CurrencyUdt(value=7, fiat_values=[1.0]),
        token_values={"WTRX": CurrencyUdt(value=2, fiat_values=[0.5])},
    )
    flat = pickle.loads(pickle.dumps(flatten_value(row)))
    assert flat.address_id == 3
    assert flat.total_received.value == 7
    assert flat.token_values["WTRX"].fiat_values == [0.5]


def test_flatten_list_of_udts():
    vals = [CurrencyUdt(value=1, fiat_values=[]), CurrencyUdt(value=2, fiat_values=[])]
    flat = flatten_value(vals)
    assert [v.value for v in flat] == [1, 2]


def test_plain_rows_equal_when_data_equal():
    a = flatten_value(CurrencyUdt(value=1, fiat_values=[2.0]))
    b = flatten_value(CurrencyUdt(value=1, fiat_values=[2.0]))
    assert a == b


# --- ParallelDbPool machinery -------------------------------------------
# Worker functions must be module-level so the spawn context can import
# them by qualified name in the child process.

_WORKER_STATE = {}


def _init_test_worker(marker):
    _WORKER_STATE["marker"] = marker


def _square_chunk(chunk):
    return [x * x for x in chunk]


def _read_marker_chunk(chunk):
    return [_WORKER_STATE["marker"] for _ in chunk]


def test_pool_map_chunked_preserves_order_and_completeness():
    from graphsenselib.db.parallel import ParallelDbPool

    with ParallelDbPool(
        num_workers=2, initializer=_init_test_worker, initargs=("m",)
    ) as pool:
        result = pool.map_chunked(_square_chunk, list(range(100)))
    assert result == [x * x for x in range(100)]


def test_pool_runs_initializer_in_workers():
    from graphsenselib.db.parallel import ParallelDbPool

    with ParallelDbPool(
        num_workers=2, initializer=_init_test_worker, initargs=("sentinel",)
    ) as pool:
        result = pool.map_chunked(_read_marker_chunk, [1, 2, 3, 4])
    assert result == ["sentinel"] * 4


def test_pool_map_chunked_empty_input():
    from graphsenselib.db.parallel import ParallelDbPool

    with ParallelDbPool(
        num_workers=2, initializer=_init_test_worker, initargs=("m",)
    ) as pool:
        assert pool.map_chunked(_square_chunk, []) == []


def _chunk_size_chunk(chunk):
    # one entry per chunk so the parent can observe the split
    return [("chunk", len(chunk))]


def test_pool_splits_work_into_num_workers_chunks():
    from graphsenselib.db.parallel import ParallelDbPool

    with ParallelDbPool(
        num_workers=3, initializer=_init_test_worker, initargs=("m",)
    ) as pool:
        result = pool.map_chunked(_chunk_size_chunk, list(range(10)))
    sizes = [n for _, n in result]
    assert sum(sizes) == 10
    assert len(sizes) == 3
    # near-even split: no chunk more than one item larger than another
    assert max(sizes) - min(sizes) <= 1
