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


def test_plainrow_replace_overrides_field_and_keeps_others():
    # The UTXO updater calls tx._replace(inputs=...) on coinbase rows, so
    # flattened rows must mirror namedtuple._replace semantics.
    flat = flatten_value(
        AddressRow(
            address_id=1, address=b"\x00", total_received=None, token_values=None
        )
    )
    replaced = flat._replace(address_id=9)
    assert isinstance(replaced, PlainRow)
    assert replaced.address_id == 9
    assert replaced.address == b"\x00"
    assert flat.address_id == 1


def test_plainrow_replace_rejects_unknown_field():
    flat = flatten_value(CurrencyUdt(value=1, fiat_values=[]))
    with pytest.raises(ValueError):
        flat._replace(does_not_exist=5)


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


def _ignore_signals_init():
    # Mirrors the signal handling that init_worker installs in real workers:
    # ignore SIGINT/SIGTERM so a signal to the process group cannot tear a
    # worker down mid-chunk. (The DB session part of init_worker needs a real
    # cluster, so it is exercised separately by integration tests.)
    import signal as _signal

    _signal.signal(_signal.SIGINT, _signal.SIG_IGN)
    _signal.signal(_signal.SIGTERM, _signal.SIG_IGN)


def _signal_self_then_return_chunk(chunk):
    import os
    import signal as _signal

    # Without SIG_IGN this terminates the worker and breaks the pool; with the
    # init handler it is a no-op and the chunk completes normally.
    os.kill(os.getpid(), _signal.SIGTERM)
    return [x + 1 for x in chunk]


def test_worker_survives_signal_when_init_ignores_it():
    from graphsenselib.db.parallel import ParallelDbPool

    with ParallelDbPool(
        num_workers=2, initializer=_ignore_signals_init, initargs=()
    ) as pool:
        result = pool.map_chunked(_signal_self_then_return_chunk, list(range(20)))
    assert result == [x + 1 for x in range(20)]


def _get_signal_dispositions_chunk(chunk):
    import signal

    return [
        (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM))
        for _ in chunk
    ]


def test_pool_workers_ignore_termination_signals():
    # Terminal Ctrl-C signals the whole foreground process group. Workers
    # must ignore SIGINT/SIGTERM so the parent's graceful flag-based
    # shutdown can finish the batch instead of dying on BrokenProcessPool
    # mid-write; workers only stop via pool shutdown.
    import signal

    from graphsenselib.db.parallel import ParallelDbPool

    with ParallelDbPool(
        num_workers=1, initializer=_init_test_worker, initargs=("m",)
    ) as pool:
        [(sigint, sigterm)] = pool.map_chunked(_get_signal_dispositions_chunk, [1])
    assert sigint == signal.SIG_IGN
    assert sigterm == signal.SIG_IGN


def test_pool_still_runs_caller_initializer_with_signal_guard():
    from graphsenselib.db.parallel import ParallelDbPool

    with ParallelDbPool(
        num_workers=1, initializer=_init_test_worker, initargs=("guarded",)
    ) as pool:
        result = pool.map_chunked(_read_marker_chunk, [1])
    assert result == ["guarded"]


def test_plainrow_serializes_identically_to_driver_udt_value():
    # Characterization test against the real driver serializer: a flattened
    # UDT value must produce the same wire bytes as the driver's own
    # namedtuple-based UDT value when bound for a write.
    from cassandra.cqltypes import FloatType, ListType, LongType, UserType

    udt_class = UserType.make_udt_class(
        keyspace="ks",
        udt_name="currency",
        field_names=("value", "fiat_values"),
        field_types=(LongType, ListType.apply_parameters([FloatType])),
    )
    native = CurrencyUdt(value=12345, fiat_values=[1.5, 2.5])
    flattened = flatten_value(native)
    assert udt_class.serialize_safe(flattened, 4) == udt_class.serialize_safe(native, 4)
