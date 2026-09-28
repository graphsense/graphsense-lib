"""The order in which a non-atomic apply writes a batch (WritePhase).

A reader must never follow a reference to a row that is not written yet: prod
served an address whose cluster row was still missing and answered
/addresses/{a}/cluster with a 500.
"""

import inspect
import random
import re

import pytest
from cassandra import InvalidRequest

from graphsenselib.datatypes import DbChangeType, EntityType
from graphsenselib.db.analytics import ApplyChangesResult, DbChange
from graphsenselib.db.parallel import worker_apply_changes
from graphsenselib.deltaupdate.update import generic
from graphsenselib.deltaupdate.update.abstractupdater import TABLE_NAME_DELTA_HISTORY
from graphsenselib.deltaupdate.update.account import createchanges
from graphsenselib.deltaupdate.update.account import update as account_update
from graphsenselib.deltaupdate.update.utxo import update as utxo_update
from graphsenselib.deltaupdate.update.utxo.update import (
    _WRITE_PHASE_BY_TABLE,
    WritePhase,
    apply_changes,
    split_into_write_phases,
    write_phase,
)

OK = ApplyChangesResult(attempts_made=1, total_retry_wait_seconds=0.0)


def new(table, **data):
    return DbChange.new(table=table, data=data)


def upd(table, **data):
    return DbChange.update(table=table, data=data)


def delete(table, **data):
    return DbChange.delete(table=table, data=data)


# Before the batch: addresses 1-7 (legacy singletons), fresh clusters 2 = {2, 3}
# and 4 = {4, 5}; 6 and 7 are fresh singletons.
ADDRESSES = [(1, "y"), (2, "a2"), (3, "a3"), (4, "a4"), (5, "a5"), (6, "a6"), (7, "a7")]
PRE = {
    "address": {
        (i,): {"address_id": i, "address": a, "cluster_id": i} for i, a in ADDRESSES
    },
    "cluster": {(i,): {"cluster_id": i} for i, _ in ADDRESSES},
    "address_ids_by_address_prefix": {
        (a,): {"address": a, "address_id": i} for i, a in ADDRESSES
    },
    "fresh_address_cluster": {
        (a,): {"address_id": a, "cluster_id": c}
        for a, c in [(2, 2), (3, 2), (4, 4), (5, 4)]
    },
    "fresh_cluster_addresses": {
        (c, a): {"cluster_id": c, "address_id": a}
        for c, a in [(2, 2), (2, 3), (4, 4), (4, 5)]
    },
    "fresh_cluster_stats": {(c,): {"cluster_id": c} for c in (2, 4)},
}

# The batch's raw txs, as the addresses REST resolves by string.
TX_IO = {100: ["y", "x"], 101: ["a3", "a4", "x", "y"], 102: ["a6", "a7", "y"]}

# What the UTXO updater writes for tx 100 (y pays the new address x = 10),
# tx 101 (a3, a4 and x co-spend to y: fresh cluster 4 is absorbed into 2, and
# x joins 2) and tx 102 (a6 and a7 co-spend to y: new fresh cluster 6).
BATCH = [
    new("address", address_id=10, address="x", cluster_id=10),
    upd("address", address_id=1, address="y", cluster_id=1),
    upd("address", address_id=3, address="a3", cluster_id=3),
    upd("address", address_id=4, address="a4", cluster_id=4),
    new("cluster", cluster_id=10),
    upd("cluster", cluster_id=1),
    upd("cluster", cluster_id=3),
    upd("cluster", cluster_id=4),
    upd("address", address_id=6, address="a6", cluster_id=6),
    upd("address", address_id=7, address="a7", cluster_id=7),
    upd("cluster", cluster_id=6),
    upd("cluster", cluster_id=7),
    new("fresh_cluster_stats", cluster_id=2),
    new("fresh_cluster_stats", cluster_id=6),
    new("address_ids_by_address_prefix", address="x", address_id=10),
    new("address_outgoing_relations", src_address_id=1, dst_address_id=10),
    new("address_incoming_relations", dst_address_id=10, src_address_id=1),
    new("address_outgoing_relations", src_address_id=10, dst_address_id=1),
    new("address_incoming_relations", dst_address_id=1, src_address_id=10),
    new("cluster_outgoing_relations", src_cluster_id=1, dst_cluster_id=10),
    new("cluster_incoming_relations", dst_cluster_id=10, src_cluster_id=1),
    new("cluster_addresses", cluster_id=10, address_id=10),
    new("address_transactions", address_id=1, tx_id=100),
    new("address_transactions", address_id=10, tx_id=100),
    new("cluster_transactions", cluster_id=1, tx_id=101),
    new("fresh_address_cluster", address_id=4, cluster_id=2),
    new("fresh_address_cluster", address_id=5, cluster_id=2),
    new("fresh_address_cluster", address_id=10, cluster_id=2),
    new("fresh_cluster_addresses", cluster_id=2, address_id=4),
    new("fresh_cluster_addresses", cluster_id=2, address_id=5),
    new("fresh_cluster_addresses", cluster_id=2, address_id=10),
    new("address_transactions", address_id=6, tx_id=102),
    new("address_transactions", address_id=7, tx_id=102),
    new("fresh_address_cluster", address_id=6, cluster_id=6),
    new("fresh_address_cluster", address_id=7, cluster_id=6),
    new("fresh_cluster_addresses", cluster_id=6, address_id=6),
    new("fresh_cluster_addresses", cluster_id=6, address_id=7),
    delete("fresh_cluster_addresses", cluster_id=4, address_id=4),
    delete("fresh_cluster_addresses", cluster_id=4, address_id=5),
    delete("fresh_cluster_stats", cluster_id=4),
    new("summary_statistics", id=0),
    new(TABLE_NAME_DELTA_HISTORY, last_synced_block=968489),
]

KEYS = {
    "address": ("address_id",),
    "cluster": ("cluster_id",),
    "fresh_cluster_stats": ("cluster_id",),
    "address_ids_by_address_prefix": ("address",),
    "address_incoming_relations": ("dst_address_id", "src_address_id"),
    "address_outgoing_relations": ("src_address_id", "dst_address_id"),
    "cluster_incoming_relations": ("dst_cluster_id", "src_cluster_id"),
    "cluster_outgoing_relations": ("src_cluster_id", "dst_cluster_id"),
    "cluster_addresses": ("cluster_id", "address_id"),
    "fresh_address_cluster": ("address_id",),
    "fresh_cluster_addresses": ("cluster_id", "address_id"),
    "address_transactions": ("address_id", "tx_id"),
    "cluster_transactions": ("cluster_id", "tx_id"),
    "summary_statistics": ("id",),
    TABLE_NAME_DELTA_HISTORY: ("last_synced_block",),
}


def shuffled(changes):
    changes = list(changes)
    random.Random(7).shuffle(changes)
    return changes


def _apply(state, changes):
    after = {table: dict(state.get(table, {})) for table in KEYS}
    for change in changes:
        key = tuple(change.data[k] for k in KEYS[change.table])
        if change.action == DbChangeType.DELETE:
            after[change.table].pop(key, None)
        else:
            after[change.table][key] = change.data
    return after


def _dangling(reach, present):
    """References a reader follows from every address findable by its string
    whose target row is not in `present`. `reach` maps each row key to every
    version a reader may see (before or after an overwrite)."""
    missing = set()
    seen = set()

    def target(table, key):
        if key not in present[table]:
            missing.add((table, key))
        return reach[table].get(key, [])

    def pointing(table, **match):
        return [
            row
            for rows in reach[table].values()
            for row in rows
            if all(row[k] == v for k, v in match.items())
        ]

    def visit(kind, key):
        if (kind, key) in seen:
            return
        seen.add((kind, key))
        if kind == "address":
            for row in target("address", (key,)):
                visit("cluster", row["cluster_id"])
            for row in reach["fresh_address_cluster"].get((key,), []):
                visit("fresh_cluster", row["cluster_id"])
            for row in pointing("address_outgoing_relations", src_address_id=key):
                visit("address", row["dst_address_id"])
            for row in pointing("address_incoming_relations", dst_address_id=key):
                visit("address", row["src_address_id"])
            for row in pointing("address_transactions", address_id=key):
                visit("tx", row["tx_id"])
        elif kind == "cluster":
            if target("cluster", (key,)):
                # the root address: the address row whose id is the cluster id
                visit("address", key)
            for row in pointing("cluster_outgoing_relations", src_cluster_id=key):
                visit("cluster", row["dst_cluster_id"])
            for row in pointing("cluster_incoming_relations", dst_cluster_id=key):
                visit("cluster", row["src_cluster_id"])
            for row in pointing("cluster_addresses", cluster_id=key):
                visit("address", row["address_id"])
            for row in pointing("cluster_transactions", cluster_id=key):
                visit("tx", row["tx_id"])
        elif kind == "fresh_cluster":
            if target("fresh_cluster_stats", (key,)):
                visit("address", key)  # the fresh root: its min member id
            for row in pointing("fresh_cluster_addresses", cluster_id=key):
                visit("address", row["address_id"])
        elif kind == "tx":
            # /clusters/{c}/links looks a listed tx's addresses up by string
            for address in TX_IO[key]:
                for row in target("address_ids_by_address_prefix", (address,)):
                    visit("address", row["address_id"])

    for rows in list(reach["address_ids_by_address_prefix"].values()):
        for row in rows:
            visit("address", row["address_id"])
    return missing


def dangling_while_writing(phases):
    """Every reference that dangles at some moment while the phases are
    written one after the other. Within a phase any subset of its changes
    may be visible: a reader can see both versions of an overwritten row,
    and only rows present before and after the phase are sure to exist."""
    found = set()
    state = _apply(PRE, [])
    for phase in phases:
        after = _apply(state, phase)
        reach = {
            table: {
                key: [
                    row
                    for row in (state[table].get(key), after[table].get(key))
                    if row is not None
                ]
                for key in state[table].keys() | after[table].keys()
            }
            for table in KEYS
        }
        present = {table: state[table].keys() & after[table].keys() for table in KEYS}
        found |= _dangling(reach, present)
        state = after
    return found


def test_state_before_the_batch_is_consistent():
    assert dangling_while_writing([[]]) == set()


def test_phased_write_never_exposes_a_dangling_reference():
    assert dangling_while_writing(split_into_write_phases(shuffled(BATCH))) == set()


def test_writing_the_batch_in_one_go_exposes_the_prod_failure():
    found = dangling_while_writing([BATCH])
    assert ("address", (10,)) in found  # prefix row before the address row
    assert ("cluster", (10,)) in found  # address row before its cluster row


@pytest.mark.parametrize(
    "first, second",
    [
        (WritePhase.RECORDS, WritePhase.PUBLISH),
        (WritePhase.PUBLISH, WritePhase.EDGES),
        (WritePhase.EDGES, WritePhase.UNLINKS),
    ],
)
def test_each_phase_boundary_is_needed(first, second):
    merged = {}
    for change in BATCH:
        phase = write_phase(change)
        merged.setdefault(first if phase == second else phase, []).append(change)
    phases = [merged[phase] for phase in sorted(merged)]
    assert dangling_while_writing(phases) != set()


def test_write_phase_of_each_kind_of_row():
    assert write_phase(new("cluster", cluster_id=1)) == WritePhase.RECORDS
    assert write_phase(upd("address", address_id=1)) == WritePhase.RECORDS
    assert (
        write_phase(new("address_ids_by_address_prefix", address="x"))
        == WritePhase.PUBLISH
    )
    assert write_phase(new("cluster_transactions", cluster_id=1)) == WritePhase.EDGES
    assert write_phase(new("fresh_cluster_stats", cluster_id=4)) == WritePhase.RECORDS
    assert write_phase(delete("fresh_cluster_stats", cluster_id=4)) == (
        WritePhase.UNLINKS
    )
    assert write_phase(new("summary_statistics", id=0)) == WritePhase.BOOKKEEPING


def test_every_table_the_updaters_write_has_a_phase():
    """A table a change builder starts writing must get a phase here, not
    stop a prod batch at apply time."""
    written = {TABLE_NAME_DELTA_HISTORY}
    for module in (generic, createchanges, account_update, utxo_update):
        source = inspect.getsource(module)
        written |= set(re.findall(r'table="(\w+)"', source))
        written |= set(re.findall(r'tablename = "(\w+)"', source))
        for suffix in re.findall(r'table=f"\{mode\}(\w*)"', source):
            written |= {f"{str(mode)}{suffix}" for mode in EntityType}
    assert len(written) >= 20  # the scan still finds the builders' tables
    assert written - _WRITE_PHASE_BY_TABLE.keys() == set()


class RecordingTransformed:
    def __init__(self, atomic_error=None):
        self.atomic_error = atomic_error
        self.calls = []

    def apply_changes(self, changes, atomic):
        if atomic and self.atomic_error is not None:
            raise self.atomic_error
        self.calls.append((list(changes), atomic))
        return OK


class RecordingDb:
    def __init__(self, atomic_error=None):
        self.transformed = RecordingTransformed(atomic_error)


class RecordingPool:
    def __init__(self):
        self.dispatched = []

    def map_chunked(self, fn, items):
        self.dispatched.append((fn, list(items)))
        return [OK]


PHASE_ORDER = [
    {WritePhase.RECORDS},
    {WritePhase.PUBLISH},
    {WritePhase.EDGES},
    {WritePhase.UNLINKS},
    {WritePhase.BOOKKEEPING},
]


def test_single_process_writes_phase_after_phase():
    db = RecordingDb()
    apply_changes(db, shuffled(BATCH), pedantic=False, try_atomic_writes=False)
    calls = db.transformed.calls
    assert [{write_phase(c) for c in changes} for changes, _ in calls] == PHASE_ORDER
    assert not any(atomic for _, atomic in calls)
    assert sorted(map(repr, (c for changes, _ in calls for c in changes))) == sorted(
        map(repr, BATCH)
    )


def test_pool_gets_one_dispatch_per_phase():
    pool = RecordingPool()
    apply_changes(None, shuffled(BATCH), False, try_atomic_writes=False, pool=pool)
    assert all(fn is worker_apply_changes for fn, _ in pool.dispatched)
    assert [{write_phase(c) for c in items} for _, items in pool.dispatched] == (
        PHASE_ORDER
    )


def test_atomic_write_stays_one_logged_batch():
    db = RecordingDb()
    changes = shuffled(BATCH)
    apply_changes(db, changes, pedantic=False, try_atomic_writes=True)
    assert db.transformed.calls == [(changes, True)]


def test_too_large_atomic_batch_falls_back_to_phases():
    db = RecordingDb(atomic_error=InvalidRequest("Batch too large"))
    apply_changes(db, shuffled(BATCH), pedantic=False, try_atomic_writes=True)
    calls = db.transformed.calls
    assert [{write_phase(c) for c in changes} for changes, _ in calls] == PHASE_ORDER


@pytest.mark.parametrize("use_pool", [False, True])
def test_table_without_a_phase_fails_before_any_write(use_pool):
    db, pool = RecordingDb(), RecordingPool()
    changes = [new("address", address_id=1), new("not_a_table", k=1)]
    with pytest.raises(ValueError, match="not_a_table"):
        apply_changes(
            db,
            changes,
            pedantic=False,
            try_atomic_writes=False,
            pool=pool if use_pool else None,
        )
    assert db.transformed.calls == []
    assert pool.dispatched == []
