"""Check a schema against the design rules.

The v2 equivalent normalises whitespace to compare CQL text and collapses runs of
spaces but not single ones, so ``frozen <currency>`` in the files never matches
``frozen<currency>`` from Cassandra: it false-positives on every UDT and
collection column, and is consequently ignored. Checking the model instead of the
rendered text removes that whole class of problem.
"""

from __future__ import annotations

from graphsense_v3.schema.model import Schema, Table

#: Partition-key columns that reintroduce the v2 pathology. A group derived from
#: a *block* or *transaction* id is fine -- rows per block are bounded, and the
#: bucket size is chosen for it. A group derived from an ENTITY is not: entity
#: sizes span nine orders of magnitude, so one bucket size simultaneously
#: over-partitions the median (v2 account address_transactions averages ~42 rows)
#: and under-partitions the tail (a measured 455M-row partition, and 1.86 GiB
#: against Cassandra's ~100 MB guideline).
_ENTITY_GROUPS = frozenset(
    {
        "address_id_group",
        "cluster_id_group",
        "address_id_secondary_group",
        "cluster_id_secondary_group",
    }
)


def _violations_for_table(table: Table, schema: Schema) -> list[str]:
    out: list[str] = []
    declared = table.column_names()

    for column in (*table.key.partition, *table.key.clustering):
        if column not in declared:
            out.append(f"{table.name}: key column {column!r} is not declared")

    for column, direction in table.key.order:
        if column not in table.key.clustering:
            out.append(f"{table.name}: CLUSTERING ORDER on non-clustering {column!r}")
        if direction not in ("ASC", "DESC"):
            out.append(f"{table.name}: bad clustering direction {direction!r}")

    if "compaction" not in table.options:
        out.append(f"{table.name}: no compaction strategy declared (rule 7)")

    for column in table.columns:
        bare = column.type.replace(" ", "")
        if bare.startswith(("list<", "set<", "map<")):
            out.append(f"{table.name}.{column.name}: unfrozen collection (rule 6)")
        if bare == "timestamp":
            out.append(
                f"{table.name}.{column.name}: use bigint seconds, not timestamp (rule 5)"
            )
        if (
            bare in ("int",)
            and column.name.endswith(("_tx_id", "_id"))
            and column.name not in _BOUNDED_32_BIT_IDS
        ):
            out.append(f"{table.name}.{column.name}: 32-bit id (rule 5)")
        if bare == "text" and column.name in ("address", "src_address", "dst_address"):
            out.append(f"{table.name}.{column.name}: addresses are blob, not text")

    for column in table.key.partition:
        if column in _ENTITY_GROUPS:
            out.append(
                f"{table.name}: entity group {column!r} in partition key (rule 1)"
            )

    if table.name.endswith("_secondary_ids"):
        out.append(
            f"{table.name}: watermark table -- the split factor is a constant (rule 2)"
        )

    return out


#: Ids rule 5 does not apply to: each is bounded by the protocol rather than by
#: our own surrogate-key counter, which is the thing that overflows (BTC's
#: address_id is at 72% of int32 with no cap). A block height, a within-block
#: log/trace index and a TRON TRC10 asset id are all int32 at the source, and
#: widening them here would only disagree with the lake.
_BOUNDED_32_BIT_IDS = frozenset(
    {"block_id", "log_index", "trace_index", "call_token_id"}
)


def violations(schema: Schema) -> list[str]:
    """Return every design-rule violation in ``schema``; empty means clean."""
    out: list[str] = []
    seen: set[str] = set()
    for table in schema.tables:
        if table.name in seen:
            out.append(f"{table.name}: defined twice")
        seen.add(table.name)
        out.extend(_violations_for_table(table, schema))

    declared_types = {t.name for t in schema.types}
    for table in schema.tables:
        for column in table.columns:
            bare = column.type.replace(" ", "")
            if bare.startswith("frozen<") and bare.endswith(">"):
                inner = bare[len("frozen<") : -1]
                if inner.isidentifier() and inner not in declared_types:
                    out.append(f"{table.name}.{column.name}: unknown type {inner!r}")
    return out
