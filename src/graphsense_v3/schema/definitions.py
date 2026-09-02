"""The v3 schema.

Design rules, each of which the v2 schema violates somewhere:

1. A partition key names one entity. No ``*_id_group`` on a table whose
   rows-per-entity is unbounded; a bucket survives only where rows-per-entity is
   bounded, and there it is the *whole* partition key with the entity as a
   clustering column (otherwise it reduces nothing).
2. The split factor is a constant, never a discovered maximum. No
   ``*_secondary_ids`` tables.
3. The ordering column sits immediately after the partition key. A discriminator
   that must be pushed down goes into the partition key.
4. The hot write path never reads. Aggregates are summable rows (see
   ``address_stats``); compaction is an optimisation, not a correctness
   requirement.
5. All ids and timestamps are 64-bit. Timestamps are ``bigint`` seconds, not
   ``timestamp``: the query helper interpolates WHERE values verbatim and accepts
   only str/int/float, so a type needing quoting breaks every caller.
6. Every collection is frozen. A non-frozen collection overwrite emits a range
   tombstone.
7. Every table declares its compaction, compression and caching.
"""

from __future__ import annotations

from graphsense_v3.schema.model import (
    BULK,
    CACHED,
    CHURN,
    LCS,
    STCS,
    Column as C,
    Family,
    Key,
    Kind,
    Schema,
    Table,
    UserType,
)

#: Clustering value of the compacted row in every ``*_stats`` table. Epochs above
#: it are un-absorbed deltas; a read sums the whole partition slice.
EPOCH_BASE = 0

CURRENCY = UserType(
    "currency",
    (
        C("value", "varint"),
        C("fiat_values", "frozen<map<text, double>>"),
    ),
)

TX_REFERENCE = UserType(
    "tx_reference",
    (
        C("trace_index", "int"),
        C("log_index", "int"),
    ),
)


def _housekeeping(kind: Kind) -> tuple[Table, ...]:
    """Tables both keyspace kinds carry."""
    tables = [
        Table(
            "configuration",
            (
                C("keyspace_name", "text"),
                C("entity_buckets", "int", "murmur3(entity) % this"),
                C("tx_page_size", "int", "rows per *_transactions partition"),
                C("relation_buckets", "int"),
                C("epoch_size", "int", "blocks per stats epoch"),
                C("address_prefix_length", "int"),
                C("tx_prefix_length", "int"),
                C("block_bucket_size", "int"),
                C(
                    "tx_block_bucket_size",
                    "int",
                    "blocks per transaction partition",
                ),
                C("fiat_currencies", "frozen<list<text>>"),
                C("schema_version", "int"),
            ),
            Key(("keyspace_name",)),
            CACHED,
        ),
        Table(
            "state",
            (C("key", "text"), C("value", "text"), C("updated_at", "bigint")),
            Key(("key",)),
            CACHED,
        ),
        Table(
            "summary_statistics",
            (
                C("id", "int"),
                C("timestamp", "bigint", "was int: unix seconds, 2038 cliff"),
                C("timestamp_transform", "bigint"),
                C("no_blocks", "bigint"),
                C("no_blocks_transform", "bigint"),
                C("no_transactions", "bigint"),
                C("no_addresses", "bigint"),
                C("no_address_relations", "bigint"),
            ),
            Key(("id",)),
            CACHED,
        ),
    ]
    return tuple(tables)


# --------------------------------------------------------------------------- #
# raw                                                                          #
# --------------------------------------------------------------------------- #

_BLOCK_BY_DATE = Table(
    "block_by_date",
    (C("day", "date"), C("timestamp", "bigint"), C("block_id", "int")),
    Key(
        ("day",), ("timestamp", "block_id"), (("timestamp", "ASC"), ("block_id", "ASC"))
    ),
    STCS,
    comment=(
        "Serves block-by-date as one partition slice. v2 had no such index, so the\n"
        "lookup was a ~25-read serial binary search or an ALLOW FILTERING full scan."
    ),
)


#: Both families address a transaction the same way (D13): the partition is a
#: run of blocks, the clustering key is the id, and the id is derivable from the
#: transaction. Defined once so the two cannot drift.
def _transaction_key() -> Key:
    return Key(("block_id_group",), ("tx_id",), (("tx_id", "ASC"),))


_TRANSACTION_COMMENT = (
    "Addressed by id, not by hash (D13). tx_id is (block_id << 32) + index in\n"
    "both families, so block_id_group is a shift and a division away and a\n"
    "lookup by id is ONE point read -- where v2 spent two per transaction, an\n"
    "id->hash mapping table then the transaction\n"
    "(`cassandra.py:5177-5203`), for every row of every page.\n"
    "\n"
    "Partitioned by a run of blocks rather than one block: TRON would otherwise\n"
    "have 85.8M partitions. tx_block_bucket_size is per network because a BTC\n"
    "block holds ~1 480 transactions and a ZEC block ~5."
)

#: Hash -> id, and the prefix-search index. Narrow on purpose: search reads whole
#: rows, and the transaction rows are wide (an ETH `input` runs to kilobytes).
_TRANSACTION_BY_TX_PREFIX = Table(
    "transaction_by_tx_prefix",
    (C("tx_prefix", "text"), C("tx_hash", "blob"), C("tx_id", "bigint")),
    Key(("tx_prefix",), ("tx_hash",)),
    {**STCS, "caching": "{'keys':'ALL','rows_per_partition':'NONE'}"},
    comment=(
        "The only route from a hash to a transaction, in both families. An exact\n"
        "hash is a point read giving tx_id; a prefix is a range slice over\n"
        "tx_hash within the one partition."
    ),
)


def raw_utxo() -> Schema:
    tables = (
        Table(
            "block",
            (
                C("block_id_group", "int"),
                C("block_id", "int"),
                C("block_hash", "blob"),
                C("timestamp", "bigint"),
                C("no_transactions", "int"),
            ),
            Key(("block_id_group",), ("block_id",), (("block_id", "DESC"),)),
            STCS,
        ),
        _BLOCK_BY_DATE,
        Table(
            "transaction",
            (
                C("block_id_group", "int", "block_id // tx_block_bucket_size"),
                C("block_id", "int"),
                C("tx_id", "bigint", "(block_id << 32) + transaction_index"),
                C("tx_hash", "blob"),
                C("timestamp", "bigint"),
                C("coinbase", "boolean"),
                C("coinjoin", "boolean"),
                C("total_input", "bigint"),
                C("total_output", "bigint"),
                C("no_inputs", "int", "graph/compare work gate"),
                C("no_outputs", "int"),
                C("version", "int"),
                C("lock_time", "bigint"),
            ),
            _transaction_key(),
            BULK,
            comment=(
                _TRANSACTION_COMMENT + "\n"
                "\n"
                "Header only. no_inputs/no_outputs live here so /graph/compare can\n"
                "apply its _MAX_TOTAL_IOS gate without fetching the IO lists."
            ),
        ),
        Table(
            "transaction_io",
            (
                C("block_id_group", "int"),
                C("tx_id", "bigint"),
                C("is_output", "boolean"),
                C("io_index", "int"),
                C("address", "frozen<list<blob>>", "was list<text>"),
                C("value", "bigint"),
                C("address_type", "smallint"),
                C("script_hex", "blob"),
                C("txinwitness", "frozen<list<blob>>"),
                C("sequence", "bigint"),
            ),
            Key(
                ("block_id_group",),
                ("tx_id", "is_output", "io_index"),
                (("tx_id", "ASC"), ("is_output", "ASC"), ("io_index", "ASC")),
            ),
            BULK,
            comment=(
                "Replaces transaction.inputs/outputs list<FROZEN<tx_input_output>>.\n"
                "Partitioned by block like `transaction`, so one transaction's IOs are\n"
                "a clustering slice and a whole block's are one partition -- but it\n"
                "pages, and there is no >16MB mutation cliff.\n"
                "An oversized mutation is REJECTED, not truncated, so under v2 a\n"
                "20k-input transaction is simply unwritable."
            ),
        ),
        Table(
            "transaction_spent_in",
            (
                C("spent_tx_prefix", "text"),
                C("spent_tx_hash", "blob"),
                C("spent_output_index", "int"),
                C("spending_tx_hash", "blob"),
                C("spending_input_index", "int"),
            ),
            Key(("spent_tx_prefix",), ("spent_tx_hash", "spent_output_index")),
            STCS,
        ),
        Table(
            "transaction_spending",
            (
                C("spending_tx_prefix", "text"),
                C("spending_tx_hash", "blob"),
                C("spending_input_index", "int"),
                C("spent_tx_hash", "blob"),
                C("spent_output_index", "int"),
            ),
            Key(("spending_tx_prefix",), ("spending_tx_hash", "spending_input_index")),
            STCS,
        ),
        _TRANSACTION_BY_TX_PREFIX,
        Table(
            "exchange_rates",
            (C("date", "text"), C("fiat_values", "frozen<map<text, double>>")),
            Key(("date",)),
            CACHED,
            comment="list<float> -> map: the positional list silently corrupts every "
            "historical row when a fiat currency is added or reordered.",
        ),
        *_housekeeping(Kind.RAW),
    )
    return Schema(Kind.RAW, Family.UTXO, (), tables)


TRC10_FROZEN_SUPPLY = UserType(
    "trc10_frozen_supply",
    (C("frozen_amount", "bigint"), C("frozen_days", "bigint")),
)

#: Columns only one chain has. The *shared* trace columns are shared by
#: construction -- which is the property v2 lacked, where three column types
#: drifted between raw_account_schema.sql and raw_account_trx_schema.sql without
#: anything noticing. Only genuinely chain-specific data varies here.
_TRACE_EXTRA: dict[str, tuple[C, ...]] = {
    "eth": (
        C("transaction_index", "int"),
        C("from_address", "blob"),
        C("to_address", "blob"),
        C("value", "varint"),
        C("input", "blob"),
        C("output", "blob"),
        C("trace_type", "text"),
        C("call_type", "text"),
        C("reward_type", "text"),
        C("gas", "bigint", "was int"),
        C("gas_used", "bigint"),
        C("subtraces", "int"),
        C("trace_address", "text"),
        C("error", "text"),
        C("status", "smallint"),
        C("trace_id", "text"),
    ),
    "trx": (
        C("internal_index", "smallint"),
        C("caller_address", "blob"),
        C("transferto_address", "blob"),
        C("call_info_index", "smallint"),
        C("call_token_id", "int"),
        C("call_value", "varint"),
        C("note", "text"),
        C("rejected", "boolean"),
    ),
}


def _trace_table(network: str) -> Table:
    """Traces: shared key and tx pointer, plus that chain's own columns.

    TRX has no ``transaction_index`` on a trace -- it is an EVM-trace column, so
    it lives in the eth block rather than in the shared set.
    """
    return Table(
        "trace",
        (
            C("block_id_group", "int"),
            C("block_id", "int"),
            C("trace_index", "int"),
            C("tx_hash", "blob"),
            *_TRACE_EXTRA[network],
        ),
        Key(
            ("block_id_group",),
            ("block_id", "trace_index"),
            (("block_id", "ASC"), ("trace_index", "ASC")),
        ),
        BULK,
        comment="Shared columns first; the rest are that chain's own trace model.",
    )


def _trx_tables() -> tuple[Table, ...]:
    """Tables only TRON has."""
    return (
        Table(
            "trc10",
            (
                C("id", "int"),
                C("owner_address", "blob"),
                C("name", "text"),
                C("abbr", "text"),
                C("total_supply", "varint"),
                C("trx_num", "varint"),
                C("num", "varint"),
                C("start_time", "varint", "last 3 digits dropped, as on eth"),
                C("end_time", "varint"),
                C("description", "text"),
                C("url", "text"),
                C("frozen_supply", "frozen<list<frozen<trc10_frozen_supply>>>"),
                C("public_latest_free_net_time", "varint"),
                C("vote_score", "smallint"),
                C("free_asset_net_limit", "bigint"),
                C("public_free_asset_net_limit", "bigint"),
                C("precision", "smallint"),
            ),
            Key(("id",)),
            CACHED,
            comment="Small and joined onto nearly every TRC10 transfer.",
        ),
        Table(
            "fee",
            (
                C("block_id_group", "int"),
                C("tx_id", "bigint"),
                C("tx_hash", "blob"),
                C("fee", "bigint"),
                C("energy_usage", "bigint"),
                C("energy_fee", "bigint"),
                C("origin_energy_usage", "bigint"),
                C("energy_usage_total", "bigint"),
                C("net_usage", "bigint"),
                C("net_fee", "bigint"),
                C("result", "int"),
                C("energy_penalty_total", "bigint"),
            ),
            _transaction_key(),
            BULK,
            comment=(
                "Keyed like `transaction` (D13), so the tx_id already in hand reads\n"
                "the fee directly. Addressed by hash it would have cost a third hop:\n"
                "id -> transaction -> hash -> prefix -> fee."
            ),
        ),
    )


def raw_account(network: str) -> Schema:
    """The account raw schema for one chain.

    One definition, rendered per chain: the shared columns cannot drift, and the
    chain-specific blocks are the only thing that varies.
    """
    if network not in _TRACE_EXTRA:
        raise KeyError(f"no account raw schema for network {network!r}")
    tables = (
        Table(
            "block",
            (
                C("block_id_group", "int"),
                C("block_id", "int"),
                C("block_hash", "blob"),
                C("parent_hash", "blob"),
                C("nonce", "blob"),
                C("sha3_uncles", "blob"),
                C("logs_bloom", "blob"),
                C("transactions_root", "blob"),
                C("state_root", "blob"),
                C("receipts_root", "blob"),
                C("miner", "blob"),
                C("difficulty", "varint"),
                C("total_difficulty", "varint"),
                C("size", "int"),
                C("extra_data", "blob"),
                C("gas_limit", "bigint", "was int (eth) / varint (trx)"),
                C("gas_used", "bigint", "was int (eth) / bigint (trx)"),
                C("base_fee_per_gas", "bigint"),
                C("timestamp", "bigint"),
                C("transaction_count", "int", "was smallint"),
            ),
            Key(("block_id_group",), ("block_id",), (("block_id", "DESC"),)),
            STCS,
            comment="Unifies raw_account and raw_account_trx, which had silently "
            "drifted in three column types.",
        ),
        _BLOCK_BY_DATE,
        _TRANSACTION_BY_TX_PREFIX,
        Table(
            "transaction",
            (
                C("block_id_group", "int", "block_id // tx_block_bucket_size"),
                C("tx_id", "bigint", "(block_id << 32) + transaction_index"),
                C("tx_hash", "blob"),
                C("nonce", "int"),
                C("block_hash", "blob"),
                C("block_id", "int"),
                C("transaction_index", "int"),
                C("from_address", "blob"),
                C("to_address", "blob"),
                C("value", "varint"),
                C("gas", "bigint"),
                C("gas_price", "varint"),
                C("input", "blob"),
                C("block_timestamp", "bigint"),
                C("max_fee_per_gas", "bigint"),
                C("max_priority_fee_per_gas", "bigint"),
                C("transaction_type", "tinyint", "was bigint"),
                C("receipt_cumulative_gas_used", "bigint", "was varint"),
                C("receipt_gas_used", "bigint", "was varint"),
                C("receipt_contract_address", "blob"),
                C("receipt_root", "blob"),
                C("receipt_status", "tinyint", "was bigint"),
                C("receipt_effective_gas_price", "bigint"),
                C("max_fee_per_blob_gas", "bigint"),
                C("blob_versioned_hashes", "frozen<list<blob>>"),
                C("v", "smallint"),
                C("r", "blob", "was varint; opaque 32 bytes"),
                C("s", "blob"),
                C("first_log_index", "int", "range pointer, see comment"),
                C("no_logs", "int"),
                C("first_trace_index", "int"),
                C("no_traces", "int"),
            ),
            _transaction_key(),
            BULK,
            comment=(
                _TRANSACTION_COMMENT + "\n"
                "\n"
                "The four range pointers replace per-transaction log/trace tables.\n"
                "A transaction's logs occupy a contiguous log_index range, because\n"
                "log_index is a block-scoped counter and transactions execute in order,\n"
                "so the pointers turn a whole-block partition scan into an exact\n"
                "clustering slice -- the same read shape at ~2% of the storage\n"
                "(~46 GB against ~2 TB on ETH). This is what /txs/{h}/flows and\n"
                "normalize_address_transactions need; the latter does up to `pagesize`\n"
                "such lookups per page.\n"
                "PRE-RUN CHECK: contiguity is certain for ETH logs, unverified for\n"
                "trace_index, and TRX uses a different trace model. If traces are not\n"
                "contiguous, fall back to a duplicated table for traces only."
            ),
        ),
        Table(
            "log",
            (
                C("block_id_group", "int"),
                C("block_id", "int"),
                C("log_index", "int"),
                C("block_hash", "blob"),
                C("address", "blob"),
                C("data", "blob"),
                C("topics", "frozen<list<blob>>"),
                C("topic0", "blob"),
                C("tx_hash", "blob"),
                C("transaction_index", "int"),
            ),
            Key(
                ("block_id_group",),
                ("block_id", "log_index"),
                (("block_id", "ASC"), ("log_index", "ASC")),
            ),
            BULK,
            comment=(
                "Re-keyed off topic0. As a clustering column it meant a block's logs\n"
                "could not be read in execution order, a specific log could not be\n"
                "fetched without knowing its topic, and a topicless log had no legal\n"
                "clustering value -- worked around by storing an empty blob."
            ),
        ),
        _trace_table(network),
        Table(
            "exchange_rates",
            (C("date", "text"), C("fiat_values", "frozen<map<text, double>>")),
            Key(("date",)),
            CACHED,
        ),
        Table(
            "token_exchange_rates",
            (
                C("asset", "text", "'token' is reserved"),
                C("date", "text"),
                C("fiat_values", "frozen<map<text, double>>"),
            ),
            Key(("asset",), ("date",)),
            CACHED,
        ),
        *(_trx_tables() if network == "trx" else ()),
        *_housekeeping(Kind.RAW),
    )
    types = (TRC10_FROZEN_SUPPLY,) if network == "trx" else ()
    return Schema(Kind.RAW, Family.ACCOUNT, types, tables)


# --------------------------------------------------------------------------- #
# transformed                                                                  #
# --------------------------------------------------------------------------- #


def _mirror(table: Table, name: str, renames: dict[str, str]) -> Table:
    """Derive a cluster-level table from its address-level twin.

    The six cluster tables that are genuine mirrors are generated rather than
    written out, so they cannot drift from the address-level definitions.
    """

    def r(column: str) -> str:
        return renames.get(column, column)

    return Table(
        name,
        tuple(C(r(c.name), c.type, c.comment) for c in table.columns),
        Key(
            tuple(r(c) for c in table.key.partition),
            tuple(r(c) for c in table.key.clustering),
            tuple((r(c), d) for c, d in table.key.order),
        ),
        dict(table.options),
        table.comment,
    )


def _stats_table(name: str, entity: str, family: Family, extra: tuple[C, ...]) -> Table:
    bucket = f"{entity}_bucket"
    tokens: tuple[C, ...] = (
        (
            C("total_tokens_received", "frozen<map<text, frozen<currency>>>"),
            C("total_tokens_spent", "frozen<map<text, frozen<currency>>>"),
        )
        if family is Family.ACCOUNT
        else ()
    )
    return Table(
        name,
        (
            C(bucket, "int", "murmur3(entity) % entity_buckets"),
            C(entity, "blob"),
            C("epoch", "int", "0 = compacted base; else block_id // epoch_size + 1"),
            # --- summable: every epoch row carries a partial value ---
            C("no_incoming_txs", "bigint"),
            C("no_outgoing_txs", "bigint"),
            C("no_incoming_txs_zero_value", "bigint"),
            C("no_outgoing_txs_zero_value", "bigint"),
            C("total_received", "frozen<currency>"),
            C("total_spent", "frozen<currency>"),
            *tokens,
            C("first_tx_id", "bigint", "min-merge"),
            C("last_tx_id", "bigint", "max-merge"),
            # --- epoch 0 only ---
            C("in_degree", "bigint", "epoch 0 only: not summable"),
            C("out_degree", "bigint"),
            C("in_degree_zero_value", "bigint"),
            C("out_degree_zero_value", "bigint"),
            C("in_tx_page_max", "int", "epoch 0 only: paging cursors"),
            C("out_tx_page_max", "int"),
            C("in_tx_ordinal_next", "bigint"),
            C("out_tx_ordinal_next", "bigint"),
            *extra,
        ),
        Key((bucket,), (entity, "epoch"), ((entity, "ASC"), ("epoch", "ASC"))),
        LCS,
        comment=(
            "Aggregates as SUMMABLE ROWS, replacing v2's client-side read-modify-write.\n"
            "\n"
            "A read is `WHERE bucket = ? AND entity = ?` and sum the slice. Base rows\n"
            "and delta rows have the same shape, so there is no watermark: compaction\n"
            "replaces N rows with their sum, which is idempotent and re-runnable, and a\n"
            "read is correct whether it ran or not. That is the whole point -- an\n"
            "append-only log with a `folded_through` watermark just relocates the\n"
            "correctness bug it was meant to remove.\n"
            "\n"
            "The ingest path therefore never reads: it blind-inserts one epoch row per\n"
            "touched entity. Deleted with the read-before-write: 4-7 point reads per\n"
            "touched address per batch, most of wal.py's reason to exist,\n"
            "collection-overwrite tombstones (v2 relations sit at ~41% droppable),\n"
            "db/parallel.py (which exists only for the ~1K reads/s per-process cap from\n"
            "UDT deserialisation on that path), and the LOGGED multi-table batch.\n"
            "\n"
            "Because the bucket is the whole partition key, every epoch row for an\n"
            "entity is in ONE partition -- so a compaction (write the sum to epoch 0,\n"
            "delete the absorbed epochs) is a single-partition batch, which Cassandra\n"
            "applies atomically. Compaction cannot half-happen.\n"
            "\n"
            "Degrees are distinct-counterparty counts and are NOT summable, so they live\n"
            "on epoch 0 and are maintained by compaction: stale between runs, exact\n"
            "after one. That is the accepted staleness tradeoff, spent where it costs\n"
            "least. Identity columns are blind-upserted to epoch 0 by the writer, which\n"
            "is a per-column upsert, not a read-modify-write."
        ),
    )


def _txs_table(name: str, entity: str, family: Family, *, recent: bool) -> Table:
    split = ("block_batch", "int") if recent else ("tx_page", "int")
    account_only: tuple[C, ...] = (
        (C("tx_reference", "frozen<tx_reference>"), C("currency", "text"))
        if family is Family.ACCOUNT
        else ()
    )
    clustering = ("tx_id", *(c.name for c in account_only))
    order = (("tx_id", "DESC"),) + tuple(
        (c.name, "DESC" if c.name == "tx_reference" else "ASC") for c in account_only
    )
    return Table(
        name,
        (
            C(entity, "blob"),
            C("is_outgoing", "boolean"),
            C(split[0], split[1]),
            C("tx_id", "bigint"),
            *account_only,
            C("value", "varint"),
        ),
        Key((entity, "is_outgoing", split[0]), clustering, order),
        CHURN if recent else STCS,
        comment=(
            (
                "Append-only tail. Ingest writes ONLY here, keyed by block_batch, which\n"
                "is derivable from the block and needs no read -- an ordinal would\n"
                "reintroduce the read-before-write that the stats model removes.\n"
                "Compaction drains a batch into the paged table, assigning ordinals in\n"
                "tx_id order. A read merges tail-then-pages, both tx_id DESC. Since\n"
                "newest-first is the API default, the common request is served from the\n"
                "tail alone and never touches a page."
            )
            if recent
            else (
                "Paged by the entity's own transaction ordinal, so every partition holds\n"
                "exactly tx_page_size rows: immune to burst and to dormancy, no empty\n"
                "buckets, no 455M-row cell. tx_page_max lives on the stats row, which is\n"
                "already read on nearly every path, so there is no side table.\n"
                "\n"
                "is_outgoing is in the PARTITION key because Cassandra requires\n"
                "clustering restrictions to form a prefix -- below tx_id it could not be\n"
                "pushed down at all, and direction=out on an address with 10M incoming\n"
                "and 100 outgoing would scan the lot. currency stays below tx_id: in the\n"
                "partition key it would make the UNFILTERED query fan out over every\n"
                "asset held, which is exactly the v2 pathology."
            )
        ),
    )


def _relations_table(name: str, near: str, far: str, family: Family) -> Table:
    tokens: tuple[C, ...] = (
        (C("token_values", "frozen<map<text, frozen<currency>>>"),)
        if family is Family.ACCOUNT
        else ()
    )
    return Table(
        name,
        (
            C(near, "blob"),
            C("rel_bucket", "int", "murmur3(far side) % relation_buckets"),
            C(far, "blob"),
            C("epoch", "int", "as address_stats: summable"),
            C("no_transactions", "bigint", "was int"),
            C("value", "frozen<currency>"),
            *tokens,
            C("link_page_max", "int", "epoch 0 only"),
            C("link_ordinal_next", "bigint"),
        ),
        Key((near, "rel_bucket"), (far, "epoch"), ((far, "ASC"), ("epoch", "ASC"))),
        LCS,
        comment=(
            "One entity's relations, not 25 000 entities'. relation_buckets is a config\n"
            "constant so a read scatters over 0..N-1 unconditionally and stops on\n"
            "in_degree/out_degree -- the four *_secondary_ids watermark tables are gone.\n"
            "only_ids stays a point read: the bucket is computed from the counterparty.\n"
            "v2 uses 100 buckets; 16 is enough and cuts the fan-out four-fold.\n"
            "\n"
            "The edge carries its OWN page cursor rather than deriving one from\n"
            "no_transactions. The two are not interchangeable while the UTXO transform\n"
            "nets flows per (tx, entity) -- and that is precisely the chain where /links\n"
            "already cannot trust the netted edge."
        ),
    )


def _link_txs_table(name: str, src: str, dst: str, family: Family) -> Table:
    """The /links fix. Layout differs by family; see the comment."""
    comment = (
        "The single most expensive table in v3 -- roughly three times the cost of\n"
        "byte-keying everything else -- so the layout is chosen per family.\n"
        "\n"
        "A partition costs its key plus ~40 bytes of overhead; a row costs its\n"
        "clustering columns. The two layouts trade the counterparty between them.\n"
        "UTXO has 1.5e9 BTC addresses averaging 1.2 transactions per edge, so\n"
        "per-partition overhead dominates and partition-per-source wins by ~42%\n"
        "(935 vs 1621 logical GiB). Account has an order of magnitude fewer\n"
        "addresses with more transactions per edge (TRX 2.27, ETH 1.92), so the\n"
        "repeated destination costs more than the partitions it saves.\n"
        "\n"
        "Both serve /links as a point-slice; the UTXO form restricts dst as a\n"
        "clustering PREFIX, so it is pushed down in full. Both writers already\n"
        "materialise these tuples when computing no_transactions -- today they\n"
        "aggregate them away, which is why /links has to rescan raw io membership."
    )
    if family is Family.UTXO:
        return Table(
            name,
            (
                C(src, "blob"),
                C("dst_bucket", "int", "murmur3(dst) % relation_buckets"),
                C(dst, "blob"),
                C("tx_id", "bigint"),
                C("value", "bigint"),
            ),
            Key((src, "dst_bucket"), (dst, "tx_id"), ((dst, "ASC"), ("tx_id", "DESC"))),
            STCS,
            comment=comment,
        )
    return Table(
        name,
        (
            C(src, "blob"),
            C(dst, "blob"),
            C("tx_page", "int"),
            C("tx_id", "bigint"),
            C("tx_reference", "frozen<tx_reference>"),
            C("currency", "text"),
            C("value", "varint"),
        ),
        Key(
            (src, dst, "tx_page"),
            ("tx_id", "tx_reference", "currency"),
            (("tx_id", "DESC"), ("tx_reference", "DESC"), ("currency", "ASC")),
        ),
        STCS,
        comment=comment,
    )


def transformed(family: Family) -> Schema:
    is_utxo = family is Family.UTXO

    addr_stats = _stats_table(
        "address_stats",
        "address",
        family,
        (C("cluster_address", "blob", "UTXO: the cluster's root member"),)
        if is_utxo
        else (C("is_contract", "boolean"),),
    )
    addr_txs = _txs_table("address_transactions", "address", family, recent=False)
    addr_txs_recent = _txs_table(
        "address_transactions_recent", "address", family, recent=True
    )
    addr_pages = Table(
        "address_tx_pages",
        (
            C("address", "blob"),
            C("is_outgoing", "boolean"),
            C("first_tx_id", "bigint"),
            C("tx_page", "int"),
        ),
        Key(("address", "is_outgoing"), ("first_tx_id",), (("first_tx_id", "DESC"),)),
        STCS,
        comment=(
            "Ordinal pages are not tx_id-aligned -- the one thing block bucketing did\n"
            "better -- so a height or date filter cannot compute which pages hold its\n"
            "range. The entry page is a point-slice (first_tx_id <= :hi LIMIT 1) and the\n"
            "walk proceeds downward. One row for a typical address, ~37 000 for TRON\n"
            "USDT, all in one partition of about a megabyte. Read only when a range\n"
            "filter is present."
        ),
    )
    incoming = _relations_table(
        "address_incoming_relations", "dst_address", "src_address", family
    )
    outgoing = _relations_table(
        "address_outgoing_relations", "src_address", "dst_address", family
    )
    links = _link_txs_table(
        "address_link_transactions", "src_address", "dst_address", family
    )

    balance = Table(
        "balance",
        (
            C("address_bucket", "int"),
            C("address", "blob"),
            C("currency", "text"),
            C("epoch", "int"),
            C("balance", "varint", "signed delta; summable like address_stats"),
        ),
        Key(("address_bucket",), ("address", "currency", "epoch")),
        LCS,
    )

    by_prefix = Table(
        "address_by_prefix",
        (C("address_prefix", "text"), C("address", "blob")),
        Key(("address_prefix",), ("address",)),
        STCS,
        comment=(
            "Search only -- exact address -> entity is now direct, so this is no longer\n"
            "a lookup table and carries no id.\n"
            "\n"
            "Packed bytes are not order-preserving ACROSS address types: get_codec\n"
            "dispatches on alphabet (bech32 5 bits/char, base58 ~5.86, base62 ~5.95), so\n"
            "a prefix range slice on the packed form returns wrong results. Prefix search\n"
            "therefore reads the whole prefix partition and filters client-side, which is\n"
            "only viable if partitions are small -- making the bech32 prefix fix a\n"
            "PREREQUISITE, not an improvement. v2 strips only 'bc' from BTC, leaving\n"
            "1q.../1p... so two of four prefix chars are constants and the entire segwit\n"
            "space lands in 32^2 = 1024 partitions of ~390k rows, where LTC gets ~1M\n"
            "partitions of ~380."
        ),
    )

    common: list[Table] = [
        addr_stats,
        addr_txs,
        addr_txs_recent,
        addr_pages,
        incoming,
        outgoing,
        links,
        balance,
        by_prefix,
        Table(
            "exchange_rates",
            (
                C("block_id_group", "int"),
                C("block_id", "int"),
                C("fiat_values", "frozen<map<text, double>>"),
            ),
            Key(("block_id_group",), ("block_id",), (("block_id", "DESC"),)),
            CACHED,
            comment=(
                "Bucketed. v2 keys this by block_id alone: 20.25M single-row partitions\n"
                "of 86 bytes on TRX, carrying a 24 MB bloom filter and 2.5 MB index\n"
                "summary over 778 MB of data -- the partition index exceeds what it\n"
                "indexes. /rates/{height} stays a point read; the group is computed\n"
                "client-side."
            ),
        ),
        Table(
            "delta_updater_history",
            (
                C("last_synced_block", "bigint"),
                C("last_synced_block_timestamp", "bigint"),
                C("timestamp", "bigint"),
                C("runtime_seconds", "int"),
            ),
            Key(("last_synced_block",)),
            {
                "default_time_to_live": "7776000",
                "compaction": (
                    "{'class':'TimeWindowCompactionStrategy',"
                    "'compaction_window_unit':'DAYS','compaction_window_size':'7'}"
                ),
            },
            comment="v2 grows one partition per run forever and reads it by unbounded "
            "scan. highest_address_id is gone: there is no allocator.",
        ),
        *_housekeeping(Kind.TRANSFORMED),
    ]

    if family is Family.ACCOUNT:
        common.extend(
            [
                Table(
                    "token_exchange_rates",
                    (
                        C("asset", "text"),
                        C("block_id", "int"),
                        C("fiat_values", "frozen<map<text, double>>"),
                    ),
                    Key(("asset",), ("block_id",), (("block_id", "DESC"),)),
                    CACHED,
                    comment="Order A5 (rate at or before a height) preserved.",
                ),
                Table(
                    "token_configuration",
                    (
                        C("currency_ticker", "text"),
                        C("token_address", "blob"),
                        C("standard", "text"),
                        C("decimals", "int"),
                        C("decimal_divisor", "bigint"),
                        C("peg_currency", "text"),
                    ),
                    Key(("currency_ticker",)),
                    CACHED,
                ),
            ]
        )

    if is_utxo:
        renames = {
            "address": "cluster_address",
            "address_bucket": "cluster_bucket",
            "src_address": "src_cluster_address",
            "dst_address": "dst_cluster_address",
        }
        common.extend(
            [
                _stats_table(
                    "cluster_stats",
                    "cluster_address",
                    family,
                    (
                        C("no_addresses", "bigint"),
                        C("total_received_adj", "frozen<currency>"),
                        C("total_spent_adj", "frozen<currency>"),
                    ),
                ),
                _mirror(addr_txs, "cluster_transactions", renames),
                _mirror(addr_txs_recent, "cluster_transactions_recent", renames),
                _mirror(addr_pages, "cluster_tx_pages", renames),
                _mirror(incoming, "cluster_incoming_relations", renames),
                _mirror(outgoing, "cluster_outgoing_relations", renames),
                _mirror(links, "cluster_link_transactions", renames),
                Table(
                    "cluster_addresses",
                    (
                        C("cluster_bucket", "int"),
                        C("cluster_address", "blob"),
                        C("address", "blob"),
                    ),
                    Key(("cluster_bucket", "cluster_address"), ("address",)),
                    STCS,
                    comment=(
                        "Not a mirror, so written out. One cluster's members are one\n"
                        "partition rather than a slice inside a 25 000-cluster partition.\n"
                        "Address -> cluster needs no table: it is the cluster_address\n"
                        "column on the address_stats epoch-0 row."
                    ),
                ),
            ]
        )

    types = (CURRENCY,) if is_utxo else (CURRENCY, TX_REFERENCE)
    return Schema(Kind.TRANSFORMED, family, types, tuple(common))


#: Every network, and which family it renders as.
NETWORKS: dict[str, Family] = {
    "btc": Family.UTXO,
    "bch": Family.UTXO,
    "ltc": Family.UTXO,
    "zec": Family.UTXO,
    "eth": Family.ACCOUNT,
    "trx": Family.ACCOUNT,
}


def schema_for(network: str, kind: Kind) -> Schema:
    family = NETWORKS[network]
    if kind is Kind.TRANSFORMED:
        return transformed(family)
    return raw_utxo() if family is Family.UTXO else raw_account(network)
