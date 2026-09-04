"""The v3 schema is generated, so these guard the generator, not the CQL text."""

import pytest

from graphsense_v3.schema import NETWORKS, Family, Kind, schema_for, violations
from graphsense_v3.schema.model import Key
from graphsense_v3.schema.definitions import raw_account, derived
from graphsense_v3.schema.render import render_schema

ALL = [(n, k) for n in NETWORKS for k in Kind]


@pytest.mark.parametrize(("network", "kind"), ALL)
def test_schema_satisfies_design_rules(network: str, kind: Kind) -> None:
    assert violations(schema_for(network, kind)) == []


@pytest.mark.parametrize(("network", "kind"), ALL)
def test_renders_to_cql(network: str, kind: Kind) -> None:
    cql = render_schema(
        schema_for(network, kind),
        f"{network}_{kind.value}_v3",
        "{'class':'NetworkTopologyStrategy','DC1':'2'}",
    )
    assert cql.startswith("-- generated:")
    assert cql.count("CREATE TABLE") == len(schema_for(network, kind).tables)
    assert ";" in cql and cql.endswith("\n")


def test_networks_of_a_family_render_identically() -> None:
    """The anti-drift property: two networks of one family cannot diverge,
    because there is one definition. v2 kept parallel .sql files and three column
    types had already drifted between the two raw account schemas.

    Raw account is the one exception -- eth and trx have genuinely different
    trace models and trx has two tables eth does not -- and the property that
    replaces it there is asserted by the next test.
    """
    for kind in Kind:
        for family in Family:
            if kind is Kind.RAW and family is Family.ACCOUNT:
                continue
            nets = [n for n, f in NETWORKS.items() if f is family]
            rendered = {
                render_schema(schema_for(n, kind), "ks", "{'class':'Simple'}")
                for n in nets
            }
            assert len(rendered) == 1, f"{family.value}/{kind.value} drifted"


def test_account_raw_chains_agree_on_every_shared_column() -> None:
    """Exactly the v2 defect: `gas_limit` was int on eth and varint on trx,
    `gas_used` int vs bigint, `transaction_count` smallint vs int -- three types
    silently drifted between two hand-maintained files. Here the chains may add
    columns, but a column they share must have one type."""
    eth = {t.name: t for t in raw_account("eth").tables}
    trx = {t.name: t for t in raw_account("trx").tables}
    for name in eth.keys() & trx.keys():
        eth_cols = {c.name: c.type for c in eth[name].columns}
        trx_cols = {c.name: c.type for c in trx[name].columns}
        for column in eth_cols.keys() & trx_cols.keys():
            assert eth_cols[column] == trx_cols[column], (
                f"{name}.{column} drifted: "
                f"eth {eth_cols[column]} vs trx {trx_cols[column]}"
            )
        assert eth[name].key == trx[name].key, f"{name} key drifted"


def test_account_raw_carries_the_chain_specific_tables() -> None:
    """The v2 schemas' chain-specific tables, which the first v3 draft dropped to
    a comment -- the Spark writer rejects a column the model does not declare, so
    the loader could not have written a trace at all."""
    eth = {t.name for t in raw_account("eth").tables}
    trx = {t.name for t in raw_account("trx").tables}
    assert {"trc10", "fee"} <= trx
    assert not {"trc10", "fee"} & eth

    trx_trace = {c.name for c in raw_account("trx").table("trace").columns}
    eth_trace = {c.name for c in raw_account("eth").table("trace").columns}
    assert "call_token_id" in trx_trace and "call_token_id" not in eth_trace
    assert "trace_type" in eth_trace and "trace_type" not in trx_trace


def test_traces_and_logs_link_back_by_id_on_both_chains() -> None:
    """Was the last asymmetry: an eth trace carried `transaction_index` and a
    TRON trace carried nothing, so a trace linked back to its transaction on one
    chain and not the other. `tx_id` addresses the `transaction` row directly
    (D13) and the index is its low 32 bits, so nothing is lost."""
    for chain in ("eth", "trx"):
        raw = raw_account(chain)
        for table in ("log", "trace"):
            columns = {c.name: c.type for c in raw.table(table).columns}
            assert columns.get("tx_id") == "bigint"
            assert "transaction_index" not in columns


def test_trace_success_is_one_column_on_both_chains() -> None:
    """TRON reports a `rejected` boolean and has no status code; it is mapped
    onto eth's convention in the loader. `error` stays eth-only -- TRON has no
    per-trace revert reason to put in it."""
    for chain in ("eth", "trx"):
        columns = {c.name: c.type for c in raw_account(chain).table("trace").columns}
        assert columns["status"] == "smallint"
        assert "rejected" not in columns
    assert "error" in {c.name for c in raw_account("eth").table("trace").columns}
    assert "error" not in {c.name for c in raw_account("trx").table("trace").columns}


def test_traces_share_who_sent_what_to_whom() -> None:
    """TRON's source calls these caller_address, transferto_address and
    call_value; they are the same three things, and naming them apart made every
    reader of traces branch on the chain."""
    for chain in ("eth", "trx"):
        columns = {c.name for c in raw_account(chain).table("trace").columns}
        assert {"from_address", "to_address", "value"} <= columns
        assert not {"caller_address", "transferto_address", "call_value"} & columns


def test_one_name_and_one_type_for_each_shared_concept() -> None:
    """The harmonisation pass: same thing, same name, same type, everywhere."""
    for network in NETWORKS:
        raw = schema_for(network, Kind.RAW)
        block = {c.name: c.type for c in raw.table("block").columns}
        # transactions in a block: was `no_transactions` on utxo and
        # `transaction_count` on account.
        assert "no_transactions" in block and "transaction_count" not in block
        tx = {c.name: c.type for c in raw.table("transaction").columns}
        # the block's timestamp copied onto the transaction: was `timestamp` on
        # utxo, `block_timestamp` on account.
        assert "block_timestamp" in tx and "timestamp" not in tx

    # A transferred amount is varint everywhere, including UTXO, where the link
    # table alone had it as bigint while address_transactions used varint.
    # The link table names its amounts input_value/output_value -- what each
    # side really moved -- rather than `value`, which is the APPORTIONED edge
    # weight and lives on the relations row.
    # The link table's amount columns differ by FAMILY, and deliberately:
    # `links_response` reports a UTXO link as the two real amounts each side
    # moved, while an eth-like link goes through `txs_from_rows` and is a
    # transaction, so one `value` is the whole story there.
    link_amounts = {
        Family.UTXO: ("input_value", "output_value"),
        Family.ACCOUNT: ("value",),
    }
    for family in Family:
        tf = derived(family)
        checks = {
            "address_transactions": ("value",),
            "address_link_transactions": link_amounts[family],
        }
        for table, columns in checks.items():
            types = {c.name: c.type for c in tf.table(table).columns}
            assert {types[name] for name in columns} == {"varint"}, (
                f"{family.value}.{table} stores an amount as something other "
                "than varint"
            )


def test_transactions_are_addressed_identically_in_both_families() -> None:
    """D13. tx_id is (block_id << 32) + index in both families now, so both
    address a transaction by id over a run of blocks -- one point read, where the
    account path spent two (an id->hash table, then the transaction) for every
    row of every page."""
    keys = set()
    for network in NETWORKS:
        raw = schema_for(network, Kind.RAW)
        table = raw.table("transaction")
        assert table.key.partition == ("block_id_group",)
        assert table.key.clustering == ("tx_id",)
        keys.add(table.key)
        # The one route from a hash, in both families.
        assert raw.table("transaction_by_tx_prefix").key == Key(
            ("tx_prefix",), ("tx_hash",)
        )
        # Subsumed: the id addresses the transaction directly.
        assert "block_transactions" not in raw.table_names()
    assert len(keys) == 1, "the two families' transaction keys drifted"

    # tx_id is sparse, so the constant that bucketed a dense id is gone.
    config = schema_for("btc", Kind.RAW).table("configuration").column_names()
    assert "tx_bucket_size" not in config
    assert "tx_block_bucket_size" in config

    io = schema_for("btc", Kind.RAW).table("transaction_io")
    assert io.key.partition == ("block_id_group",)
    assert io.key.clustering[0] == "tx_id"


def test_no_secondary_ids_tables() -> None:
    for network, kind in ALL:
        names = {t.name for t in schema_for(network, kind).tables}
        assert not {n for n in names if n.endswith("_secondary_ids")}


def test_utxo_link_table_partitions_by_source() -> None:
    """Layout is per family: partition-per-source on UTXO (many low-degree
    addresses, so partition overhead dominates), partition-per-edge on account."""
    utxo = derived(Family.UTXO).table("address_link_transactions")
    account = derived(Family.ACCOUNT).table("address_link_transactions")
    assert utxo.key.partition == ("src_address", "dst_bucket")
    assert "dst_address" in utxo.key.clustering
    assert account.key.partition == ("src_address", "dst_address", "tx_page")


def test_direction_is_pushed_down() -> None:
    """is_outgoing must be in the partition key: Cassandra requires clustering
    restrictions to form a prefix, so below tx_id it cannot be pushed down."""
    for family in Family:
        table = derived(family).table("address_transactions")
        assert "is_outgoing" in table.key.partition
        assert table.key.clustering[0] == "tx_id"


def test_stats_tables_are_summable() -> None:
    """Epoch is the last clustering column, so one entity's rows are a slice a
    read can sum, and compaction of one entity is a single-partition batch."""
    for family in Family:
        table = derived(family).table("address_stats")
        assert table.key.clustering[-1] == "epoch"
        assert table.key.partition == ("address_bucket",)
        assert "address" in table.key.clustering


def test_generated_sql_is_up_to_date() -> None:
    """The .sql files are artifacts of the model. If this fails, run
    `uv run python -m graphsense_v3.schema.emit`."""
    from graphsense_v3.schema.emit import GENERATED_DIR, render_all

    for filename, expected in render_all().items():
        path = GENERATED_DIR / filename
        assert path.exists(), f"{filename} has not been generated"
        assert path.read_text(encoding="utf-8") == expected, (
            f"{filename} is stale; regenerate with "
            "`uv run python -m graphsense_v3.schema.emit`"
        )


def test_replication_refuses_a_single_replica() -> None:
    """A production keyspace really did run at DC1: 1, and nothing caught it."""
    import pytest

    from graphsense_v3.schema.render import replication

    assert replication({"DC1": 2}) == "{'class':'NetworkTopologyStrategy','DC1':'2'}"
    with pytest.raises(ValueError, match="single node loss"):
        replication({"DC1": 1})
    # A local or test keyspace has one node, so it has to be sayable -- in code.
    assert "'DC1':'1'" in replication({"DC1": 1}, allow_single_replica=True)


def test_replication_rejects_nonsense() -> None:
    import pytest

    from graphsense_v3.schema.render import replication

    with pytest.raises(ValueError, match="at least one datacenter"):
        replication({})
    with pytest.raises(ValueError, match=">= 1"):
        replication({"DC1": 0}, allow_single_replica=True)


def test_configuration_row_matches_the_table_and_its_spark_schema() -> None:
    """`as_row` is a positional tuple, so its order and the DDL's must agree --
    and both must name exactly the columns the table declares."""
    from graphsense_v3.config import CONFIGURATION_SCHEMA, config_for

    names = [field.split()[0] for field in CONFIGURATION_SCHEMA.split(", ")]
    for network in NETWORKS:
        table = schema_for(network, Kind.RAW).table("configuration")
        assert set(names) == set(table.column_names())
        assert len(config_for(network).as_row("ks")) == len(names)
    assert names[0] == "keyspace_name"


def test_no_unexplained_type_drift_between_any_two_schemas() -> None:
    """The harmonisation pass, kept honest: a column name that appears in two
    schemas must mean the same type in both. This is the check that found
    `address_link_transactions.value` as bigint on UTXO and varint on account."""
    from itertools import combinations

    from graphsense_v3.schema.definitions import raw_account, raw_utxo

    schemas = {
        "raw/utxo": raw_utxo(),
        "raw/eth": raw_account("eth"),
        "raw/trx": raw_account("trx"),
        "tf/utxo": derived(Family.UTXO),
        "tf/account": derived(Family.ACCOUNT),
    }
    for (a_name, a), (b_name, b) in combinations(schemas.items(), 2):
        a_tables = {t.name: t for t in a.tables}
        b_tables = {t.name: t for t in b.tables}
        for name in a_tables.keys() & b_tables.keys():
            a_cols = {c.name: c.type for c in a_tables[name].columns}
            b_cols = {c.name: c.type for c in b_tables[name].columns}
            for column in a_cols.keys() & b_cols.keys():
                assert a_cols[column] == b_cols[column], (
                    f"{name}.{column}: {a_name} has {a_cols[column]}, "
                    f"{b_name} has {b_cols[column]}"
                )


def test_no_temporal_column_types_anywhere() -> None:
    """Design rule 5. `timestamp` is milliseconds where everything upstream and
    downstream speaks seconds, and a `date` partition key buys nothing over an
    integer when the only access pattern is equality on the day."""
    for network in NETWORKS:
        for kind in Kind:
            for table in schema_for(network, kind).tables:
                for column in table.columns:
                    assert column.type not in ("timestamp", "date"), (
                        f"{table.name}.{column.name} is {column.type}"
                    )


def test_the_delta_updater_history_table_is_gone() -> None:
    """Its job in v2 was detecting a torn bookkeeping write, which matters only
    because applying a batch twice is unsafe there. A v3 epoch row is keyed
    (bucket, entity, epoch) and carries that epoch's sum, so rewriting it is an
    identical upsert -- there is nothing left for it to guard."""
    for network in NETWORKS:
        assert (
            "delta_updater_history"
            not in schema_for(network, Kind.DERIVED).table_names()
        )


def test_the_markers_table_replaces_state() -> None:
    """v2 called this `state`, which reads as an invitation to keep cursors in
    it -- and a cursor is exactly what must not live there, since these rows are
    set once and never advanced. I misread it that way myself before renaming."""
    from graphsense_v3.schema.definitions import MARKERS

    for network in NETWORKS:
        for kind in Kind:
            names = schema_for(network, kind).table_names()
            assert MARKERS in names
            assert "state" not in names


def test_the_completion_marker_is_documented_in_the_schema() -> None:
    """A key/value table earns its shape only if the key set is written down."""
    from graphsense_v3.schema.definitions import MARKER_COMPLETE

    comment = schema_for("btc", Kind.RAW).table("markers").comment or ""
    assert MARKER_COMPLETE in comment
    assert "Written LAST" in comment


def test_one_rate_table_covers_every_asset() -> None:
    """Split in two, the token half inherited no bucketing: v2 keys it
    (asset, block_id), so one asset's partition grows with the chain -- 85.8M
    rows for a TRON stablecoin. Merged, both get the same key."""
    for network in NETWORKS:
        for kind in Kind:
            schema = schema_for(network, kind)
            assert "token_exchange_rates" not in schema.table_names()
            rates = schema.table("exchange_rates")
            assert "asset" in rates.column_names()
            assert rates.key.partition[0] == "asset"
    # and the derived one is bucketed on the block, in both families
    for network in ("btc", "eth"):
        rates = schema_for(network, Kind.DERIVED).table("exchange_rates")
        assert rates.key.partition == ("asset", "block_id_group")
        assert rates.key.clustering == ("block_id",)


def test_no_cluster_tables_while_clustering_is_unbuilt() -> None:
    """Empty is worse than absent: a reader hitting an empty cluster table gets
    "no data", which in a comparison reads as a difference in the DATA rather
    than a feature that is not built. D9 stages clustering to run 2."""
    for network in NETWORKS:
        names = schema_for(network, Kind.DERIVED).table_names()
        assert not [n for n in names if n.startswith("cluster")]
    # the pointer goes with them: a NULL cluster_address is indistinguishable
    # from "this address is its own cluster"
    stats = schema_for("btc", Kind.DERIVED).table("address_stats")
    assert "cluster_address" not in stats.column_names()
