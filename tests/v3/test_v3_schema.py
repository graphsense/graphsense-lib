"""The v3 schema is generated, so these guard the generator, not the CQL text."""

import pytest

from graphsense_v3.schema import NETWORKS, Family, Kind, schema_for, violations
from graphsense_v3.schema.definitions import transformed
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
    types had already drifted between the two raw account schemas."""
    for kind in Kind:
        for family in Family:
            nets = [n for n, f in NETWORKS.items() if f is family]
            rendered = {
                render_schema(schema_for(n, kind), "ks", "{'class':'Simple'}")
                for n in nets
            }
            assert len(rendered) == 1, f"{family.value}/{kind.value} drifted"


def test_no_secondary_ids_tables() -> None:
    for network, kind in ALL:
        names = {t.name for t in schema_for(network, kind).tables}
        assert not {n for n in names if n.endswith("_secondary_ids")}


def test_utxo_link_table_partitions_by_source() -> None:
    """Layout is per family: partition-per-source on UTXO (many low-degree
    addresses, so partition overhead dominates), partition-per-edge on account."""
    utxo = transformed(Family.UTXO).table("address_link_transactions")
    account = transformed(Family.ACCOUNT).table("address_link_transactions")
    assert utxo.key.partition == ("src_address", "dst_bucket")
    assert "dst_address" in utxo.key.clustering
    assert account.key.partition == ("src_address", "dst_address", "tx_page")


def test_direction_is_pushed_down() -> None:
    """is_outgoing must be in the partition key: Cassandra requires clustering
    restrictions to form a prefix, so below tx_id it cannot be pushed down."""
    for family in Family:
        table = transformed(family).table("address_transactions")
        assert "is_outgoing" in table.key.partition
        assert table.key.clustering[0] == "tx_id"


def test_stats_tables_are_summable() -> None:
    """Epoch is the last clustering column, so one entity's rows are a slice a
    read can sum, and compaction of one entity is a single-partition batch."""
    for family in Family:
        table = transformed(family).table("address_stats")
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
