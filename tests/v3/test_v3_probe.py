"""The DAL access-pattern probe.

The probe itself needs a cluster, so what is testable here is the part that
would rot silently: whether the catalogue still covers the schema. A table
added without a probe is an access pattern nobody checked.
"""

import pytest

from graphsense_v3 import probe as prober
from graphsense_v3.schema import Kind, schema_for

RAW = "ltc_raw_v3_test"
DERIVED = "ltc_derived_v3_test"

CONFIG = {
    "entity_buckets": 128,
    "tx_page_size": 100_000,
    "relation_buckets": 16,
    "block_bucket_size": 100_000,
    "tx_block_bucket_size": 25_000,
    "address_prefix_length": 5,
    "tx_prefix_length": 5,
}

#: Tables the probe deliberately does not exercise, and why. An entry here is a
#: decision; a table missing from BOTH this and the catalogue is an oversight.
NOT_PROBED = {
    "address_transactions_recent": (
        "the ingest tail. A backfill never writes it, so probing a backfilled "
        "keyspace would only ever report an empty table."
    ),
    "exchange_rates": (
        "read from the production rates keyspace, not the v3 raw one -- the "
        "derived copy is probed instead."
    ),
}


class FakeSession:
    """Records CQL and returns nothing, so the catalogue can run dry."""

    def __init__(self):
        self.seen = []

    def execute(self, cql, params=()):
        self.seen.append(cql)
        return []


def _full_fixtures() -> prober.Fixtures:
    """Every fixture present, so no probe is skipped for want of a key."""
    return prober.Fixtures(
        address=b"\xa1" * 21,
        is_outgoing=True,
        is_zero_value=False,
        tx_page=0,
        busiest_address=b"\xb0" * 21,
        busiest_pages=3,
        tx_hash=b"\xcc" * 32,
        tx_prefix="abcde",
        tx_id=(100 << 32) + 1,
        block_id=100,
        day=20110101,
        link_src=b"\xa1" * 21,
        link_dst=b"\xb0" * 21,
        address_prefix="1A1zP",
        currency="LTC",
    )


def _run_dry() -> tuple[list, FakeSession]:
    session = FakeSession()
    runner = prober.Prober(session, RAW, DERIVED, CONFIG)
    return runner.run(_full_fixtures()), session


def _tables_touched(session: FakeSession) -> set:
    tables = set()
    for cql in session.seen:
        for part in cql.split():
            for keyspace in (RAW, DERIVED):
                if part.startswith(f"{keyspace}."):
                    tables.add(part.split(".", 1)[1])
    return tables


def test_every_derived_table_has_an_access_pattern() -> None:
    """A derived table exists to serve a read. If none of them is probed, the
    table is either unnecessary or untested -- both worth knowing."""
    _, session = _run_dry()
    touched = _tables_touched(session)
    declared = set(schema_for("btc", Kind.DERIVED).table_names())
    missing = declared - touched - set(NOT_PROBED)
    assert missing == set(), f"derived tables with no probe: {sorted(missing)}"


def test_every_raw_table_has_an_access_pattern() -> None:
    _, session = _run_dry()
    touched = _tables_touched(session)
    declared = set(schema_for("btc", Kind.RAW).table_names())
    missing = declared - touched - set(NOT_PROBED)
    assert missing == set(), f"raw tables with no probe: {sorted(missing)}"


def test_exemptions_name_real_tables() -> None:
    """An exemption for a table that no longer exists hides a real gap."""
    declared = set(schema_for("btc", Kind.RAW).table_names()) | set(
        schema_for("btc", Kind.DERIVED).table_names()
    )
    assert set(NOT_PROBED) <= declared


def test_the_report_names_the_two_keyspaces_apart() -> None:
    """Both readiness probes were labelled "v3": the keyspace name splits to
    <network>_<kind>_v3_<label>, so [-2] is the version, not the kind."""
    results, _ = _run_dry()
    names = [r.name for r in results if "complete marker" in r.name]
    assert names == ["raw complete marker", "derived complete marker"]


def test_address_format_check_is_reported_not_asserted() -> None:
    """A lake written before the network-aware P2PK fix carries BTC-version
    addresses in an LTC keyspace, and v3 re-encodes them faithfully. That is a
    finding about the DATA, so it is surfaced as a histogram rather than a
    verdict -- judging it needs someone who knows the chain's prefixes."""
    session = FakeSession()
    runner = prober.Prober(session, RAW, DERIVED, CONFIG)
    results = runner.run(_full_fixtures(), network="ltc")
    check = next(r for r in results if r.name == "address first characters")
    assert check.kind is prober.OPTIONAL
    assert check.ok, "an empty sample must not read as a failure"


def test_relation_probes_scatter_over_every_bucket() -> None:
    """Listing neighbours has no watermark table to stop it early: it reads
    relation_buckets partitions unconditionally, and the report has to say so
    or the cost is invisible."""
    results, _ = _run_dry()
    scatter = [r for r in results if "neighbors (all buckets)" in r.name]
    assert len(scatter) == 2
    for result in scatter:
        assert result.reads == CONFIG["relation_buckets"]


def test_a_failing_probe_reports_paste_able_cql() -> None:
    """The whole point of recording the CQL: a failure has to be runnable in
    cqlsh without reconstructing the parameters."""
    results, _ = _run_dry()
    stats = next(r for r in results if r.name == "address stats (epoch slice)")
    assert "0x" + "a1" * 21 in stats.cql
    assert "%s" not in stats.cql


def test_bucket_literals_are_crc32_not_murmur3() -> None:
    """The schema comments said murmur3 for months while `codec.bucket` was
    CRC-32. A DAL that believed the comment would address the wrong partition
    and get an empty result that looks exactly like missing data."""
    from graphsense_v3.codec import bucket

    address = b"\xa1" * 21
    results, _ = _run_dry()
    stats = next(r for r in results if r.name == "address stats (epoch slice)")
    expected = bucket(address, CONFIG["entity_buckets"])
    assert f"address_bucket = {expected}" in stats.cql


def test_render_inlines_every_parameter_type() -> None:
    cql = "SELECT * FROM t WHERE a = %s AND b = %s AND c = %s AND d = %s"
    rendered = prober._render(cql, (b"\x01\x02", True, "x", 7))
    assert (
        rendered
        == "SELECT * FROM t WHERE a = 0x0102 AND b = true AND c = 'x' AND d = 7"
    )


def test_missing_configuration_row_is_a_clear_failure() -> None:
    """Every bucket the probe computes comes from that row; without it each
    query would address a wrong partition and report an empty table."""
    with pytest.raises(SystemExit, match="configuration row"):
        prober._configuration(FakeSession(), DERIVED, fallback=RAW)


def test_configuration_falls_back_to_the_raw_keyspace() -> None:
    """Derived keyspaces backfilled before the job wrote their own row have an
    empty configuration table. Both are written from one NetworkConfig in a
    single run, so the raw row is the same constants."""

    class OnlyRaw(FakeSession):
        def execute(self, cql, params=()):
            super().execute(cql, params)
            if RAW in cql:
                row = type("Row", (), {"_asdict": lambda self: dict(CONFIG)})
                return [row()]
            return []

    assert prober._configuration(OnlyRaw(), DERIVED, fallback=RAW) == CONFIG


def test_report_lists_required_failures_separately() -> None:
    results = [
        prober.Result("a", "e", "SELECT 1", 0, 1.0, 1, prober.REQUIRED, "boom", "why"),
        prober.Result("b", "e", "SELECT 2", 0, 1.0, 1, prober.OPTIONAL, "meh", ""),
        prober.Result("c", "e", "SELECT 3", 5, 1.0, 1),
    ]
    text = prober.report(results, CONFIG)
    assert "1 required failure(s)" in text
    assert "FAIL" in text and "warn" in text
    assert "entity_buckets=128" in text
