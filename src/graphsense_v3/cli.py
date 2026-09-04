"""``graphsense-v3`` -- the v3 backfill CLI.

Deliberately its own entry point rather than a group added to
``graphsense-cli``: the v3 work is meant not to touch existing code, and a
separate command also makes it obvious at a glance which backend a shell
history entry was driving.

Everything comes from ``graphsense.yaml``. The one thing that does *not* is the
target keyspace name -- see :mod:`graphsense_v3.settings` for why.
"""

from __future__ import annotations

import logging
from typing import Optional

import click

from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.schema.emit import KEYSPACE_PLACEHOLDER, REPLICATION_PLACEHOLDER
from graphsense_v3.schema.render import render_schema, replication
from graphsense_v3.settings import RunSettings, from_config

_ENV = click.option(
    "--env", "-e", default="prod", show_default=True, help="config environment"
)
_NETWORK = click.option("--network", "-n", required=True, help="btc, eth, trx, ...")
_LABEL = click.option(
    "--label",
    default=None,
    help="suffix keeping successive v3 runs apart, e.g. --label aug",
)


def _settings(env: str, network: str, label: Optional[str], profile: Optional[str]):
    return from_config(env, network, label=label, spark_profile=profile)


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="debug logging")
def cli(verbose: bool) -> None:
    """Backfill and inspect the v3 keyspaces."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


@cli.command("plan")
@_ENV
@_NETWORK
@_LABEL
@click.option("--spark-profile", default=None, help="override the configured profile")
@click.option(
    "--writer",
    type=click.Choice(["connector", "sidecar"]),
    default="connector",
    show_default=True,
    help="which write path the plan should REPORT. Pass the same value the run "
    "will use, or the plan describes a run nobody is going to make.",
)
def plan(
    env: str,
    network: str,
    label: Optional[str],
    spark_profile: Optional[str],
    writer: str,
) -> None:
    """Show what a run would do, without touching anything.

    Worth reading before every run: it names the keyspaces that get written and
    the one that is only read.
    """
    settings = _settings(env, network, label, spark_profile)
    if writer == "sidecar":
        settings = settings.with_sidecar()
    click.echo(settings.describe())


def _replication_options(command):
    for option in reversed(
        [
            click.option(
                "--replication-factor", type=int, default=2, show_default=True
            ),
            click.option("--datacenter", default="DC1", show_default=True),
            click.option(
                "--allow-single-replica",
                is_flag=True,
                help="permit RF 1. Fine for a benchmark keyspace, where it "
                "halves the disk and the write cost; never for one anything "
                "depends on, since a single node loss then loses data -- which "
                "is how a production keyspace sat at RF 1 unnoticed.",
            ),
        ]
    ):
        command = option(command)
    return command


def _cql(network, kind, label, factor, datacenter, single):
    from graphsense_v3.settings import v3_keyspace

    cql = render_schema(
        schema_for(network, kind),
        v3_keyspace(network, kind, label),
        replication({datacenter: factor}, allow_single_replica=single),
    )
    assert KEYSPACE_PLACEHOLDER not in cql and REPLICATION_PLACEHOLDER not in cql
    return cql


@cli.command("schema")
@_NETWORK
@_LABEL
@click.option("--kind", type=click.Choice([k.value for k in Kind]), required=True)
@_replication_options
def schema(
    network: str,
    label: Optional[str],
    kind: str,
    replication_factor: int,
    datacenter: str,
    allow_single_replica: bool,
) -> None:
    """Print the CQL for a v3 keyspace, ready to pipe into cqlsh."""
    click.echo(
        _cql(
            network,
            Kind(kind),
            label,
            replication_factor,
            datacenter,
            allow_single_replica,
        )
    )


@cli.command("create")
@_ENV
@_NETWORK
@_LABEL
@_replication_options
@click.option(
    "--kind",
    type=click.Choice([*(k.value for k in Kind), "both"]),
    default="both",
    show_default=True,
)
def create(
    env: str,
    network: str,
    label: Optional[str],
    kind: str,
    replication_factor: int,
    datacenter: str,
    allow_single_replica: bool,
) -> None:
    """Create the v3 keyspaces if they do not exist.

    Every statement is IF NOT EXISTS, so this is idempotent and safe to re-run
    before each attempt. It can only ever address a v3 keyspace name.
    """
    from graphsense_v3.cassandra import apply_cql

    settings = _settings(env, network, label, None)
    kinds = list(Kind) if kind == "both" else [Kind(kind)]
    for which in kinds:
        cql = _cql(
            network, which, label, replication_factor, datacenter, allow_single_replica
        )
        click.echo(f"{which.value}: {apply_cql(settings, cql)} statement(s) applied")


@cli.command("run")
@_ENV
@_NETWORK
@_LABEL
@click.option("--start-block", type=int, default=None)
@click.option("--end-block", type=int, default=None)
@click.option(
    "--stages",
    default="raw,derived",
    show_default=True,
    help="comma-separated: raw, derived",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="build every frame and check it against the schema, but write nothing",
)
@click.option("--spark-profile", default=None, help="override the configured profile")
@click.option(
    "--writer",
    type=click.Choice(["connector", "sidecar"]),
    default="connector",
    show_default=True,
    help="sidecar bulk-writes SSTables instead of going through the CQL path. "
    "Verified on LTC 0-100000: identical summary_statistics, 4m28s against 88m, "
    "with transaction_io alone going from 4933s to under 85s.",
)
@click.option("--local", is_flag=True, help="run Spark locally (for a smoke test)")
@click.option("--yes", is_flag=True, help="skip the confirmation prompt")
def run(
    env: str,
    network: str,
    label: Optional[str],
    start_block: Optional[int],
    end_block: Optional[int],
    stages: str,
    dry_run: bool,
    spark_profile: Optional[str],
    writer: str,
    local: bool,
    yes: bool,
) -> None:
    """Backfill the v3 keyspaces from the Delta Lake."""
    from graphsense_v3.spark import job
    from graphsense_v3.spark.session import create_session

    settings = _settings(env, network, label, spark_profile)
    if writer == "sidecar":
        settings = settings.with_sidecar()
    click.echo(settings.describe())
    wanted = tuple(s.strip() for s in stages.split(",") if s.strip())
    unknown = set(wanted) - {"raw", "derived"}
    if unknown:
        raise click.BadParameter(f"unknown stage(s): {', '.join(sorted(unknown))}")

    if not (dry_run or yes):
        click.confirm("\nStart this run?", abort=True)

    spark = create_session(
        f"graphsense-v3-{network}",
        local=local,
        cassandra_nodes=settings.cassandra_nodes,
        cassandra_username=settings.username,
        cassandra_password=settings.password,
        s3_credentials=settings.s3_credentials,
        spark_config=settings.spark_config,
        spark_packages=settings.spark_packages,
    )
    try:
        job.run(
            spark,
            settings,
            start_block=start_block,
            end_block=end_block,
            dry_run=dry_run,
            stages=wanted,
        )
    finally:
        spark.stop()


@cli.command("probe")
@_ENV
@_NETWORK
@_LABEL
@click.option(
    "--hosts",
    default=None,
    help="host[:port][,host...]. Given, graphsense.yaml is not read at all -- "
    "the keyspace names come from --network/--label, which is everything this "
    "command needs. Useful from a laptop that has the cluster but not the "
    "config.",
)
@click.option("--username", default=None, help="only with --hosts")
@click.option("--password", default=None, help="only with --hosts")
def probe(
    env: str,
    network: str,
    label: Optional[str],
    hosts: Optional[str],
    username: Optional[str],
    password: Optional[str],
) -> None:
    """Run every read the DAL needs against a backfilled keyspace.

    Read-only: every statement is a SELECT, and both keyspace names are checked
    against the v3 pattern first. Reports how many partition reads each access
    pattern costs and prints the CQL, so a failure pastes into cqlsh.
    """
    from graphsense_v3 import probe as prober
    from graphsense_v3.settings import v3_keyspace

    if hosts:
        nodes = [h.strip() for h in hosts.split(",") if h.strip()]
        raw = v3_keyspace(network, Kind.RAW, label)
        derived = v3_keyspace(network, Kind.DERIVED, label)
    else:
        settings = _settings(env, network, label, None)
        nodes = settings.cassandra_nodes
        raw, derived = settings.raw_keyspace, settings.derived_keyspace
        username, password = settings.username, settings.password
    click.echo(f"probing {raw} + {derived} on {', '.join(nodes)}", err=True)
    results, config = prober.run(
        nodes, raw, derived, username=username, password=password
    )
    click.echo(prober.report(results, config))
    failed = sum(1 for r in results if r.kind is prober.REQUIRED and not r.ok)
    if failed:
        raise SystemExit(f"{failed} required access pattern(s) not satisfied")


@cli.command("fill-rates")
@_NETWORK
@_LABEL
@click.option("--hosts", default=None, help="host[:port][,host...]")
@click.option("--username", default=None)
@click.option("--password", default=None)
@click.option("--asset", default=None, help="defaults to the network's own symbol")
@click.option("--env", default="prod", help="only used without --hosts")
@click.option("--dry-run", is_flag=True, help="report what would be written")
def fill_rates_cmd(
    network: str,
    label: Optional[str],
    hosts: Optional[str],
    username: Optional[str],
    password: Optional[str],
    asset: Optional[str],
    env: str,
    dry_run: bool,
) -> None:
    """Carry the last exchange rate forward over a keyspace's unrated tail.

    Rates land a day at a time, so a backfill that reaches the chain tip always
    ends with a few hundred unrated blocks -- and one missing rate row at the
    tip takes out `get_address` and every neighbour listing, because those ask
    for CURRENT rates and that resolves to the tip.

    The same thing a backfill now does, without re-running one. Writes ONLY to
    a v3 derived keyspace's `exchange_rates`.
    """
    from graphsense_v3 import ratefill
    from graphsense_v3.cassandra import connect_to
    from graphsense_v3.probe import configuration
    from graphsense_v3.settings import Kind, v3_keyspace

    raw = v3_keyspace(network, Kind.RAW, label)
    derived = v3_keyspace(network, Kind.DERIVED, label)
    if hosts:
        nodes = [h.strip() for h in hosts.split(",") if h.strip()]
    else:
        settings = _settings(env, network, label, None)
        nodes = settings.cassandra_nodes
        username, password = settings.username, settings.password

    cluster = connect_to(nodes, username, password)
    session = cluster.connect()
    try:
        config = configuration(session, derived, fallback=raw)
        symbol = (asset or network).upper()
        # The tail first, so those blocks get the last REAL rate; the zero fill
        # then only reaches blocks with no earlier rate to carry.
        summary = ratefill.fill(
            session,
            raw,
            derived,
            symbol,
            size=config["block_bucket_size"],
            dry_run=dry_run,
        )
        zeros = ratefill.zero_fill(
            session,
            raw,
            derived,
            symbol,
            size=config["block_bucket_size"],
            dry_run=dry_run,
        )
    finally:
        cluster.shutdown()

    verb = "would write" if dry_run else "wrote"
    click.echo(
        f"{derived}: last real rate at block {summary['rated']}, tip "
        f"{summary['tip']}; {verb} {summary['written']} carried row(s), skipped "
        f"{summary['skipped']}; {verb} {zeros['written']} zero row(s) for blocks "
        f"with no rate to carry"
    )
    if (summary["written"] or zeros["written"]) and not dry_run:
        click.echo(
            "Carried rows repeat a real rate; zero rows say 'no rate existed', "
            "which is what v2 already reports for the early chain. Neither is "
            "an independent observation, and a backfill overwrites both with "
            "whatever the feed really holds."
        )


@cli.command("backtest")
@_NETWORK
@_LABEL
@click.option(
    "--config-file",
    default=None,
    help="gs-rest config yaml. Supplies the v2 keyspaces and the Cassandra "
    "connection BOTH backends are read through, so the comparison differs in "
    "the DAL and nothing else.",
)
@click.option(
    "--address",
    "addresses",
    multiple=True,
    help="fixture address, in V3's spelling; the v2 side is re-versioned. "
    "Repeatable. Omitted, fixtures are discovered in the v3 keyspace.",
)
@click.option("--tx-hash", "tx_hashes", multiple=True, help="fixture tx hash (hex)")
@click.option("--block", "blocks", multiple=True, type=int, help="fixture block height")
@click.option(
    "--stub-clusters",
    is_flag=True,
    help="let get_fresh_cluster_id report None instead of raising, so the rest "
    "of the address surface can be compared before clustering (D9) lands. "
    "Cluster FIELDS stay excluded, and the report says the run used this.",
)
@click.option(
    "--sample",
    default=0,
    type=int,
    help="additionally compare N addresses sampled across the token ring. Two "
    "auto-picked fixtures agreeing proves little; a spread is what catches "
    "systematic drift.",
)
def backtest_cmd(
    network: str,
    label: Optional[str],
    config_file: Optional[str],
    addresses: tuple,
    tx_hashes: tuple,
    blocks: tuple,
    sample: int,
    stub_clusters: bool,
) -> None:
    """Compare v2 and v3 REST answers, service against service.

    Read-only on both sides. The tagstore is stubbed out in both containers, so
    a difference reported here is a Cassandra difference and nothing else.
    """
    import asyncio

    from graphsenselib.web.app import resolve_rest_config

    from graphsense_v3 import backtest as harness
    from graphsense_v3 import compare
    from graphsense_v3.cassandra import connect_to
    from graphsense_v3.db.core import Dal
    from graphsense_v3.db.legacy import LegacyAdapter
    from graphsense_v3.probe import configuration
    from graphsense_v3.settings import Kind, assert_v3_keyspace, v3_keyspace

    rest_config = (
        resolve_rest_config(config_file=config_file)
        if config_file
        else resolve_rest_config()
    )
    db_config = rest_config.database
    if db_config is None:
        raise SystemExit(
            "the resolved gs-rest config has no `database` section, so there is "
            "no v2 keyspace to compare against; pass --config-file"
        )

    raw = v3_keyspace(network, Kind.RAW, label)
    derived = v3_keyspace(network, Kind.DERIVED, label)
    for keyspace in (raw, derived):
        assert_v3_keyspace(keyspace)

    nodes = harness.with_port(db_config.nodes, db_config.port)
    cluster = connect_to(nodes, db_config.username, db_config.password)
    session = cluster.connect()
    try:
        if addresses or tx_hashes or blocks:
            fixtures = harness.Fixtures(
                network=network,
                addresses=list(addresses),
                tx_hashes=list(tx_hashes),
                blocks=list(blocks),
            )
        else:
            fixtures = harness.fixtures_from_v3(session, raw, derived, network)
        if sample:
            drawn = harness.sample_addresses(
                session,
                derived,
                network,
                sample,
                buckets=configuration(session, derived, raw)["entity_buckets"],
            )
            fixtures.addresses = list(dict.fromkeys(fixtures.addresses + drawn))
        warning = harness.rate_coverage_warning(
            session,
            raw,
            derived,
            network,
            configuration(session, derived, raw).get("block_bucket_size") or 100,
        )
        if warning:
            click.echo(f"\nWARNING: {warning}\n", err=True)
        click.echo(
            f"fixtures: {len(fixtures.addresses)} address(es), "
            f"{len(fixtures.tx_hashes)} tx(s), {len(fixtures.blocks)} block(s)",
            err=True,
        )

        v2_db = _v2_dal(db_config)
        v3_db = LegacyAdapter(
            {network: Dal(session, raw, derived, configuration(session, derived, raw))},
            stub_clusters=stub_clusters,
        )

        async def _compare():
            # The synchronous `get_token_configuration` cannot fetch on demand,
            # so an account network's tokens are loaded before the first call.
            await v3_db.preload_token_configuration()
            return await harness.run(
                harness.build_services(rest_config, v2_db),
                harness.build_services(rest_config, v3_db),
                fixtures,
            )

        reports = asyncio.run(_compare())
    finally:
        cluster.shutdown()

    notes = []
    if stub_clusters:
        notes.append(
            "CLUSTERS ARE STUBBED (--stub-clusters): get_fresh_cluster_id "
            "reported None rather than raising, so calls that enrich a result "
            "with cluster data completed. Nothing about clustering was "
            "verified -- v3 has no cluster tables (D9)."
        )
    if warning:
        notes.append(warning)
    click.echo(compare.report(reports, notes))
    disagreed = [r for r in reports if not r.agrees and r.skipped is None]
    if disagreed:
        raise SystemExit(f"{len(disagreed)} call(s) differ")


def _v2_dal(db_config):
    """The v2 async DAL, built the way the web app builds it."""
    import importlib
    import logging

    driver = db_config.driver.lower()
    module = importlib.import_module("graphsenselib.db.asynchronous." + driver)
    return getattr(module, driver.capitalize())(db_config, logging.getLogger("v2"))


def main() -> None:
    cli()


__all__ = ["cli", "main", "RunSettings"]
