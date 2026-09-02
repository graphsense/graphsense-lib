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
def plan(
    env: str, network: str, label: Optional[str], spark_profile: Optional[str]
) -> None:
    """Show what a run would do, without touching anything.

    Worth reading before every run: it names the keyspaces that get written and
    the one that is only read.
    """
    click.echo(_settings(env, network, label, spark_profile).describe())


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
    help="sidecar bulk-writes SSTables instead of going through the CQL path; "
    "much faster for volume, and not yet exercised from PySpark against a real "
    "cluster -- prove it on a small block range first.",
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


def main() -> None:
    cli()


__all__ = ["cli", "main", "RunSettings"]
