import csv
import sys
from io import StringIO

import click

from ..cli.common import out_file, require_currency, require_environment
from ..config import get_config
from ..utils.console import console
from .consistency import EXIT_INCONSISTENT, Status, run_consistency_check
from .monitoring import (
    DbSummaryRecord,
    check_raw_ingest_staleness,
    get_db_summary_record,
)
from .notifications import send_msg_to_topic


@click.group()
def monitoring_cli():
    pass


@monitoring_cli.group("monitoring")
def monitoring():
    """Tools to monitor the graphsense infrastructure."""
    pass


@monitoring.command(
    "monitor-raw-ingest",
    short_help="checks if last data point is older than a threshold, optionally send slack notifications. ",
)
@require_environment()
@require_currency(required=False)
@click.option(
    "--threshold", type=int, required=True, help="threshold in hours", default=12
)
@click.option(
    "--topic",
    "-t",
    type=str,
    help="Topic to send to.",
    required=True,
    default="exceptions",
)
@click.option("--dry-run/--no-dry-run", default=False)
def monitor_raw_ingest(env, currency, threshold, topic, dry_run):
    """Receives a summary record of the current database state.
    \f

    Args:
        env (str): Env to work on
        currency (str): currency to work on (optional)
    """
    config = get_config()
    if currency is None:
        # check all configured currencies
        networks = config.get_environment(env).get_configured_currencies()
    else:
        networks = [currency]

    for net in networks:
        check_raw_ingest_staleness(env, net, threshold, topic=topic, dry_run=dry_run)


@monitoring.command(
    "check-consistency",
    short_help="Checks raw, delta lake and transformed for inconsistencies "
    "in the most recent blocks.",
)
@require_environment()
@require_currency(required=False)
@click.option(
    "--blocks",
    "n_blocks",
    type=int,
    default=100,
    show_default=True,
    help="Size of the window of most recent blocks to check.",
)
@click.option(
    "--sample-addresses",
    type=int,
    default=50,
    show_default=True,
    help="UTXO only: addresses from the newest synced blocks whose tx counters "
    "are recounted from address_transactions. 0 disables the recount.",
)
@click.option(
    "--max-address-rows",
    type=int,
    default=5000,
    show_default=True,
    help="Skip sampled addresses with more txs than this (their rows are read).",
)
@click.option(
    "--raw-tx-blocks",
    type=int,
    default=10,
    show_default=True,
    help="Account only: newest blocks whose lake txs are read back from raw by "
    "hash (raw has no per-block tx index; one read per tx). 0 disables.",
)
@click.option(
    "--lock/--no-lock",
    "use_lock",
    default=True,
    show_default=True,
    help="Hold the transformed keyspace lock during the check, so a delta "
    "update cannot change the counters under it.",
)
@click.option(
    "--topic",
    "-t",
    type=str,
    default=None,
    help="If set, send failed checks to this notification topic.",
)
def check_consistency(
    env,
    currency,
    n_blocks,
    sample_addresses,
    max_address_rows,
    raw_tx_blocks,
    use_lock,
    topic,
):
    """Checks raw, delta lake and transformed for inconsistencies in the most
    recent blocks: pending WAL / torn bookkeeping, height order, rows left
    above the highest block by an interrupted ingest, per-block
    tx/trace/log counts across raw and lake (including raw tx rows),
    transformed exchange rates, account block_transactions vs lake, and (UTXO)
    a sampled exact recount of address tx counters. Read-only.

    Exits 92 if any check fails, 911 if a lock could not be taken.
    \f
    Args:
        env (str): Env to work on
        currency (str): currency to work on (optional, default: all configured)
    """
    from ..utils.locking import LockAcquisitionError

    config = get_config()
    if currency is None:
        networks = config.get_environment(env).get_configured_currencies()
    else:
        networks = [currency]

    failed, locked = False, False
    for net in networks:
        try:
            findings = run_consistency_check(
                env,
                net,
                n_blocks,
                sample_addresses,
                max_address_rows,
                raw_tx_blocks,
                use_lock,
            )
        except LockAcquisitionError as e:
            locked = True
            console.print(
                f"{net}: {e} A delta update is probably running; retry later or "
                "pass --no-lock (counts may then move during the check)."
            )
            continue
        failures = [f for f in findings if f.status == Status.FAIL]
        if failures:
            failed = True
            if topic:
                send_msg_to_topic(
                    topic,
                    f"Consistency check {env}/{net} failed:\n"
                    + "\n".join(f"- {f.check}: {f.detail}" for f in failures),
                )
    if failed:
        sys.exit(EXIT_INCONSISTENT)
    if locked:
        sys.exit(911)


@monitoring.command(
    "get-summary",
    short_help="Receives a summary record of the current database state.",
)
@require_environment()
@require_currency(required=False)
@out_file(required=False, append=True)
@click.option("--no-header/--header", default=False)
def summary(env, currency, out_file, no_header):
    """Receives a summary record of the current database state.
    \f

    Args:
        env (str): Env to work on
        currency (str): currency to work on (optional)
    """
    config = get_config()
    if currency is None:
        # create rows for all configured currencies
        records = [
            get_db_summary_record(env, currency_config)
            for currency_config in config.get_environment(
                env
            ).get_configured_currencies()
        ]
    else:
        records = [get_db_summary_record(env, currency)]

    output_stream = StringIO() if out_file is None else out_file

    writer = csv.DictWriter(output_stream, fieldnames=DbSummaryRecord.get_fields())

    if not no_header and (out_file is None or out_file.tell() == 0):
        # write header either for new file or if print on console
        writer.writeheader()

    writer.writerows([record.get_dict() for record in records])

    if out_file is None:
        # print records to stdout if no file is specified
        console.print(output_stream.getvalue())


@monitoring.command(
    "notify",
    short_help="Sends a message to the "
    "configured handlers (e.g. a slack channel) by topic.",
)
@click.option(
    "--topic",
    "-t",
    type=str,
    help="Topic to send to.",
    required=True,
)
@click.option(
    "--msg",
    "-m",
    type=str,
    help="Message to send.",
    required=True,
)
def notify(topic, msg):
    send_msg_to_topic(topic, msg)
