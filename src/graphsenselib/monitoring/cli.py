import csv
from io import StringIO

import click

from ..cli.common import out_file, require_currency, require_environment
from ..config import config
from ..utils.console import console
from .monitoring import DbSummaryRecord, get_db_summary_record
from .notifications import send_msg_to_topic


@click.group()
def monitoring_cli():
    pass


@monitoring_cli.group("monitoring")
def monitoring():
    """Tools to monitor the graphsense infrastructure."""
    pass


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
