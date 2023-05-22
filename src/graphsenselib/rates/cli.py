from datetime import date, timedelta

import click

from ..cli.common import require_currency, require_environment
from ..config import supported_fiat_currencies
from ..utils.console import console
from .coindesk import MIN_START as MS_CD
from .coindesk import fetch as fetchCD
from .coindesk import ingest as ingestCD
from .coinmarketcap import MIN_START as MS_CMK
from .coinmarketcap import fetch as fetchCMK
from .coinmarketcap import ingest as ingestCMK


def shared_flags(coinmarketcap=True):
    def inner(function):
        prev_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        function = click.option(
            "--fiat-currencies",
            default=supported_fiat_currencies,
            multiple=True,
            show_default=True,
            help="List of fiat currencies.",
        )(function)

        function = click.option(
            "--start-date",
            default=MS_CMK if coinmarketcap else MS_CD,
            type=str,
            help="start date for fetching exchange rates",
        )(function)

        function = click.option(
            "--end-date",
            default=prev_date,
            show_default=True,
            type=str,
            help="end date for fetching exchange rates",
        )(function)

        return function

    return inner


def shared_ingest_flags():
    def inner(function):
        function = click.option(
            "--force",
            "-f",
            is_flag=True,
            help=(
                "don't continue from last found Cassandra record "
                "and force overwrite of existing rows"
            ),
        )(function)

        function = click.option(
            "-t",
            "--table",
            default="exchange_rates",
            show_default=True,
            type=str,
            help="Name of the target exchange rate table",
        )(function)

        function = click.option(
            "--abort-on-gaps",
            is_flag=True,
            help=(
                "ECB does not provide courses on weekends and holidays. "
                "The default behavior of the script is to fill (interpolate). "
                "With this flag the script aborts if gaps are still present."
            ),
        )(function)

        function = click.option(
            "--dry-run",
            is_flag=True,
            help="Don't write new records to Cassandra.",
        )(function)
        return function

    return inner


@click.group()
def rates_cli():
    pass


@rates_cli.group()
def exchange_rates():
    """Fetching and ingesting exchange rates."""
    pass


@exchange_rates.group()
def coindesk():
    """From coindesk."""
    pass


@exchange_rates.group()
def coinmarketcap():
    """From coinmarketcap."""
    pass


@coinmarketcap.command("fetch")
@require_environment()
@require_currency()
@shared_flags()
def fetch_cmk(
    env: str, currency: str, fiat_currencies: list[str], start_date: str, end_date: str
):
    """Only fetches the to be imported exchange rates.
    \f
    Args:
        env (str): -
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
    """
    df = fetchCMK(env, currency, list(fiat_currencies), start_date, end_date)
    console.rule("Rates Coinmarketcap")
    console.print(df)


@coindesk.command("fetch")
@require_environment()
@require_currency()
@shared_flags(coinmarketcap=False)
def fetch_cd(
    env: str, currency: str, fiat_currencies: list[str], start_date: str, end_date: str
):
    """Only fetches the to be imported exchange rates.
    \f
    Args:
        env (str): -
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
    """
    df = fetchCD(env, currency, list(fiat_currencies), start_date, end_date)
    console.rule("Rates Coindesk")
    console.print(df)


@coinmarketcap.command("ingest")
@require_environment()
@require_currency()
@shared_flags()
@shared_ingest_flags()
def ingest_cmk(
    env: str,
    currency: str,
    fiat_currencies: list[str],
    start_date: str,
    end_date: str,
    table: str,
    force: bool,
    dry_run: bool,
    abort_on_gaps: bool,
):
    """Ingest exchange rates into Cassandra
    \f
    Args:
        env (str): -
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
        table (str): -
        force (bool): -
        dry_run (bool): -
        abort_on_gaps (bool): -
    """
    ingestCMK(
        env,
        currency,
        list(fiat_currencies),
        start_date,
        end_date,
        table,
        force,
        dry_run,
        abort_on_gaps,
    )


@coindesk.command("ingest")
@require_environment()
@require_currency()
@shared_flags(coinmarketcap=False)
@shared_ingest_flags()
def ingest_cd(
    env,
    currency,
    fiat_currencies,
    start_date,
    end_date,
    table,
    force,
    dry_run,
    abort_on_gaps,
):
    """Ingests new exchange rates into cassandra raw keyspace.
    \f
    Args:
        env (str): -
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
        table (str): -
        force (bool): -
        dry_run (bool): -
        abort_on_gaps (bool): -
    """
    ingestCD(
        env,
        currency,
        list(fiat_currencies),
        start_date,
        end_date,
        table,
        force,
        dry_run,
        abort_on_gaps,
    )
