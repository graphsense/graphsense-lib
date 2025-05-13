import logging
import sys
from datetime import date, timedelta

import click

from ..cli.common import require_currency, require_environment
from ..config import get_config, supported_fiat_currencies
from ..utils.console import console
from .coindesk import MIN_START as MS_CD
from .coindesk import fetch as fetchCD
from .coindesk import ingest as ingestCD
from .coingecko import fetch as fetchGecko
from .coingecko import fetch_impl as fetchGeckoDump
from .coingecko import ingest as ingestGecko
from .coinmarketcap import MIN_START as MS_CMK
from .coinmarketcap import fetch as fetchCMK
from .coinmarketcap import fetch_impl as fetchCMKDump
from .coinmarketcap import ingest as ingestCMK
from .cryptocompare import MIN_START as MS_CC
from .cryptocompare import fetch as fetchCC
from .cryptocompare import fetch_impl as dumpCC
from .cryptocompare import ingest as ingestCC

logger = logging.getLogger(__name__)


def get_api_key(key):
    config = get_config()
    api_key_key = f"{key}_api_key"
    api_key = getattr(config, api_key_key)
    if not api_key.strip():
        logger.error(f"Please provide an API key (graphsense.yaml -> {api_key_key})")
        sys.exit(1)
    return api_key


def shared_flags(provider="cmc"):
    def inner(function):
        if provider == "cryptocompare":
            # needs to be offset-aware because
            prev_date = date.today() - timedelta(days=1)
            prev_date = prev_date.strftime("%Y-%m-%dT00:00:00.000000+00:00")
        else:
            prev_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        min_date = "2009-01-03"
        if provider == "cmc":
            min_date = MS_CMK
        elif provider == "cdesk":
            min_date = MS_CD
        elif provider == "cryptocompare":
            min_date = MS_CC

        function = click.option(
            "--fiat-currencies",
            default=supported_fiat_currencies,
            multiple=True,
            show_default=True,
            help="List of fiat currencies.",
        )(function)

        function = click.option(
            "--start-date",
            default=min_date,
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


@exchange_rates.group()
def coingecko():
    """From coingecko."""
    pass


@exchange_rates.group()
def cryptocompare():
    """From cryptocompare."""
    pass


@coinmarketcap.command("dump")
@require_currency()
@shared_flags()
@click.option(
    "--out-file",
    default="rates.csv",
    type=str,
    help="file to dump into.",
)
def fetch_cmk_dump(
    currency: str,
    fiat_currencies: list[str],
    start_date: str,
    end_date: str,
    out_file: str,
):
    """Save exchange rates to file.
    \f
    Args:
        env (str): -
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
    """
    api_key = get_api_key("coinmarketcap")
    df = fetchCMKDump(
        None,
        None,
        currency,
        list(fiat_currencies),
        start_date,
        end_date,
        None,
        True,
        False,
        False,
        api_key,
    )
    console.rule("Rates Coinmarketcap")
    console.print(df)
    console.rule(f"Writing to {out_file}")
    df.to_csv(out_file)


@coingecko.command("dump")
@require_currency()
@shared_flags(provider="gecko")
@click.option(
    "--out-file",
    default="rates.csv",
    type=str,
    help="file to dump into.",
)
def fetch_coingecko_dump(
    currency: str,
    fiat_currencies: list[str],
    start_date: str,
    end_date: str,
    out_file: str,
):
    """Save exchange rates to file.
    \f
    Args:
        env (str): -
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
    """
    api_key = get_api_key("coingecko")
    df = fetchGeckoDump(
        None,
        None,
        currency,
        list(fiat_currencies),
        start_date,
        end_date,
        None,
        True,
        False,
        False,
        api_key,
    )
    console.rule("Rates Coingecko")
    console.print(df)
    console.rule(f"Writing to {out_file}")
    df.to_csv(out_file)


@cryptocompare.command("dump")
@require_currency()
@shared_flags(provider="cryptocompare")
@click.option(
    "--out-file",
    default="rates.csv",
    type=str,
    help="file to dump into.",
)
def fetch_cryptocompare_dump(
    currency: str,
    fiat_currencies: list[str],
    start_date: str,
    end_date: str,
    out_file: str,
):
    """Save exchange rates to file.
    \f
    Args:
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
        out_file (str): -
    """
    df = dumpCC(
        None,
        currency,
        list(fiat_currencies),
        start_date,
        end_date,
        None,
        True,
        True,
        False,
    )
    console.rule("Rates Coingecko")
    console.print(df)
    console.rule(f"Writing to {out_file}")
    df.to_csv(out_file)


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
    api_key = get_api_key("coinmarketcap")
    df = fetchCMK(env, currency, list(fiat_currencies), start_date, end_date, api_key)
    console.rule("Rates Coinmarketcap")
    console.print(df)


@coingecko.command("fetch")
@require_environment()
@require_currency()
@shared_flags(provider="gecko")
def fetch_gecko(
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
    api_key = get_api_key("coingecko")
    df = fetchGecko(env, currency, list(fiat_currencies), start_date, end_date, api_key)
    console.rule("Rates Coingecko")
    console.print(df)


@coindesk.command("fetch")
@require_environment()
@require_currency()
@shared_flags(provider="cdesk")
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


@cryptocompare.command("fetch")
@require_environment()
@require_currency()
@shared_flags(provider="cryptocompare")
def fetch_cc(
    env: str, currency: str, fiat_currencies: list[str], start_date: str, end_date: str
):
    """Fetches and prints exchange rates.
    \f
    Args:
        env (str): -
        currency (str): -
        fiat_currencies (list[str]): -
        start_date (str): -
        end_date (str): -
    """
    df = fetchCC(env, currency, list(fiat_currencies), start_date, end_date)
    console.rule("Rates cryptocompare")
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
    api_key = get_api_key("coinmarketcap")
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
        api_key,
    )


@coingecko.command("ingest")
@require_environment()
@require_currency()
@shared_flags()
@shared_ingest_flags()
def ingest_gecko(
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
    api_key = get_api_key("coingecko")
    ingestGecko(
        env,
        currency,
        list(fiat_currencies),
        start_date,
        end_date,
        table,
        force,
        dry_run,
        abort_on_gaps,
        api_key,
    )


@coindesk.command("ingest")
@require_environment()
@require_currency()
@shared_flags(provider="cdesk")
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


@cryptocompare.command("ingest")
@require_environment()
@require_currency()
@shared_flags(provider="cryptocompare")
@shared_ingest_flags()
def ingest_cc(
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
    ingestCC(
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
