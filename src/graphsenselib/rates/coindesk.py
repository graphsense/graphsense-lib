import logging
from datetime import datetime
from typing import List

import pandas as pd
import requests
from simplejson.errors import JSONDecodeError

from ..db import DbFactory
from ..db.analytics import DATE_FORMAT

MIN_START = "2010-10-17"  # no CoinDesk exchange rates available before
logger = logging.getLogger(__name__)


def fetch_exchange_rates(
    start_date: str, end_date: str, symbol_list: List
) -> pd.DataFrame:
    """Fetch BTC exchange rates from CoinDesk.

    Parameters
    ----------
    start_date : str
        Start date (ISO-format YYYY-mm-dd).
    end_date : str
        End date (ISO-format YYYY-mm-dd).
    symbol_list: list[str]
        ["EUR", "USD", "JPY" ...]

    Returns
    -------
    DataFrame
        Exchange rates in pandas DataFrame with columns 'date', 'fiat_values'
    """

    base_url = "https://api.coindesk.com/v1/bpi/historical/close.json"
    param = "?currency={}&start={}&end={}"

    df_merged = pd.DataFrame()

    for fiat in symbol_list:
        url = base_url + param.format(fiat, start_date, end_date)
        logger.info(f"Fetching url: {url}")
        request = requests.get(url)
        try:
            json = request.json()
            logger.info(json["disclaimer"])
            rates = pd.DataFrame.from_records([json["bpi"]]).transpose()
            rates.rename(columns={0: fiat}, inplace=True)
            df_merged = rates.join(df_merged)
        except JSONDecodeError as symbol_not_found:
            logger.error(f"Unknown currency: {fiat}")
            raise SystemExit(1) from symbol_not_found

    df_merged.reset_index(level=0, inplace=True)
    df_merged.rename(columns={"index": "date"}, inplace=True)
    df_merged["fiat_values"] = df_merged.drop("date", axis=1).to_dict(orient="records")

    [
        df_merged.drop(c, axis=1, inplace=True)
        for c in symbol_list
        if c in df_merged.keys()
    ]
    return df_merged


def fetch(env, currency, fiat_currencies, start_date, end_date):
    with DbFactory().from_config(env, currency) as db:
        return fetch_impl(
            db,
            env,
            currency,
            fiat_currencies,
            start_date,
            end_date,
            None,
            start_date != MIN_START,
            False,
            False,
        )


def fetch_impl(
    db,
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

    if datetime.fromisoformat(start_date) < datetime.fromisoformat(MIN_START):
        logger.warning(f"Warning: Exchange rates not available before {MIN_START}")
        start_date = MIN_START

    # query most recent data in 'exchange_rates' table
    if not force:
        logger.info(f"Get last imported rate from {db.raw.get_keyspace()}")
        most_recent_date = db.raw.get_last_exchange_rate_date(table=table)
        if most_recent_date is not None:
            start_date = most_recent_date.strftime(DATE_FORMAT)

    logger.info("*** Starting exchange rate ingest for BTC ***")
    logger.info(f"Start date: {start_date}")
    logger.info(f"End date: {end_date}")

    if datetime.fromisoformat(start_date) > datetime.fromisoformat(end_date):
        logger.error("Error: start date after end date.")
        raise SystemExit(1)

    logger.info(f"Target fiat currencies: {fiat_currencies}")
    exchange_rates = fetch_exchange_rates(start_date, end_date, fiat_currencies)
    return exchange_rates


def ingest(
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
    with DbFactory().from_config(env, currency) as db:
        exchange_rates = fetch_impl(
            db,
            env,
            currency,
            fiat_currencies,
            start_date,
            end_date,
            table,
            force,
            dry_run,
            abort_on_gaps,
        )

        # insert exchange rates into Cassandra table
        if not dry_run:
            logger.info(f"Writing to keyspace {db.raw.get_keyspace()}")
            db.raw.ingest(table, exchange_rates)
            print(f"Inserted rates for {len(exchange_rates)} days: ", end="")
            print(f"{exchange_rates.iloc[0].date} - {exchange_rates.iloc[-1].date}")
        else:
            print("Dry run: No data inserted. Would have inserted:")
            print(exchange_rates)
