import json
import logging
from datetime import date, datetime
from typing import List, Optional

import pandas as pd
import requests

from ..db import DbFactory
from ..db.analytics import DATE_FORMAT
from ..utils.generic import batch_date, generate_date_range_days

logger = logging.getLogger(__name__)

MIN_START = "2015-01-01"
FX_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"

coingecko_symbol_translation = {
    "btc": "bitcoin",
    "ltc": "litecoin",
    "trx": "tron",
    "zec": "zcash",
    "bch": "bitcoin-cash",
    "eth": "ethereum",
}


def fetch_ecb_rates(symbol_list: List) -> pd.DataFrame:
    """Fetch and preprocess FX rates from ECB."""

    logger.info(f"Fetching conversion rates for FIAT currencies from {FX_URL}")
    rates_eur = pd.read_csv(FX_URL)  # exchange rates based on EUR
    rates_eur = rates_eur.iloc[:, :-1]  # remove empty last column
    rates_eur["EUR"] = 1.0
    # convert to values based on USD
    rates_usd = rates_eur[symbol_list].div(rates_eur.USD, axis=0)
    rates_usd["date"] = rates_eur.Date
    logger.info(f"Last record: {rates_usd.date.tolist()[0]}")
    return rates_usd


def coingecko_historical_url(symbol: str, start: date, end: date) -> str:
    """Returns URL for CoinMarketCap API request."""

    fromd = int(datetime.combine(start, datetime.max.time()).timestamp())
    tod = int(datetime.combine(end, datetime.max.time()).timestamp())

    return (
        "https://pro-api.coingecko.com/api/v3/coins/"
        f"{coingecko_symbol_translation[symbol]}"
        f"/ohlc/range?vs_currency=usd&from={fromd}&to={tod}&interval=daily"
    )


def parse_coingecko_historical_response(
    response: requests.Response,
) -> pd.DataFrame:
    """Parse historical exchange rates (JSON) from coingecko."""
    # flake8: noqa: E741
    json_data = [
        [date.fromtimestamp(d // 1000).strftime(DATE_FORMAT), c]
        for d, o, h, l, c in json.loads(response.content)
    ]

    return pd.DataFrame(json_data, columns=["date", "USD"])


def fetch_coingecko_rates(
    start: str, end: str, crypto_currency: str, api_key: str
) -> pd.DataFrame:
    """Fetch cryptocurrency exchange rates from CoinMarketCap."""

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/87.0.4280.88 Safari/537.36"
    )
    headers = {
        "User-Agent": user_agent,
        "x-cg-pro-api-key": api_key,
        "accept": "accept: application/json",
    }

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    # coingecko restricts queries to 180 days
    cmc_rates = pd.DataFrame()
    for s, e in batch_date(start_date, end_date, days=180):
        url = coingecko_historical_url(crypto_currency, s, e)

        logger.info(f"Fetching {crypto_currency} exchange rates from {url}")
        rsession = requests.Session()
        rsession.mount("https://", requests.adapters.HTTPAdapter(max_retries=5))
        response = rsession.get(url, headers=headers)
        cmc_rates_part = parse_coingecko_historical_response(response)
        cmc_rates = pd.concat([cmc_rates, cmc_rates_part])

    # assert that all dates are consecutive
    assert len(cmc_rates.date) == len(set(cmc_rates.date))
    assert (
        set(cmc_rates.date).difference(
            {
                x.strftime(DATE_FORMAT)
                for x in generate_date_range_days(start_date, end_date)
            }
        )
        == set()
    )

    if len(cmc_rates) > 0:
        last_record = cmc_rates.date.tolist()[-1]
    else:
        last_record = None
    logger.info(f"Last record: {last_record}")
    return cmc_rates


def fetch(env, currency, fiat_currencies, start_date, end_date, api_key: str):
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
            True,
            api_key,
        )


def fetch_impl(
    db: Optional[object],
    env: Optional[str],
    currency: str,
    fiat_currencies: List[str],
    start_date: Optional[str],
    end_date: Optional[str],
    table: Optional[str],
    force: bool,
    dry_run: bool,
    abort_on_gaps: bool,
    api_key: str,
):
    if datetime.fromisoformat(start_date) < datetime.fromisoformat(MIN_START):
        start_date = MIN_START

    # query most recent data
    if not force and db:
        logger.info(f"Get last imported rate from {db.raw.get_keyspace()}")
        most_recent_date = db.raw.get_last_exchange_rate_date(table=table)
        if most_recent_date is not None:
            start_date = most_recent_date.strftime(DATE_FORMAT)

    logger.info(f"*** Fetch exchange rates for {currency} ***")
    logger.info(f"Start date: {start_date}")
    logger.info(f"End date: {end_date}")
    logger.info(f"Target fiat currencies: {fiat_currencies}")

    if datetime.fromisoformat(start_date) > datetime.fromisoformat(end_date):
        logger.error("Error: start date after end date.")
        raise SystemExit

    # fetch cryptocurrency exchange rates in USD
    cmc_rates = fetch_coingecko_rates(start_date, end_date, currency, api_key)

    ecb_rates = fetch_ecb_rates(fiat_currencies)

    # query conversion rates and merge converted values in exchange rates
    exchange_rates = cmc_rates
    date_range = pd.date_range(
        date.fromisoformat(start_date), date.fromisoformat(end_date)
    )
    date_range = pd.DataFrame(date_range, columns=["date"])
    date_range = date_range["date"].dt.strftime("%Y-%m-%d")

    for fiat_currency in set(fiat_currencies) - {"USD"}:
        ecb_rate = ecb_rates[["date", fiat_currency]].rename(
            columns={fiat_currency: "fx_rate"}
        )
        merged_df = cmc_rates.merge(ecb_rate, on="date", how="left").merge(
            date_range, how="right"
        )

        # fill gaps over weekends
        merged_df["fx_rate"] = merged_df["fx_rate"].ffill()
        merged_df["fx_rate"] = merged_df["fx_rate"].bfill()

        if abort_on_gaps and merged_df["fx_rate"].isnull().values.any():
            logger.error(
                "Error: found missing values for currency "
                f"{fiat_currency}, aborting import. Probably a weekend."
            )
            logger.error(merged_df[merged_df["fx_rate"].isnull()])
            if not dry_run:
                # in case of dry run let it run
                # to see what would have been written to the db
                if len(merged_df[merged_df["fx_rate"].isnull()]) > 4:
                    # if missing more than 4 days, critical error
                    raise SystemExit(2)
                else:
                    raise SystemExit(15)
        merged_df[fiat_currency] = merged_df["USD"] * merged_df["fx_rate"]
        merged_df = merged_df[["date", fiat_currency]]
        exchange_rates = exchange_rates.merge(merged_df, on="date")

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
    api_key,
):
    if dry_run:
        logger.warning("This is a Dry-Run. Nothing will be written to the database!")
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
            api_key,
        ).dropna()

        # insert final exchange rates into Cassandra
        if "USD" not in fiat_currencies:
            exchange_rates.drop("USD", axis=1, inplace=True)
        exchange_rates["fiat_values"] = exchange_rates.drop("date", axis=1).to_dict(
            orient="records"
        )
        exchange_rates.drop(fiat_currencies, axis=1, inplace=True)

        # insert exchange rates into Cassandra table
        if not dry_run:
            logger.info(f"Writing to keyspace {db.raw.get_keyspace()}")
            if len(exchange_rates) > 0:
                db.raw.ingest(table, exchange_rates.to_dict("records"))
                logger.info(f"Inserted rates for {len(exchange_rates)} days: ")
                logger.info(
                    f"{exchange_rates.iloc[0].date} - {exchange_rates.iloc[-1].date}"
                )
            else:
                logger.info("Nothing to insert.")
        else:
            logger.info(
                "Dry run: No data inserted. "
                f"Would have inserted {len(exchange_rates)} days."
            )
            logger.info(exchange_rates)
