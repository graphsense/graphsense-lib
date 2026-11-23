import json
import logging
from datetime import date, datetime, time, timezone
from typing import List, Optional

import pandas as pd
import requests

from ..db import DbFactory
from ..db.analytics import DATE_FORMAT
from ..utils.generic import batch_date, generate_date_range_days
from .utils import (
    HTTP_TIMEOUT,
    forward_filled_fx_rate,
    normalize_date_bounds,
    rates_session,
    read_csv_url,
)

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
    rates_eur = read_csv_url(FX_URL, compression="zip")  # rates based on EUR
    rates_eur = rates_eur.iloc[:, :-1]  # remove empty last column
    rates_eur["EUR"] = 1.0
    # convert to values based on USD
    rates_usd = rates_eur[symbol_list].div(rates_eur.USD, axis=0)
    rates_usd["date"] = rates_eur.Date
    logger.info(f"Last record: {rates_usd.date.tolist()[0]}")
    return rates_usd


def coingecko_historical_url(symbol: str, start: date, end: date) -> str:
    """Returns URL for CoinGecko API request."""

    # UTC day bounds: the whole start day (00:00:00) up to the end of the end
    # day. Naive datetimes would be read in the host's local time zone.
    fromd = int(datetime.combine(start, time.min, tzinfo=timezone.utc).timestamp())
    tod = int(datetime.combine(end, time.max, tzinfo=timezone.utc).timestamp())

    return (
        "https://pro-api.coingecko.com/api/v3/coins/"
        f"{coingecko_symbol_translation[symbol]}"
        f"/ohlc/range?vs_currency=usd&from={fromd}&to={tod}&interval=daily"
    )


def parse_coingecko_historical_response(
    response: requests.Response,
) -> pd.DataFrame:
    """Parse historical exchange rates (JSON) from coingecko."""
    # Fail with the HTTP status (bad key, rate or plan limit) instead of
    # trying to unpack the error object as candles.
    response.raise_for_status()
    candles = json.loads(response.content)
    if not isinstance(candles, list):
        raise ValueError(f"Unexpected CoinGecko OHLC response: {candles!r:.500}")
    # flake8: noqa: E741
    json_data = [
        [
            datetime.fromtimestamp(d // 1000, tz=timezone.utc).strftime(DATE_FORMAT),
            c,
        ]
        for d, o, h, l, c in candles
    ]

    return pd.DataFrame(json_data, columns=["date", "USD"])


def fetch_coingecko_rates(
    start: str, end: str, crypto_currency: str, api_key: str
) -> pd.DataFrame:
    """Fetch cryptocurrency exchange rates from CoinGecko."""

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/87.0.4280.88 Safari/537.36"
    )
    headers = {
        "User-Agent": user_agent,
        "x-cg-pro-api-key": api_key,
        "accept": "application/json",
    }

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    # coingecko restricts queries to 180 days; batch_date includes both
    # ends, so a step of 179 days yields at most 180 days per request
    parts = []
    for s, e in batch_date(start_date, end_date, days=179):
        url = coingecko_historical_url(crypto_currency, s, e)

        logger.info(f"Fetching {crypto_currency} exchange rates from {url}")
        response = rates_session().get(url, headers=headers, timeout=HTTP_TIMEOUT)
        parts.append(parse_coingecko_historical_response(response))
    coingecko_rates = (
        pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame(columns=["date", "USD"])
    )

    expected_dates = [
        x.strftime(DATE_FORMAT) for x in generate_date_range_days(start_date, end_date)
    ]
    # drop candles outside the requested days, e.g. a boundary candle
    coingecko_rates = coingecko_rates[coingecko_rates.date.isin(expected_dates)]
    duplicates = coingecko_rates.date[coingecko_rates.date.duplicated()]
    if len(duplicates) > 0:
        raise ValueError(
            f"CoinGecko returned duplicate {crypto_currency} rates for "
            f"{sorted(set(duplicates))}"
        )
    coingecko_rates = coingecko_rates.sort_values("date").reset_index(drop=True)
    # missing days are not an error here: fetch_impl reports them and
    # decides, depending on abort_on_gaps, whether to abort
    missing = sorted(set(expected_dates) - set(coingecko_rates.date))
    if missing:
        logger.warning(
            f"CoinGecko returned no {crypto_currency} rate for {len(missing)} "
            f"day(s): {missing}"
        )

    if len(coingecko_rates) > 0:
        last_record = coingecko_rates.date.tolist()[-1]
    else:
        last_record = None
    logger.info(f"Last record: {last_record}")
    return coingecko_rates


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
    most_recent_date = None
    # query most recent data
    if not force and db:
        logger.info(f"Get last imported rate from {db.raw.get_keyspace()}")
        most_recent_date = db.raw.get_last_exchange_rate_date(table=table)

    start_dt, end_dt = normalize_date_bounds(
        start_date, end_date, MIN_START, most_recent_date
    )
    start_date = start_dt.date().isoformat()
    end_date = end_dt.date().isoformat()

    logger.info(f"*** Fetch exchange rates for {currency} ***")
    logger.info(f"Start date: {start_date}")
    logger.info(f"End date: {end_date}")
    logger.info(f"Target fiat currencies: {fiat_currencies}")

    if start_dt > end_dt:
        if most_recent_date is not None:
            # The newest rate already in the DB lies beyond the fetch window
            # (the end date defaults to yesterday, the last complete day),
            # e.g. after a manual forward-fill of today's rate. Nothing to
            # fetch is not an error; return an empty frame so ingest is a
            # no-op.
            logger.info(
                f"Rates already available up to {start_date}, nothing to "
                f"fetch until end date {end_date}."
            )
            return pd.DataFrame(
                columns=["date", "USD"] + [f for f in fiat_currencies if f != "USD"]
            )
        logger.error("Error: start date after end date.")
        raise SystemExit(1)

    # fetch cryptocurrency exchange rates in USD
    coingecko_rates = fetch_coingecko_rates(start_date, end_date, currency, api_key)

    ecb_rates = fetch_ecb_rates(fiat_currencies)

    # query conversion rates and merge converted values in exchange rates
    exchange_rates = coingecko_rates
    date_range = pd.date_range(start_dt.date(), end_dt.date())
    date_range = pd.DataFrame(date_range, columns=["date"])
    date_range = date_range["date"].dt.strftime("%Y-%m-%d")

    # CoinGecko gaps: unlike a weekend ECB gap, a missing crypto price is
    # never filled in, and ingest() drops the day via dropna(). A gap at the
    # end of the window (not published yet) heals on the next run, so it is
    # recoverable (15); a gap inside the window stays after the next resume,
    # so it is critical (2). Re-run without --abort-on-gaps to write around it.
    missing_usd = sorted(set(date_range) - set(coingecko_rates.date))
    if abort_on_gaps and missing_usd:
        trailing = list(date_range[-len(missing_usd) :]) == missing_usd
        logger.error(
            f"Error: CoinGecko has no {currency} USD rate for {missing_usd}, "
            "aborting import."
        )
        if not dry_run:
            raise SystemExit(15 if trailing else 2)

    for fiat_currency in set(fiat_currencies) - {"USD"}:
        # Gap-free, forward-filled ECB rate so weekend / not-yet-published
        # days inherit the most recent known FX rate; the anchoring rate can
        # lie before the import window (see forward_filled_fx_rate).
        ecb_rate = forward_filled_fx_rate(ecb_rates, fiat_currency, end_date)
        merged_df = (
            pd.DataFrame({"date": date_range})
            .merge(coingecko_rates, on="date", how="left")
            .merge(ecb_rate, on="date", how="left")
        )

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
            exchange_rates = exchange_rates.drop("USD", axis=1)
        exchange_rates["fiat_values"] = exchange_rates.drop("date", axis=1).to_dict(
            orient="records"
        )
        exchange_rates = exchange_rates.drop(fiat_currencies, axis=1)

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
