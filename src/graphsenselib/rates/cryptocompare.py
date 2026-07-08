# -*- coding: utf-8 -*-
import json
import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests

from graphsenselib.db import DbFactory
from graphsenselib.db.analytics import DATE_FORMAT
from graphsenselib.rates.coingecko import fetch_ecb_rates
from graphsenselib.rates.utils import (
    as_utc_datetime,
    forward_filled_fx_rate,
    normalize_date_bounds,
)

logger = logging.getLogger(__name__)

MIN_START = "2010-07-17T00:00:00.000000+00:00"


def cryptocompare_historical_url(
    start: str | datetime, end: str | datetime, symbol: str, fiat: str
):
    # https://min-api.cryptocompare.com/documentation?key=Historical&cat=dataHistoday
    toDt = as_utc_datetime(end)
    sDt = as_utc_datetime(start)
    limit = (toDt.date() - sDt.date()).days + 1
    toTS = toDt.timestamp()
    # Docs of cryptocompre say max record limit is 2k so if more just load
    # the entire dataset
    if limit < 2000:
        return (
            "https://min-api.cryptocompare.com/data/v2/histoday"
            f"?fsym={symbol}&tsym={fiat}&toTS={toTS}&limit={limit}"
        )
    else:
        return (
            "https://min-api.cryptocompare.com/data/v2/histoday"
            f"?fsym={symbol}&tsym={fiat}&allData=true"
        )


def fetch_cryptocompare_rates(
    start: str, end: str, symbol: str, fiat: str, api_key: str
):
    headers = {"Authorization": f"Apikey {api_key}"} if api_key else {}
    r1 = requests.get(
        cryptocompare_historical_url(start, end, symbol, fiat), headers=headers
    )
    body = json.loads(r1.content)
    data = body.get("Data")
    rows = data.get("Data") if isinstance(data, dict) else None
    if not rows:
        msg = (
            body.get("Err", {}).get("message")
            or body.get("Message")
            or f"unexpected response {r1.content[:200]!r}"
        )
        logger.error(f"CryptoCompare API request failed: {msg}")
        if "api key" in str(msg).lower():
            logger.error(
                "Anonymous access to min-api.cryptocompare.com was switched "
                "off in June 2026. Get a key at https://developers.coindesk.com/ "
                "and set cryptocompare_api_key in graphsense.yaml."
            )
        raise SystemExit(1)
    rates = pd.DataFrame(rows)
    rates["date"] = pd.to_datetime(rates["time"], unit="s").dt.floor("D")

    # assert rates.date[0] == pd.Timestamp(MIN_START)
    assert len(rates.date) == len(set(rates.date))
    rates["date_check"] = rates.date.diff()
    diffs = rates.date_check.value_counts()
    assert len(diffs) == 1
    assert diffs.keys().unique()[0] == pd.Timedelta("1 days")

    rates.date = rates.date.dt.strftime(DATE_FORMAT)
    rates.rename(columns={"close": fiat}, inplace=True)

    return rates[["date", fiat]]


def fetch(env, currency, fiat_currencies, start_date, end_date, api_key: str):
    with DbFactory().from_config(env, currency) as db:
        return fetch_impl(
            db,
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
    if not force and db:
        logger.info(f"Get last imported rate from {db.raw.get_keyspace()}")
        most_recent_date = db.raw.get_last_exchange_rate_date(table=table)

    start_dt, end_dt = normalize_date_bounds(
        start_date, end_date, MIN_START, most_recent_date
    )

    start_date = start_dt.isoformat()
    end_date = end_dt.isoformat()

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

    usd_rates = fetch_cryptocompare_rates(
        start_date, end_date, currency, "USD", api_key
    )

    ecb_rates = fetch_ecb_rates(fiat_currencies)

    # query conversion rates and merge converted values in exchange rates
    exchange_rates = usd_rates
    date_range = pd.date_range(start_dt, end_dt)
    date_range = pd.DataFrame(date_range, columns=["date"])
    date_range = date_range["date"].dt.strftime("%Y-%m-%d")

    for fiat_currency in set(fiat_currencies) - {"USD"}:
        # Gap-free, forward-filled ECB rate so weekend / not-yet-published
        # days inherit the most recent known FX rate; the anchoring rate can
        # lie before the import window (see forward_filled_fx_rate).
        ecb_rate = forward_filled_fx_rate(ecb_rates, fiat_currency, end_date)
        merged_df = (
            pd.DataFrame({"date": date_range})
            .merge(usd_rates, on="date", how="left")
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
            currency,
            fiat_currencies,
            start_date,
            end_date,
            table,
            force,
            dry_run,
            abort_on_gaps,
            api_key,
        )

        if exchange_rates.isna().values.any():
            logger.warning("exchange_rates contain NaNs, dropping them now")
            exchange_rates.dropna(inplace=True)

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
