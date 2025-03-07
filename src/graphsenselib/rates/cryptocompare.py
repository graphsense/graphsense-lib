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

logger = logging.getLogger(__name__)

MIN_START = "2010-07-17T00:00:00.000000+00:00"


def cryptocompare_historical_url(start: str, end: str, symbol: str, fiat: str):
    # https://min-api.cryptocompare.com/documentation?key=Historical&cat=dataHistoday
    toDt = datetime.fromisoformat(end)
    sDt = datetime.fromisoformat(start)
    limit = (toDt - sDt).days + 1
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


def fetch_cryptocompare_rates(start: str, end: str, symbol: str, fiat: str):
    r1 = requests.get(cryptocompare_historical_url(start, end, symbol, fiat))
    rates = pd.DataFrame(json.loads(r1.content)["Data"]["Data"])
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


def fetch(env, currency, fiat_currencies, start_date, end_date):
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

    usd_rates = fetch_cryptocompare_rates(start_date, end_date, currency, "USD")

    ecb_rates = fetch_ecb_rates(fiat_currencies)

    # query conversion rates and merge converted values in exchange rates
    exchange_rates = usd_rates
    date_range = pd.date_range(
        datetime.fromisoformat(start_date), datetime.fromisoformat(end_date)
    )
    date_range = pd.DataFrame(date_range, columns=["date"])
    date_range = date_range["date"].dt.strftime("%Y-%m-%d")

    for fiat_currency in set(fiat_currencies) - {"USD"}:
        ecb_rate = ecb_rates[["date", fiat_currency]].rename(
            columns={fiat_currency: "fx_rate"}
        )
        merged_df = exchange_rates.merge(ecb_rate, on="date", how="left").merge(
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
