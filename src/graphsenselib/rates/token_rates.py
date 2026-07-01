# -*- coding: utf-8 -*-
"""Fetch per-token exchange rates for unpegged tokens.

Mirrors the native exchange-rate fetch pipeline but adds a token dimension:
for every unpegged token in ``token_configuration`` (no USD/EUR/ETH peg) it
fetches a daily USD price from the configured provider, derives the other fiat
currencies via ECB FX rates, and writes rows into the raw ``token_exchange_rates``
table (``token, date -> fiat_values``). The delta-update later maps these daily
rates to per-block rows the API serves.

Asset identification prefers the token's contract address (unambiguous) and
falls back to its ticker symbol. Tokens no provider can resolve simply yield no
rows -> the serving layer falls back to zero fiat. Individual token failures are
logged and never abort the run.
"""

import json
import logging
from datetime import date
from typing import List, Optional, Tuple

import pandas as pd
import requests

from graphsenselib.db import DbFactory
from graphsenselib.db.analytics import DATE_FORMAT
from graphsenselib.rates.coingecko import fetch_ecb_rates
from graphsenselib.rates.coinmarketcap import fetch_cmc_rates
from graphsenselib.rates.cryptocompare import fetch_cryptocompare_rates
from graphsenselib.rates.utils import (
    as_utc_datetime,
    forward_filled_fx_rate,
    normalize_date_bounds,
)

logger = logging.getLogger(__name__)

MIN_START = "2015-01-01T00:00:00.000000+00:00"

# CoinGecko asset-platform ids keyed by graphsense network (for contract lookup)
COINGECKO_PLATFORMS = {"eth": "ethereum", "trx": "tron"}


def _is_unpegged(peg) -> bool:
    return (
        peg is None
        or (isinstance(peg, float) and pd.isna(peg))
        or (isinstance(peg, str) and peg.strip() == "")
    )


def select_unpegged_tokens(token_config_df) -> List[dict]:
    """Return [{ticker, contract}] for tokens with no fiat/coin peg."""
    if token_config_df is None or len(token_config_df) == 0:
        return []
    tokens = []
    for _, row in token_config_df.iterrows():
        if _is_unpegged(row.get("peg_currency")):
            tokens.append(
                {
                    "ticker": row["currency_ticker"],
                    "contract": row.get("token_address"),
                }
            )
    return tokens


# --- provider-specific USD fetchers (contract-preferred, ticker fallback) ---


def _coingecko_market_chart_url(platform: str, contract: str, start, end) -> str:
    frm = int(as_utc_datetime(start).timestamp())
    to = int(as_utc_datetime(end).timestamp())
    return (
        "https://pro-api.coingecko.com/api/v3/coins/"
        f"{platform}/contract/{contract}/market_chart/range"
        f"?vs_currency=usd&from={frm}&to={to}"
    )


def _fetch_coingecko_contract_usd(
    platform, contract, start, end, api_key
) -> pd.DataFrame:
    headers = {
        "x-cg-pro-api-key": api_key,
        "accept": "application/json",
    }
    url = _coingecko_market_chart_url(platform, contract, start, end)
    logger.info(f"Fetching token rates (coingecko contract) from {url}")
    rsession = requests.Session()
    rsession.mount("https://", requests.adapters.HTTPAdapter(max_retries=5))
    response = rsession.get(url, headers=headers)
    prices = json.loads(response.content).get("prices", [])
    # prices: [[timestamp_ms, price_usd], ...]; collapse to one price per day
    per_day = {}
    for ts_ms, price in prices:
        d = date.fromtimestamp(ts_ms // 1000).strftime(DATE_FORMAT)
        per_day[d] = price  # keep last observation of each day
    return pd.DataFrame(sorted(per_day.items()), columns=["date", "USD"])


def _fetch_token_usd(
    provider: str, network: str, token: dict, start: str, end: str, api_key: str
) -> Optional[Tuple[str, pd.DataFrame]]:
    """Fetch a token's daily USD price, preferring contract then ticker.

    Returns (identifier_kind, dataframe[date, USD]) or None if unresolved.
    """
    ticker = token["ticker"]
    contract = token.get("contract")

    # Contract-preferred where the provider supports it.
    if provider == "coingecko":
        platform = COINGECKO_PLATFORMS.get(network)
        if platform and contract:
            try:
                df = _fetch_coingecko_contract_usd(
                    platform, contract, start, end, api_key
                )
                if df is not None and len(df) > 0:
                    return "contract", df
            except Exception as e:
                logger.warning(
                    f"coingecko contract lookup failed for {ticker} ({contract}): {e}"
                )
        # No reliable ticker->coin-id mapping for arbitrary tokens.
        return None

    # cryptocompare / coinmarketcap: ticker-based (contract not supported).
    try:
        if provider == "cryptocompare":
            df = fetch_cryptocompare_rates(start, end, ticker, "USD", api_key)
        elif provider == "coinmarketcap":
            df = fetch_cmc_rates(start, end, ticker, api_key)
        else:
            logger.warning(f"provider {provider} does not support token rates")
            return None
        if df is not None and len(df) > 0:
            return "ticker", df
    except (SystemExit, Exception) as e:  # never let one token abort the run
        logger.warning(f"{provider} ticker lookup failed for {ticker}: {e}")
    return None


def _derive_fiat_values(
    usd_rates: pd.DataFrame,
    fiat_currencies: List[str],
    ecb_rates: pd.DataFrame,
    start_dt,
    end_dt,
) -> pd.DataFrame:
    """Derive non-USD fiat columns from USD prices via ECB FX (mirrors the
    native fetch_impl merge). Returns df with [date, <fiat>...] columns."""
    exchange_rates = usd_rates
    date_range = pd.date_range(start_dt, end_dt)
    date_range = pd.DataFrame(date_range, columns=["date"])["date"].dt.strftime(
        "%Y-%m-%d"
    )
    for fiat_currency in set(fiat_currencies) - {"USD"}:
        ecb_rate = forward_filled_fx_rate(ecb_rates, fiat_currency, end_dt.isoformat())
        merged_df = (
            pd.DataFrame({"date": date_range})
            .merge(usd_rates, on="date", how="left")
            .merge(ecb_rate, on="date", how="left")
        )
        merged_df[fiat_currency] = merged_df["USD"] * merged_df["fx_rate"]
        merged_df = merged_df[["date", fiat_currency]]
        exchange_rates = exchange_rates.merge(merged_df, on="date")
    return exchange_rates


def _to_rows(
    token: str, rates_df: pd.DataFrame, fiat_currencies: List[str]
) -> List[dict]:
    """Collapse a [date, <fiat>...] frame into token_exchange_rates rows."""
    rates_df = rates_df.dropna()
    if "USD" not in fiat_currencies and "USD" in rates_df.columns:
        rates_df = rates_df.drop("USD", axis=1)
    value_cols = [c for c in rates_df.columns if c != "date"]
    rows = []
    for _, r in rates_df.iterrows():
        rows.append(
            {
                "asset": token,
                "date": r["date"],
                "fiat_values": {c: float(r[c]) for c in value_cols},
            }
        )
    return rows


def ingest_token_rates(
    env: str,
    currency: str,
    provider: str,
    fiat_currencies: List[str],
    start_date: Optional[str],
    end_date: Optional[str],
    force: bool,
    dry_run: bool,
    api_key: str,
    table: str = "token_exchange_rates",
) -> None:
    """Fetch and store daily rates for every unpegged token of `currency`."""
    with DbFactory().from_config(env, currency) as db:
        tokens = select_unpegged_tokens(db.transformed.get_token_configuration())
        if not tokens:
            logger.info(f"No unpegged tokens configured for {currency}; skipping.")
            return

        logger.info(
            f"Fetching token rates for {len(tokens)} unpegged {currency} tokens "
            f"via {provider}"
        )
        # ECB FX rates are shared across all tokens; fetch once.
        ecb_rates = fetch_ecb_rates(fiat_currencies)

        resolved, missed = [], []
        for token in tokens:
            ticker = token["ticker"]
            most_recent = None
            if not force:
                most_recent = db.raw.get_last_token_exchange_rate_date(ticker)
            start_dt, end_dt = normalize_date_bounds(
                start_date, end_date, MIN_START, most_recent
            )
            if start_dt > end_dt:
                continue

            fetched = _fetch_token_usd(
                provider,
                currency,
                token,
                start_dt.isoformat(),
                end_dt.isoformat(),
                api_key,
            )
            if fetched is None:
                missed.append(ticker)
                continue
            kind, usd_rates = fetched
            try:
                rates_df = _derive_fiat_values(
                    usd_rates, fiat_currencies, ecb_rates, start_dt, end_dt
                )
                rows = _to_rows(ticker, rates_df, fiat_currencies)
            except Exception as e:
                logger.warning(f"failed to derive fiat values for {ticker}: {e}")
                missed.append(ticker)
                continue

            resolved.append((ticker, kind, len(rows)))
            if not dry_run and rows:
                db.raw.ingest(table, rows)

        for ticker, kind, n in resolved:
            logger.info(f"  {ticker}: {n} days via {kind}")
        if missed:
            logger.warning(
                f"Could not resolve token rates for {len(missed)} tokens: "
                f"{', '.join(missed)}"
            )
        if dry_run:
            logger.info("Dry run: nothing written.")
