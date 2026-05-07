"""The `GraphSense` facade — a small, hand-written convenience class that
removes boilerplate over the generated client and bundles commonly-paired
calls.

Notes
-----
* The generated Address/Cluster/Tx models have `validate_assignment=True` and
  no `extra="allow"`, so we cannot attach auxiliary data to them. Instead,
  `lookup_*` methods return a `Bundle` object that exposes the primary model
  via `.data` plus the auxiliary calls via named attributes (`.tags`,
  `.cluster`, `.tag_summary`, ...).
* `raw` is populated at construction time by introspecting `graphsense.api`.
  New API classes added on regeneration are picked up automatically.
"""

from __future__ import annotations

import inspect
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Union

from dateutil import parser as _dateutil_parser

import graphsense
from graphsense.api_client import ApiClient
from graphsense.configuration import Configuration
from graphsense.ext.deprecation import install as install_deprecation_hook

# APIs that expose only deprecated endpoints; hidden from the convenience
# surface but still reachable through raw when
# GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS=1.
_DEPRECATED_APIS = {"EntitiesApi"}

# Environment variables consulted in this order (first set wins) when no
# explicit api_key / host is passed. Fully-qualified names (GRAPHSENSE_*,
# IKNAIO_*) are preferred since they are less likely to collide with other
# tooling in a shared shell; GS_* is a shorter alias, and bare API_KEY is
# accepted last for compatibility with the generator's own example snippets.
API_KEY_ENV_VARS = (
    "GRAPHSENSE_API_KEY",
    "IKNAIO_API_KEY",
    "GS_API_KEY",
    "API_KEY",
)
HOST_ENV_VARS = (
    "GRAPHSENSE_HOST",
    "IKNAIO_HOST",
    "GS_HOST",
)


def _first_env(names: tuple[str, ...]) -> Optional[str]:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


@dataclass
class Bundle:
    """Result of a `lookup_*` call: the primary model plus auxiliary data."""

    data: Any
    tags: Optional[Any] = None
    cluster: Optional[Any] = None
    tag_summary: Optional[Any] = None
    top_addresses: Optional[Any] = None
    io: Optional[Any] = None
    flows: Optional[Any] = None
    upstream: Optional[Any] = None
    downstream: Optional[Any] = None
    extras: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.data is not None:
            out.update(_model_to_dict(self.data))
        for key in (
            "tags",
            "cluster",
            "tag_summary",
            "top_addresses",
            "io",
            "flows",
            "upstream",
            "downstream",
        ):
            value = getattr(self, key)
            if value is not None:
                out[key] = _model_to_dict(value)
        out.update(self.extras)
        return out


class Raw:
    """Dynamic namespace exposing every non-deprecated `*Api` from
    `graphsense.api`. Instance attributes are lowercase names of the API
    group with the `Api` suffix stripped, e.g. `raw.addresses`, `raw.txs`.
    """

    def __init__(self, api_client: ApiClient, *, show_deprecated: bool = False):
        self._api_client = api_client
        self._groups: dict[str, Any] = {}
        for name in sorted(dir(graphsense)):
            if not name.endswith("Api"):
                continue
            cls = getattr(graphsense, name, None)
            if not inspect.isclass(cls):
                continue
            if name in _DEPRECATED_APIS and not show_deprecated:
                continue
            key = _api_attr_name(name)
            self._groups[key] = cls(api_client)

    def __getattr__(self, item: str) -> Any:
        try:
            return self._groups[item]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(item) from exc

    def __dir__(self) -> list[str]:
        return sorted(set(list(self._groups.keys()) + list(super().__dir__())))

    def groups(self) -> list[str]:
        """Sorted list of available API group keys (e.g. ['addresses', ...])."""
        return sorted(self._groups.keys())


def _api_attr_name(class_name: str) -> str:
    """`AddressesApi` -> `addresses`."""
    assert class_name.endswith("Api")
    return class_name[: -len("Api")].lower()


def _looks_like_date(value: Any) -> bool:
    """True for date / datetime strings the server's parser accepts.

    The server (graphsense REST) parses dates with `dateutil.parser.parse`,
    so we mirror that here: any ISO 8601-ish form is allowed
    (`2024-01-15`, `2024-01-15T12:34:56Z`, `2024-01-15 12:34`, ...). Ints
    and pure-numeric strings are rejected so they can be routed to the
    height-based endpoint instead.
    """
    if not isinstance(value, str):
        return False
    if value.isdigit():  # bare integer string is a height, not a date
        return False
    try:
        _dateutil_parser.parse(value)
    except (_dateutil_parser.ParserError, ValueError, OverflowError):
        return False
    return True


def _coerce_height(value: Any) -> int:
    """Convert `value` to an int height with a clear error if malformed.

    Used by `block` / `exchange_rates` after the date-shape check has already
    declined the value; raising a typed `ValueError` here keeps SDK callers
    out of the cryptic `int('2020-10-15:9:00')` traceback.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"expected a block height (int) or YYYY-MM-DD date, got {value!r}"
        ) from exc


def _model_to_dict(value: Any) -> Any:
    """Best-effort conversion of a generated model (or list of models) to a
    plain JSON-serializable dict."""
    if value is None:
        return None
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, list):
        return [_model_to_dict(v) for v in value]
    return value


class GraphSense:
    """High-level facade: one object, common lookups, full raw access."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        host: Optional[str] = None,
        currency: Optional[str] = None,
        *,
        api_client: Optional[ApiClient] = None,
        quiet_deprecation: bool = False,
        show_deprecated: bool = False,
        max_workers: int = 8,
        deprecation_stream: Any = None,
    ):
        self.currency = currency
        self.max_workers = max_workers

        if api_client is None:
            cfg_kwargs: dict[str, Any] = {}
            resolved_host = host or _first_env(HOST_ENV_VARS)
            if resolved_host:
                cfg_kwargs["host"] = resolved_host
            resolved_key = api_key or _first_env(API_KEY_ENV_VARS)
            if resolved_key:
                cfg_kwargs["api_key"] = {"api_key": resolved_key}
            configuration = (
                Configuration(**cfg_kwargs) if cfg_kwargs else Configuration()
            )
            api_client = ApiClient(configuration)
        self.api_client = api_client

        install_deprecation_hook(
            self.api_client,
            quiet=quiet_deprecation,
            stream=deprecation_stream,
        )

        self.raw = Raw(self.api_client, show_deprecated=show_deprecated)

    # ------------------------------------------------------------------ utils
    def close(self) -> None:
        """Close the underlying api client (and its pool)."""
        close = getattr(self.api_client, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "GraphSense":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _currency(self, currency: Optional[str]) -> str:
        resolved = currency or self.currency
        if not resolved:
            raise ValueError(
                "currency is required (pass `currency=` to GraphSense or to the call)"
            )
        return resolved

    # ------------------------------------------------------------- lookups ---
    def lookup_address(
        self,
        address: str,
        currency: Optional[str] = None,
        *,
        with_tags: bool = False,
        with_cluster: bool = False,
        with_tag_summary: bool = False,
        include_actors: bool = True,
    ) -> Bundle:
        """Fetch an address plus the usual auxiliary data in parallel.

        Non-deprecated paths only — the cluster bundle uses
        ClustersApi.get_cluster via the address's `cluster` id field,
        not the deprecated get_address_entity.
        """
        ccy = self._currency(currency)
        addresses = self.raw.addresses
        clusters = self.raw.clusters

        base = addresses.get_address(ccy, address, include_actors=include_actors)

        jobs: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            if with_tags:
                jobs["tags"] = pool.submit(addresses.list_tags_by_address, ccy, address)
            if with_tag_summary:
                jobs["tag_summary"] = pool.submit(
                    addresses.get_tag_summary_by_address, ccy, address
                )
            if with_cluster:
                cluster_id = getattr(base, "cluster", None)
                if cluster_id is not None:
                    jobs["cluster"] = pool.submit(
                        clusters.get_cluster, ccy, int(cluster_id)
                    )
            results = {k: f.result() for k, f in jobs.items()}

        return Bundle(
            data=base,
            tags=results.get("tags"),
            cluster=results.get("cluster"),
            tag_summary=results.get("tag_summary"),
        )

    def lookup_cluster(
        self,
        cluster_id: int | str,
        currency: Optional[str] = None,
        *,
        with_tag_summary: bool = False,
        with_top_addresses: bool = False,
    ) -> Bundle:
        ccy = self._currency(currency)
        clusters = self.raw.clusters
        cid = int(cluster_id)
        base = clusters.get_cluster(ccy, cid)

        jobs: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            if with_tag_summary and hasattr(clusters, "get_tag_summary_by_cluster"):
                jobs["tag_summary"] = pool.submit(
                    clusters.get_tag_summary_by_cluster, ccy, cid
                )
            if with_top_addresses:
                jobs["top_addresses"] = pool.submit(
                    clusters.list_cluster_addresses, ccy, cid
                )
            results = {k: f.result() for k, f in jobs.items()}

        return Bundle(
            data=base,
            tag_summary=results.get("tag_summary"),
            top_addresses=results.get("top_addresses"),
        )

    def lookup_tx(
        self,
        tx_hash: str,
        currency: Optional[str] = None,
        *,
        with_io: bool = False,
        with_flows: bool = False,
        with_upstream: bool = False,
        with_downstream: bool = False,
        with_heuristics: bool = False,
    ) -> Bundle:
        """Fetch a transaction plus optional auxiliary data.

        Some flags only apply to one of the two transaction models:

        - `with_io`, `with_upstream`, `with_downstream`, `with_heuristics`
          are UTXO-only (btc, ltc, ...). For account-model chains (eth,
          trx, ...) they are silently skipped.
        - `with_flows` is account-only and is silently skipped for UTXO
          chains.
        """
        ccy = self._currency(currency)
        txs = self.raw.txs

        # Heuristics are computed by `get_tx` itself when `include_heuristics`
        # is provided. The endpoint accepts the literal `"all"` to mean
        # every available heuristic.
        get_tx_kwargs: dict[str, Any] = {}
        if with_heuristics:
            get_tx_kwargs["include_heuristics"] = ["all"]
        base = txs.get_tx(ccy, tx_hash, **get_tx_kwargs)

        is_utxo = getattr(base, "tx_type", None) == "utxo"

        jobs: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            if with_io and is_utxo:
                # `get_tx_io` is direction-keyed; fetch both halves in parallel.
                jobs["io_inputs"] = pool.submit(txs.get_tx_io, ccy, tx_hash, "inputs")
                jobs["io_outputs"] = pool.submit(txs.get_tx_io, ccy, tx_hash, "outputs")
            if with_flows and not is_utxo:
                # `list_tx_flows` is account-model only.
                jobs["flows"] = pool.submit(txs.list_tx_flows, ccy, tx_hash)
            if with_upstream and is_utxo:
                jobs["upstream"] = pool.submit(txs.get_spending_txs, ccy, tx_hash)
            if with_downstream and is_utxo:
                jobs["downstream"] = pool.submit(txs.get_spent_in_txs, ccy, tx_hash)
            results = {k: f.result() for k, f in jobs.items()}

        io_payload: Optional[dict[str, Any]] = None
        if with_io and is_utxo:
            io_payload = {
                "inputs": _model_to_dict(results.get("io_inputs")),
                "outputs": _model_to_dict(results.get("io_outputs")),
            }

        return Bundle(
            data=base,
            io=io_payload,
            flows=results.get("flows"),
            upstream=results.get("upstream"),
            downstream=results.get("downstream"),
        )

    # ---------------------------------------------------------- simple passthru
    def search(self, query: str, currency: Optional[str] = None) -> Any:
        """Thin passthrough that lets the user omit the currency argument."""
        ccy = currency or self.currency
        if ccy is None:
            return self.raw.general.search(query)
        return self.raw.general.search(query, currency=ccy)

    def statistics(self) -> Any:
        return self.raw.general.get_statistics()

    def block(
        self,
        height_or_date: Union[int, str],
        currency: Optional[str] = None,
    ) -> Any:
        """Look up a block by integer height or by `YYYY-MM-DD` date.

        Dates dispatch to `BlocksApi.get_block_by_date`, which returns the
        closest block at or before that date.
        """
        ccy = self._currency(currency)
        if _looks_like_date(height_or_date):
            return self.raw.blocks.get_block_by_date(ccy, str(height_or_date))
        return self.raw.blocks.get_block(ccy, _coerce_height(height_or_date))

    def exchange_rates(
        self,
        height_or_date: Union[int, str],
        currency: Optional[str] = None,
    ) -> Any:
        """Fetch exchange rates at a height or at a date.

        The REST endpoint only accepts a height; for dates we first resolve
        the closest block via `BlocksApi.get_block_by_date` and use its
        `before_block` height.
        """
        ccy = self._currency(currency)
        if _looks_like_date(height_or_date):
            block_at_date = self.raw.blocks.get_block_by_date(ccy, str(height_or_date))
            height = getattr(block_at_date, "before_block", None) or getattr(
                block_at_date, "after_block", None
            )
            if height is None:
                raise ValueError(
                    f"no block found for {currency}/{height_or_date}; "
                    "cannot resolve exchange rates"
                )
            return self.raw.rates.get_exchange_rates(ccy, int(height))
        return self.raw.rates.get_exchange_rates(ccy, _coerce_height(height_or_date))

    def actor(self, actor_id: str) -> Any:
        return self.raw.tags.get_actor(actor_id)

    def tags_for(
        self,
        address: str,
        currency: Optional[str] = None,
        *,
        include_best_cluster_tag: bool = True,
        limit: Optional[int] = None,
        page_size: Optional[int] = 100,
    ) -> dict:
        """Iterate `list_tags_by_address` pages, returning aggregated tags.

        Walks the `next_page` token transparently. By default fetches in
        small pages (100) and keeps going until the server reports no more
        pages or `limit` items have been collected. Pass `limit=N` to cap;
        `page_size=None` defers to the server default.

        Returns a plain dict with `address_tags` (list) and `next_page`
        (the resumption token if iteration was stopped early by `limit`,
        otherwise `None`).
        """
        ccy = self._currency(currency)
        addresses = self.raw.addresses
        collected: list[Any] = []
        page_token: Optional[str] = None
        next_page_out: Optional[str] = None

        while True:
            kwargs: dict[str, Any] = {}
            if page_token is not None:
                kwargs["page"] = page_token
            if page_size is not None:
                kwargs["pagesize"] = int(page_size)
            kwargs["include_best_cluster_tag"] = include_best_cluster_tag

            page = addresses.list_tags_by_address(ccy, address, **kwargs)
            page_tags = list(getattr(page, "address_tags", []) or [])
            collected.extend(page_tags)
            page_token = getattr(page, "next_page", None)

            if limit is not None and len(collected) >= limit:
                # Truncate and surface the next-page token so callers can resume.
                if len(collected) > limit:
                    collected = collected[:limit]
                next_page_out = page_token
                break
            if not page_token:
                break

        return {
            "address_tags": [_model_to_dict(t) for t in collected],
            "next_page": next_page_out,
        }

    # ------------------------------------------------------------------- bulk
    def bulk(
        self,
        operation: str,
        keys: Iterable[Any],
        currency: Optional[str] = None,
        *,
        format: str = "json",
        num_pages: int = 1,
        key_field: str = "address",
    ) -> Any:
        """Call the /bulk.<format>/<operation> endpoint with `keys`.

        `key_field` is the dict key that the bulk operation expects in the
        request body (e.g. "address", "tx_hash", "cluster"). Default is
        "address" which is by far the most common.
        """
        ccy = self._currency(currency)
        body = {key_field: list(keys)}
        if format == "csv":
            return self.raw.bulk.bulk_csv(ccy, operation, num_pages, body)
        return self.raw.bulk.bulk_json(ccy, operation, num_pages, body)
