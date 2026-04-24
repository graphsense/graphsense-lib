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
from typing import Any, Iterable, Optional

import graphsense
from graphsense.api_client import ApiClient
from graphsense.configuration import Configuration
from graphsense.ext.deprecation import install as install_deprecation_hook

# APIs that expose only deprecated endpoints; hidden from the convenience
# surface but still reachable through raw when GS_SHOW_DEPRECATED=1.
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
    ) -> Bundle:
        ccy = self._currency(currency)
        txs = self.raw.txs
        base = txs.get_tx(ccy, tx_hash)

        jobs: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            if with_io:
                jobs["io"] = pool.submit(txs.get_tx_io, ccy, tx_hash)
            if with_flows:
                jobs["flows"] = pool.submit(txs.list_tx_flows, ccy, tx_hash)
            if with_upstream:
                jobs["upstream"] = pool.submit(txs.get_spending_txs, ccy, tx_hash)
            if with_downstream:
                jobs["downstream"] = pool.submit(txs.get_spent_in_txs, ccy, tx_hash)
            results = {k: f.result() for k, f in jobs.items()}

        return Bundle(
            data=base,
            io=results.get("io"),
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

    def block(self, height: int, currency: Optional[str] = None) -> Any:
        return self.raw.blocks.get_block(self._currency(currency), int(height))

    def exchange_rates(self, height: int, currency: Optional[str] = None) -> Any:
        return self.raw.rates.get_exchange_rates(self._currency(currency), int(height))

    def actor(self, actor_id: str) -> Any:
        return self.raw.tags.get_actor(actor_id)

    def tags_for(self, address: str, currency: Optional[str] = None) -> Any:
        return self.raw.addresses.list_tags_by_address(
            self._currency(currency), address
        )

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
