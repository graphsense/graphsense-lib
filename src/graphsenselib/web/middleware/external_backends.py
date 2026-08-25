"""Serve selected networks from an external chain-data backend.

Some networks have no Cassandra transformed keyspace but are available from a
GraphSense-API-compatible external backend (e.g. the iknaio external backend
adapter, which answers the GraphSense wire contract from a node-provider API).
This middleware makes such networks part of this deployment: requests for a
configured network are reverse-proxied to its backend, while everything the
core owns stays local.

Decision rules:
1. ``/{network}/...`` paths of a configured network proxy to its backend —
   EXCEPT address tag routes (``.../addresses/{addr}/tags`` and
   ``.../tag_summary``) and their bulk twins
   (``.../bulk.{csv,json}/{list_tags_by_address,get_tag_summary_by_address}``):
   tags are keyed by real chain addresses and live in this deployment's
   TagStore, so they are answered locally. Entity/cluster routes DO proxy: an
   external backend mints its own entity ids, which mean nothing in the local
   id space (and vice versa), so both sides of that id family must stay with
   the backend that minted them.
2. ``/stats`` is answered locally, then the per-currency entries of each
   backend (filtered to its configured networks) are merged into the
   ``currencies`` list. Local scalars (version, request_timestamp) win — they
   describe THIS deployment. A backend entry that declares a ``capabilities``
   list (absent = full core; present = lite, limited to exactly the named
   features) gets ``"tags"`` added: the backend cannot know a TagStore fronts
   it, but rule 1 makes tag routes work here, so THIS deployment truthfully
   supports tags for those networks.
3. ``/search`` without a currency filter is answered locally, then each
   backend's per-currency address/tx hits (filtered to its configured
   networks) are merged into the ``currencies`` list; labels and actors are
   TagStore data and stay local. With a currency filter naming a configured
   network, the whole request proxies to that backend; any other currency
   filter skips the backends entirely.

Backend transport errors propagate — a broken backend must be loud (500 via
the generic exception handler), not silently shaped as an empty answer.

The middleware must sit INSIDE the CORS middleware (added before it in
``create_app``) so short-circuited proxy responses still receive CORS
headers. Authentication is not a concern here: API keys are validated by the
gateway in front, never by this app (see ``security.get_api_key``).
"""

import json
import re
from typing import Dict, Optional

import httpx
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

from graphsenselib.web.config import ExternalBackendConfig, ExternalBackendsConfig

SERVED_BY_HEADER = "x-served-by"
SERVED_BY_VALUE = "external-backend"

# address tag routes stay local (rule 1): TagStore data keyed by real chain
# addresses, owned by this deployment regardless of who serves the chain data
_LOCAL_TAG_PATHS = (
    re.compile(r"^/[^/]+/addresses/[^/]+/(tags|tag_summary)$"),
    re.compile(
        r"^/[^/]+/bulk\.(csv|json)/(list_tags_by_address|get_tag_summary_by_address)$"
    ),
)

# the capability word rule 1 adds to a declaring backend's stats entry (rule 2)
_TAGS_CAPABILITY = "tags"


class ExternalBackendMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        config: ExternalBackendsConfig,
        client: Optional[httpx.AsyncClient] = None,
    ):
        super().__init__(app)
        self.config = config
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout_s)
        )

    async def dispatch(self, request: Request, call_next) -> Response:
        if not (self.config.enabled and self.config.networks):
            return await call_next(request)
        short_circuit = await self._route(request)
        if short_circuit is not None:
            return short_circuit
        if not self._wants_merge(request):
            return await call_next(request)
        response = await call_next(request)
        body = b"".join([chunk async for chunk in response.body_iterator])
        merged = await self._merge_response(request, response.status_code, body)
        if merged is not None:
            return merged
        return Response(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

    async def _route(self, request: Request) -> Optional[Response]:
        """Return a Response to short-circuit with, or None to serve locally.

        The merge endpoints are handled in ``_merge_response`` instead — they
        need the local response first.
        """
        path = request.url.path
        first = path.strip("/").split("/", 1)[0]
        backend = self.config.networks.get(first)
        if backend is not None and not any(
            pattern.match(path) for pattern in _LOCAL_TAG_PATHS
        ):
            return await self._proxy(request, backend)
        if path == "/search" and request.method == "GET":
            currency = request.query_params.get("currency")
            if currency is not None:
                backend = self.config.networks.get(currency.lower())
                if backend is not None:
                    return await self._proxy(request, backend)
        return None

    def _wants_merge(self, request: Request) -> bool:
        """True when the local response must be merged with backend answers."""
        if request.method != "GET":
            return False
        if request.url.path == "/stats":
            return True
        return (
            request.url.path == "/search"
            and request.query_params.get("currency") is None
        )

    async def _merge_response(
        self, request: Request, status_code: int, body: bytes
    ) -> Optional[Response]:
        """Merge backend answers into a buffered local /stats or /search body.

        Returns the merged response, or None when the local response should
        pass through unchanged (non-200 locally, nothing to merge).
        """
        if status_code != 200:
            return None
        local_doc = json.loads(body)
        merged = dict(local_doc)
        merged["currencies"] = list(local_doc.get("currencies", []))
        key = "name" if request.url.path == "/stats" else "currency"
        query = ("?" + str(request.url.query)) if request.url.query else ""
        for base_url, networks in self._backends_by_url().items():
            backend_doc = await self._fetch_json(
                base_url, request.url.path + query, networks
            )
            entries = [
                entry
                for entry in backend_doc.get("currencies", [])
                if entry.get(key) in networks
            ]
            if request.url.path == "/stats":
                entries = [_with_tags_capability(entry) for entry in entries]
            merged["currencies"] = _merge_keyed_lists(
                merged["currencies"], entries, key
            )
        return JSONResponse(merged, status_code=200)

    def _backends_by_url(self) -> Dict[str, set]:
        """Group configured networks by backend URL — one call per backend."""
        grouped: Dict[str, set] = {}
        for network, backend in self.config.networks.items():
            grouped.setdefault(backend.url, set()).add(network)
        return grouped

    def _api_key_for_url(self, base_url: str) -> Optional[str]:
        for backend in self.config.networks.values():
            if backend.url == base_url and backend.api_key:
                return backend.api_key
        return None

    async def _fetch_json(self, base_url: str, path_and_query: str, networks) -> dict:
        headers = {"Accept": "application/json"}
        api_key = self._api_key_for_url(base_url)
        if api_key:
            headers["Authorization"] = api_key
        response = await self.client.get(
            base_url.rstrip("/") + path_and_query, headers=headers
        )
        response.raise_for_status()
        return response.json()

    async def _proxy(
        self, request: Request, backend: ExternalBackendConfig
    ) -> Response:
        url = backend.url.rstrip("/") + request.url.path
        if request.url.query:
            url += "?" + str(request.url.query)
        headers = {"Accept": request.headers.get("accept", "application/json")}
        if backend.api_key:
            headers["Authorization"] = backend.api_key
        body = await request.body() if request.method not in ("GET", "HEAD") else None
        if body:
            headers["Content-Type"] = request.headers.get(
                "content-type", "application/json"
            )
        backend_response = await self.client.request(
            request.method, url, headers=headers, content=body
        )
        return Response(
            content=backend_response.content,
            status_code=backend_response.status_code,
            media_type=backend_response.headers.get("content-type"),
            headers={SERVED_BY_HEADER: SERVED_BY_VALUE},
        )


def _with_tags_capability(entry: dict) -> dict:
    """Add ``"tags"`` to a stats entry's declared capabilities (rule 2).

    An entry WITHOUT a ``capabilities`` field declares a full core network and
    is passed through untouched — adding a list there would demote it to lite.
    """
    capabilities = entry.get("capabilities")
    if not isinstance(capabilities, list) or _TAGS_CAPABILITY in capabilities:
        return entry
    return {**entry, "capabilities": capabilities + [_TAGS_CAPABILITY]}


def _merge_keyed_lists(base_entries: list, extra_entries: list, key: str) -> list:
    """Merge per-currency entry lists: base order is kept, an extra entry
    REPLACES a base entry with the same key (the backend is authoritative for
    the networks it serves), and remaining extras are appended."""
    extra_by_key = {entry[key]: entry for entry in extra_entries}
    merged = [extra_by_key.pop(entry[key], entry) for entry in base_entries]
    merged.extend(extra_by_key.values())
    return merged
