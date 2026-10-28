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

   A proxied ``/txs/{tx}/conversions`` answer can name a leg on eth (a row
   whose ``from_network`` or ``to_network`` is eth): its
   ``from/to_asset_transfer`` is an eth tx. Cassandra lags the chain tip, so
   for a fresh transfer that tx may not be indexed yet; the dashboard then
   fails to load the leg and the bridge goes missing from the graph. Such an
   answer is a 501 until Cassandra has every eth tx it names: the whole
   answer is refused rather than served in part. It costs one Cassandra
   lookup per eth tx named, and only for such answers.
2. ``/stats`` is answered locally, then the per-currency entries of each
   backend (filtered to its configured networks) are merged into the
   ``currencies`` list. Local scalars (version, request_timestamp) win — they
   describe THIS deployment. A legacy ``capabilities`` field on a backend
   entry is stripped: capability declaration moved to ``/capabilities``, and
   mirroring an unmodeled leftover would resurrect the old contract.
   ``no_labels`` and ``no_tagged_addresses`` are overlaid from this
   deployment's TagStore (the backend serves placeholder zeros — tag data
   lives here, exactly like rule 1's tag routes; the chain-pipeline numbers
   stay as the backend sent them).
3. ``/search`` without a currency filter is answered locally, then each
   backend's per-currency address/tx hits (filtered to its configured
   networks) are merged into the ``currencies`` list; labels and actors are
   TagStore data and stay local. With a currency filter naming a configured
   network, the whole request proxies to that backend; any other currency
   filter skips the backends entirely.
4. ``/capabilities`` is answered locally, then each backend's per-network
   entries (filtered to its configured networks) are merged into the
   ``networks`` list. ``"tags"`` is removed from a backend entry's
   ``disabled`` list: the backend cannot know a TagStore fronts it, but
   rule 1 makes tag routes work here, so THIS deployment truthfully serves
   tags for those networks. A backend that 404s the endpoint (older adapter)
   contributes no entries — consumers already treat an absent network as
   fully enabled, the same rule they apply to an old server without the
   endpoint, so deployment skew degrades uniformly.
5. ``/{network}/addresses/{addr}/related_addresses`` (pubkey, the only
   relation type the backends know) lists the twins of BOTH sides, whichever
   network it is asked on. The local pubkey table knows btc/trx/ltc/bch/zec
   and eth, a backend knows only the EVM networks it serves, and a key has
   one EVM-format address on all of them:

   - On a LOCALLY served network the answer is local, then each backend is
     asked about the key's EVM-format address — the address itself on eth,
     otherwise the ``eth`` row of the local answer — and its rows for the
     backend's configured networks are appended (an eth address gains its
     bnb/arb twins, a btc or trx address the same ones through its eth
     twin; the local rows stay first). A key with no ``eth`` row, i.e. not
     used on Ethereum, asks no backend: most btc keys never are, and the
     twin probe costs the backend provider calls on every network it
     serves. A backend that does not know the address (404) or declines the
     question (501) contributes nothing.
   - On an EXTERNALLY served network the backend answers (rule 1), and the
     local pubkey rows of the same address, asked as eth, are appended to
     its last page (a bnb address gains its btc/trx twins). That local
     lookup is an addition to an answer the backend already gave, so its
     failure only leaves those rows out and is logged.

   ``merge_related_addresses: false`` switches this rule off, so each side
   answers exactly as without this feature.
6. ``GET /{network}/txs/{tx}/conversions`` of a network in
   ``delegated_conversions`` (e.g. eth) is answered locally, and its backend
   may pair the bridges graphsense-lib finds only half of (Circle CCTP,
   defi/bridging/delegated.py): it reads one leg in the tx's logs but cannot
   find the other chain's. This middleware offers the pairing on
   ``request.state.pair_bridges``; the conversions route calls it only for a
   tx with such a half bridge, and puts the backend's rows in place of the
   half bridges they complete (web/service/txs_service.py). The answer passes
   through untouched. The backend is asked for the same path, sub-tx suffix
   included, with ``only_bridges=true``, and only its ``bridge_tx`` rows that
   name the tx are taken: the local detection answers swaps. A caller entitled
   to no gated currency is offered no pairing (behind the gateway that is how
   the client opt-out arrives), and rows naming a gated currency the caller
   lacks are left out. Without the backend's pairing the half bridge is the
   answer: a status saying that it cannot answer about the tx (400, 404, 422,
   501) is logged at INFO, any other failure is handled as on a merge (below).

Client opt-out: a request carrying ``X-Ikn-Currency-Opt-Out: all-light`` is served
exactly as without this feature — no proxying, no merging, not one call to a
backend. The dashboard sends it on every request while its "lite networks"
switch is off (user decision 2026-09-17), so a congested or unavailable
backend never delays or breaks work on the core networks; the externally
served networks then simply do not appear in ``/stats``, ``/search``,
``/capabilities`` or a twin list, and a direct ``/{network}/…`` request on one
of them gets the core's own answer (404, unknown network).

A backend that fails must never take out the btc/eth answer the core served
itself: the dashboard loads ``/stats`` and ``/capabilities`` at startup and
searches through ``/search``, so a 500 there made the whole app unusable on
every network while the backend was down (checked 2026-10-14: dead adapter,
startup dead; hung adapter, 60 s per call, then dead). So on a merge a backend
that cannot be reached, does not answer within ``merge_timeout_s`` or answers
an error is left out of the answer and named in
``X-Ikn-Backends-Declined``, like the 429 below. On a proxy (rule 1) an
unreachable backend is a 503 with ``Retry-After``, returned from here so it
still passes the CORS middleware and the browser can read it; any HTTP answer
of the backend, error or not, reaches the client as it is. A 429 is pacing
declining the call, not a fault, and is logged at WARNING only.

A hung backend still cost every merge the full ``merge_timeout_s``: the
dashboard asks for a twin list for every address it adds to the graph, and
the API answers over HTTP/1.1, so a few such calls held the browser's six
connections and delayed the core networks' own requests. So after a backend
could not be reached or did not answer in time (on a merge or a proxy), merges
leave it out without asking it for ``failure_skip_s`` (default 30 s), named in
``X-Ikn-Backends-Declined`` as before. An error answer or a 429 starts no such
window (the backend is up), proxies still always ask it, and any answer ends
the window early.

One alert per outage, not one per request: the first failure of a backend
that answered before is logged at ERROR (the level the Slack handler
forwards, web/app.py:setup_logging), every failure after it at WARNING, and
the first answer after it once more at WARNING. During an outage every
dashboard page and every MCP call would otherwise alert. The state is per
process, so each worker of a deployment alerts once.

Be precise about whose pacing, because the answer is NOT always "this
caller's". The backend charges every provider call to two buckets: the
caller's own share AND the account-wide budget everyone shares. Either can
refuse. So a 429 on a merge means "the lite networks could not be asked right
now" — possibly because THIS caller is over its share, possibly because the
whole account is saturated by somebody else, and, for a request that arrives
without an identity, because the shared host-keyed bucket is full.

A swallowed 429 is therefore a PARTIAL ANSWER, not an empty one, and it must
not be silent: it is logged, and the merged response names the backends that
declined in ``X-Ikn-Backends-Declined``. Without that a user refreshing
Pathfinder mid-investigation just sees the lite currencies disappear, and an
investigator reading a twin list sees "no cross-chain twin" for an address
that has one.

On a PROXY (rule 1, rule 3 with a currency filter) the 429 IS the answer and
reaches the client with its ``Retry-After``.

The middleware must sit INSIDE the CORS middleware (added before it in
``create_app``) so short-circuited proxy responses still receive CORS
headers. Authentication is not a concern here: API keys are validated by the
gateway in front, never by this app (see ``security.get_api_key``). The
gateway's identity header (``consumer_header``, default ``X-Username``) IS
relayed to the backends as ``X-Consumer-Username`` — on the proxy path AND on
every merge fan-out: they pace provider spend per user, and without it every
user would share one bucket. A merge used to send no identity at all, so all
of ``/search``'s provider spend landed on one bucket named after this app's
own address, charged to nobody and subtracted from everybody.
"""

import asyncio
import functools
import json
import logging
import re
import time
from typing import Collection, Dict, List, Optional, Set, Union
from urllib.parse import quote

import httpx
from pydantic import TypeAdapter, ValidationError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

from graphsenselib.db.asynchronous.services.models import ExternalConversion
from graphsenselib.errors import TransactionNotFoundException
from graphsenselib.utils.accountmodel import hex_to_bytes
from graphsenselib.web.config import (
    CurrencyRolesConfig,
    ExternalBackendConfig,
    ExternalBackendsConfig,
)
from graphsenselib.web.middleware.currency_roles import parse_roles

# Client opt-out header, the same one the gateway understands: behind APISIX
# the ikn-auth-validator consumes ``X-Ikn-Currency-Opt-Out`` and strips the
# lite-currency roles instead, so this branch only ever runs without a gateway
# in front (local development, direct access). ``all-light`` and ``all``
# bypass the feature entirely; the gateway's per-code form is not handled here.
OPT_OUT_HEADER = "x-ikn-currency-opt-out"
OPT_OUT_VALUES = frozenset({"all-light", "all"})

logger = logging.getLogger(__name__)

SERVED_BY_HEADER = "x-served-by"
SERVED_BY_VALUE = "external-backend"

# Names the backends whose rows are MISSING from an otherwise-200 merge because
# they declined (429) or failed (unreachable, timed out, error answer). A
# partial answer that looks complete is the failure mode this exists to prevent.
DECLINED_HEADER = "X-Ikn-Backends-Declined"

# how long a client should wait after a proxied request found its backend
# unreachable: the adapter's own overload answers say 30 s as well
UNREACHABLE_RETRY_AFTER_S = 30

# the one backend response header worth carrying to the client on a proxied
# 429: without it a paced caller is told to come back but not when
RETRY_AFTER_HEADER = "retry-after"

# address tag routes stay local (rule 1): TagStore data keyed by real chain
# addresses, owned by this deployment regardless of who serves the chain data
_LOCAL_TAG_PATHS = (
    re.compile(r"^/[^/]+/addresses/[^/]+/(tags|tag_summary)$"),
    re.compile(
        r"^/[^/]+/bulk\.(csv|json)/(list_tags_by_address|get_tag_summary_by_address)$"
    ),
)

# the disabled-flag rule 1 removes from a backend's /capabilities entry (rule 4)
_TAGS_CAPABILITY = "tags"

# merge endpoints: path -> (list field to merge, entry key within it)
_MERGE_FIELDS = {
    "/stats": ("currencies", "name"),
    "/search": ("currencies", "currency"),
    "/capabilities": ("networks", "network"),
}

# rule 5: an address's cross-chain twins, merged from the backends and the
# local pubkey table
_RELATED_ADDRESSES_PATH = re.compile(r"^/[^/]+/addresses/[^/]+/related_addresses$")
_PUBKEY_RELATION = "pubkey"
# the locally served network that has a key's EVM-format address: the backends
# are asked about it, and the local pubkey table is asked as it
_EVM_NETWORK = "eth"

# a tx's conversions: proxied and checked for eth legs Cassandra lacks (rule
# 1), or the bridge rows a backend pairs for the tx (rule 6)
_CONVERSIONS_PATH = re.compile(r"^/([^/]+)/txs/([^/]+)/conversions$")
_CONVERSIONS = TypeAdapter(List[ExternalConversion])
_BRIDGE_TX = "bridge_tx"
# a backend's answers that it cannot answer about the tx, rather than that it
# failed: it cannot pair it (501), does not know it (404), or refuses the
# identifier (400, 422)
_CANNOT_ANSWER_TX = frozenset({400, 404, 422, 501})


class ExternalBackendMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        config: ExternalBackendsConfig,
        client: Optional[httpx.AsyncClient] = None,
        roles_config: Optional[CurrencyRolesConfig] = None,
        gated: Optional[Set[str]] = None,
    ):
        super().__init__(app)
        self.config = config
        # The SAME role set CurrencyRoleMiddleware gates on. It wraps this
        # middleware, so on a listing path it deletes the gated entries the
        # caller lacks the role for AFTER they have been fetched -- the
        # backend has already paid provider CU for rows nobody will see. Both
        # read the gateway's header off the request, so nothing is plumbed
        # between them; None here means no gate and every network is fetched.
        self.roles_config = roles_config
        self.gated = {code.lower() for code in (gated or ())}
        # backend URL -> when it stopped answering (time.monotonic), for
        # one alert per outage (module docstring)
        self.down_since: Dict[str, float] = {}
        # backend URL -> until when merges leave it out without asking it
        # (_now(), failure_skip_s in the module docstring)
        self.skip_until: Dict[str, float] = {}
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(config.timeout_s)
        )

    def _entitled(self, request: Request, networks: Set[str]) -> Set[str]:
        """The subset of `networks` this caller may actually be shown.

        Exactly CurrencyRoleMiddleware's rule (``<prefix><code>`` in the
        gateway's roles header), applied BEFORE the fetch instead of after:
        the answer is identical either way, the difference is whether the
        backend was asked for rows that are about to be dropped. An ungated
        network is always kept, and no roles config means no gate at all."""
        if self.roles_config is None or not self.roles_config.enforce_currency_roles:
            return networks
        if not self.gated:
            return networks
        roles = parse_roles(request.headers.get(self.roles_config.roles_header))
        prefix = self.roles_config.currency_role_prefix
        return {
            code
            for code in networks
            if code.lower() not in self.gated or f"{prefix}{code.lower()}" in roles
        }

    async def dispatch(self, request: Request, call_next) -> Response:
        if not self.config.enabled or not (
            self.config.networks or self.config.delegated_conversions
        ):
            return await call_next(request)
        self._lowercase_network(request)
        opt_out = {
            part.strip().lower()
            for part in request.headers.get(OPT_OUT_HEADER, "").split(",")
        }
        if opt_out & OPT_OUT_VALUES:
            return await call_next(request)
        backend = self._related_addresses_backend(request)
        if backend is not None:
            return await self._related_addresses_of_external_network(request, backend)
        short_circuit = await self._route(request)
        if short_circuit is not None:
            return short_circuit
        backend = self._conversions_backend(request)
        if backend is not None:
            return await self._offering_bridge_pairing(request, call_next, backend)
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

    def _lowercase_network(self, request: Request) -> None:
        """Write an externally served network of the path in lower case.

        Core routes take a network in any case (``/ETH/...`` = ``/eth/...``),
        the configured networks are lower case. Rewriting the request scope,
        before anything reads ``request.url``, makes the routing below, the
        proxied path and a local tag route see the same ``/bnb/...``. Other
        networks keep the case they were written in."""
        path = request.scope["path"]
        first, sep, rest = path.lstrip("/").partition("/")
        network = first.lower()
        if network == first or network not in self.config.networks:
            return
        request.scope["path"] = f"/{network}{sep}{rest}"
        raw_path = request.scope.get("raw_path")
        if raw_path:
            _, raw_sep, raw_rest = raw_path.lstrip(b"/").partition(b"/")
            request.scope["raw_path"] = b"/" + network.encode() + raw_sep + raw_rest

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
            proxied = await self._proxy(request, backend)
            if _CONVERSIONS_PATH.match(path):
                return await self._refused_while_an_eth_leg_is_unindexed(
                    request, proxied
                )
            return proxied
        if path == "/search" and request.method == "GET":
            currency = request.query_params.get("currency")
            if currency is not None:
                backend = self.config.networks.get(currency.lower())
                if backend is not None:
                    return await self._proxy(request, backend)
        return None

    def _wants_merge(self, request: Request) -> bool:
        """True when the local response must be merged with backend answers."""
        if request.method != "GET" or not self.config.networks:
            return False
        if request.url.path in ("/stats", "/capabilities"):
            return True
        if request.url.path == "/search":
            return request.query_params.get("currency") is None
        return self._wants_related_addresses_merge(request)

    def _wants_related_addresses_merge(self, request: Request) -> bool:
        """Rule 5 applies: pubkey twins of a locally served network."""
        if not self.config.merge_related_addresses:
            return False
        if not _RELATED_ADDRESSES_PATH.match(request.url.path):
            return False
        relation_type = request.query_params.get("address_relation_type")
        return relation_type in (None, _PUBKEY_RELATION)

    async def _merge_response(
        self, request: Request, status_code: int, body: bytes
    ) -> Optional[Response]:
        """Merge backend answers into a buffered local merge-endpoint body.

        Returns the merged response, or None when the local response should
        pass through unchanged (non-200 locally, nothing to merge).
        """
        if status_code != 200:
            return None
        path = request.url.path
        if path not in _MERGE_FIELDS:
            return await self._merge_related_addresses(request, body)
        list_field, key = _MERGE_FIELDS[path]
        local_doc = json.loads(body)
        merged = dict(local_doc)
        merged[list_field] = list(local_doc.get(list_field, []))
        query = ("?" + str(request.url.query)) if request.url.query else ""
        declined: List[str] = []
        for base_url, networks in self._backends_by_url(request).items():
            backend_doc = await self._fetch_json(
                request,
                base_url,
                path + query,
                declined=declined,
                # an older backend has no /capabilities (rule 4)
                tolerated={404} if path == "/capabilities" else (),
            )
            if backend_doc is None:
                continue
            entries = [
                entry
                for entry in backend_doc.get(list_field, [])
                if entry.get(key) in networks
            ]
            if path == "/stats":
                entries = [_strip_legacy_capabilities(entry) for entry in entries]
                entries = await self._overlay_tag_counts(request, entries)
            elif path == "/capabilities":
                entries = [_without_tags_disabled(entry) for entry in entries]
            merged[list_field] = _merge_keyed_lists(merged[list_field], entries, key)
        return JSONResponse(merged, status_code=200, headers=_declined_header(declined))

    async def _merge_related_addresses(
        self, request: Request, body: bytes
    ) -> Optional[Response]:
        """Rule 5: append the backends' twins of a locally served address.

        The backends are asked about the key's EVM-format address: the address
        itself on eth, otherwise the ``eth`` row of the local answer. Without
        one (the key was not used on Ethereum) the local answer passes through
        and no backend is asked.

        Local rows keep their order and come first; a backend row is kept when
        its currency is one of that backend's configured networks and no local
        row already names the same (currency, address). Paging stays the local
        one — the backends' unpaginated twin sets are small (one row per EVM
        network at most) and the dashboard asks for a page of 100."""
        local_doc = json.loads(body)
        rows = list(local_doc.get("related_addresses", []))
        _, network, _, address, _ = request.url.path.split("/")
        if network == _EVM_NETWORK:
            path = request.url.path
        else:
            evm = next(
                (
                    row.get("address")
                    for row in rows
                    if row.get("currency") == _EVM_NETWORK
                ),
                None,
            )
            if evm is None:
                return None
            path = f"/{_EVM_NETWORK}/addresses/{evm}/related_addresses"
        present = {(row.get("currency"), row.get("address")) for row in rows}
        query = ("?" + str(request.url.query)) if request.url.query else ""
        declined: List[str] = []
        for base_url, networks in self._backends_by_url(request).items():
            backend_doc = await self._fetch_json(
                request,
                base_url,
                path + query,
                declined=declined,
                # it does not know the address, or declines the question
                tolerated={404, 501},
            )
            if backend_doc is None:
                continue
            for row in backend_doc.get("related_addresses", []):
                marker = (row.get("currency"), row.get("address"))
                if row.get("currency") in networks and marker not in present:
                    rows.append(row)
                    present.add(marker)
        return JSONResponse(
            {**local_doc, "related_addresses": rows},
            status_code=200,
            headers=_declined_header(declined),
        )

    def _related_addresses_backend(
        self, request: Request
    ) -> Optional[ExternalBackendConfig]:
        """The backend of a pubkey related_addresses request on an externally
        served network (rule 5, second case), or None."""
        if not self._wants_related_addresses_merge(request):
            return None
        return self.config.networks.get(request.url.path.split("/")[1])

    async def _related_addresses_of_external_network(
        self, request: Request, backend: ExternalBackendConfig
    ) -> Response:
        """Rule 5 on an externally served network: the backend's answer, and
        the local pubkey rows of the same address on its last page.

        Both are asked at once. Anything but a 200 of the backend reaches the
        client as it is; the local rows go on the last page only, so paging
        through the backend's answer never repeats them."""
        address = request.url.path.split("/")[3]
        proxied, local_rows = await asyncio.gather(
            self._proxy(request, backend),
            self._local_pubkey_twins(request, address),
        )
        if proxied.status_code != 200 or not local_rows:
            return proxied
        doc = json.loads(proxied.body)
        if doc.get("next_page"):
            return proxied
        rows = list(doc.get("related_addresses", []))
        present = {(row.get("currency"), row.get("address")) for row in rows}
        rows += [
            row
            for row in local_rows
            if (row.get("currency"), row.get("address")) not in present
        ]
        return JSONResponse(
            {**doc, "related_addresses": rows},
            status_code=200,
            headers={SERVED_BY_HEADER: SERVED_BY_VALUE},
        )

    async def _local_pubkey_twins(self, request: Request, address: str) -> list:
        """The local pubkey rows of an EVM-format address, asked as eth, on
        the locally served networks only; [] if the local lookup fails.

        Asked of the app behind this middleware with the request's own scope
        (the app, its state, the caller's headers), only path and query
        changed: ``call_next`` always runs the request's own path, and this
        one is on a backend's network."""
        path = f"/{_EVM_NETWORK}/addresses/{address}/related_addresses"
        scope = {
            **request.scope,
            "path": path,
            "raw_path": path.encode(),
            "query_string": f"address_relation_type={_PUBKEY_RELATION}".encode(),
        }
        received = asyncio.Event()

        async def receive() -> dict:
            # an empty GET body, then nothing until the response is complete
            if received.is_set():
                await asyncio.Event().wait()
            received.set()
            return {"type": "http.request", "body": b"", "more_body": False}

        status: List[int] = []
        chunks: List[bytes] = []

        async def send(message: dict) -> None:
            if message["type"] == "http.response.start":
                status.append(message["status"])
            elif message["type"] == "http.response.body":
                chunks.append(message.get("body", b""))

        # The second exception this module handles: the backend answers this
        # request, and these rows are an addition to its answer, so a local
        # failure (the pubkey table, Cassandra) leaves them out instead of
        # turning the backend's answer into a 500 (module docstring, rule 5).
        try:
            await self.app(scope, receive, send)
        except Exception as exc:
            logger.warning(
                "local pubkey twins of %s left out of %s: %r",
                address,
                request.url.path,
                exc,
            )
            return []
        if status != [200]:
            logger.warning(
                "local pubkey twins of %s left out of %s: HTTP %s",
                address,
                request.url.path,
                status,
            )
            return []
        return [
            row
            for row in json.loads(b"".join(chunks)).get("related_addresses", [])
            if row.get("currency") not in self.config.networks
        ]

    async def _overlay_tag_counts(self, request: Request, entries: list) -> list:
        """Overwrite a backend stats entry's tag counts with the local
        TagStore's per-network numbers (rule 2). The backend cannot know the
        TagStore fronting it and serves zeros; absent network rows (or the
        mock TagStore) yield zeros again, so the overlay never invents data."""
        if not entries:
            return entries
        tagstore_db = getattr(request.app.state, "tagstore_db", None)
        if tagstore_db is None:
            return entries
        tag_stats = await tagstore_db.get_network_statistics_cached()
        overlaid = []
        for entry in entries:
            network_stats = tag_stats.by_network.get(str(entry.get("name")).upper())
            overlaid.append(
                {
                    **entry,
                    "no_labels": network_stats.nr_labels if network_stats else 0,
                    "no_tagged_addresses": (network_stats.nr_identifiers_implicit or 0)
                    if network_stats
                    else 0,
                }
            )
        return overlaid

    def _backends_by_url(self, request: Optional[Request] = None) -> Dict[str, set]:
        """Group configured networks by backend URL — one call per backend.

        With a request, only the networks this caller is entitled to see.

        Read that precisely: it narrows which BACKENDS are called and which of
        their rows are kept, NOT what each backend is asked for. A backend
        left with no entitled network is dropped and never called at all --
        that is the whole saving, since a caller with no lite-currency role
        used to trigger the full fan-out and then have every row deleted by
        the role gate wrapping this middleware. But all lite networks share
        one backend URL today, so a caller entitled to ONE of them still
        triggers that backend's full multi-network query; only the rows it
        returns are narrowed. Cutting that would need the backend to accept a
        network filter on the listing endpoints."""
        grouped: Dict[str, set] = {}
        for network, backend in self.config.networks.items():
            grouped.setdefault(backend.url, set()).add(network)
        if request is None:
            return grouped
        entitled = {}
        for url, networks in grouped.items():
            kept = self._entitled(request, networks)
            if kept:
                entitled[url] = kept
        return entitled

    def _api_key_for_url(self, base_url: str) -> Optional[str]:
        backends = [
            *self.config.networks.values(),
            *self.config.delegated_conversions.values(),
        ]
        for backend in backends:
            if backend.url == base_url and backend.api_key:
                return backend.api_key
        return None

    def _consumer_of(self, request: Request) -> Optional[str]:
        """The caller's identity for the backend's per-user pacing.

        The gateway strips and re-asserts ``consumer_header``, so its value is
        trustworthy here. Used by BOTH the proxy and the merge fan-out: the
        backend keys a provider-spend bucket on it, and two paths disagreeing
        about who the caller is would give one user two buckets."""
        if not self.config.consumer_header:
            return None
        return request.headers.get(self.config.consumer_header)

    async def _fetch_json(
        self,
        request: Request,
        base_url: str,
        path_and_query: str,
        declined: Optional[List[str]] = None,
        tolerated: Collection[int] = (),
        timeout: Optional[float] = None,
        log_tolerated: bool = False,
    ) -> Optional[Union[dict, list]]:
        """One merge fan-out call. Returns None when the backend declines or
        fails: a 429, no answer within `timeout` (default merge_timeout_s), no
        connection, or any other status than 200 (apart from a status in
        `tolerated`, which is no failure; `log_tolerated` logs it at INFO with
        the backend's ``detail``).

        A 429 is why this takes the request at all: relaying the caller's
        identity means the backend paces per caller rather than lumping
        everyone into one bucket, and a paced request must still get its
        local btc/eth answer instead of a 500. A failed backend must not take
        that answer out either (module docstring).

        It appends to `declined` and logs, because the result is a PARTIAL
        answer that is otherwise shaped exactly like a complete one — see the
        module docstring on whose pacing a 429 actually represents.

        A backend that failed within the last failure_skip_s is not asked at
        all: it is left out and named at once, without a log line (its outage
        is already alerted)."""
        if _now() < self.skip_until.get(base_url, 0.0):
            if declined is not None and base_url not in declined:
                declined.append(base_url)
            return None
        headers = {"Accept": "application/json"}
        api_key = self._api_key_for_url(base_url)
        if api_key:
            headers["Authorization"] = api_key
        consumer = self._consumer_of(request)
        if consumer:
            headers["X-Consumer-Username"] = consumer
        # The one exception this module handles: a backend that cannot be
        # reached or does not answer in time is a partial answer, exactly
        # like a declined one (module docstring), not a 500 for every network.
        try:
            response = await self.client.get(
                base_url.rstrip("/") + path_and_query,
                headers=headers,
                timeout=self.config.merge_timeout_s if timeout is None else timeout,
            )
        except httpx.TransportError as exc:
            self._merge_failed(declined, base_url, path_and_query, repr(exc))
            self._skip(base_url)
            return None
        if response.status_code == 429:
            if declined is not None and base_url not in declined:
                declined.append(base_url)
            logger.warning(
                "external backend %s declined %s for consumer %s (429, retry after %s)"
                " — those networks are MISSING from an otherwise-200 answer",
                base_url,
                path_and_query,
                consumer or "<anonymous>",
                response.headers.get(RETRY_AFTER_HEADER, "unspecified"),
            )
            self._backend_answered(base_url)
            return None
        if response.status_code in tolerated:
            self._backend_answered(base_url)
            if log_tolerated:
                logger.info(
                    "external backend %s cannot answer %s (%s)",
                    base_url,
                    path_and_query,
                    _status_and_detail(response),
                )
            return None
        if response.status_code != 200:
            self._merge_failed(
                declined, base_url, path_and_query, f"HTTP {response.status_code}"
            )
            return None
        self._backend_answered(base_url)
        return response.json()

    def _merge_failed(
        self,
        declined: Optional[List[str]],
        base_url: str,
        path_and_query: str,
        reason: str,
    ) -> None:
        """A merge call that failed: the backend is named in the response."""
        if declined is not None and base_url not in declined:
            declined.append(base_url)
        self._backend_failed(base_url, path_and_query, reason)

    def _backend_failed(self, base_url: str, what: str, reason: str) -> None:
        """Log a failed call: ERROR (an alert) if the backend answered
        before, WARNING while it stays down."""
        if base_url in self.down_since:
            logger.warning(
                "external backend %s still not answering: %s (%s)",
                base_url,
                what,
                reason,
            )
            return
        self.down_since[base_url] = time.monotonic()
        logger.error(
            "external backend %s stopped answering (%s: %s) — %s until it answers"
            " again",
            base_url,
            what,
            reason,
            self._outage_effect(base_url),
        )

    def _outage_effect(self, base_url: str) -> str:
        """What a backend's outage takes out, for its alert."""
        effects = []
        networks = self._backends_by_url().get(base_url, set())
        if networks:
            effects.append(
                f"{', '.join(sorted(networks))} are left out of merged answers"
                " and their requests get 503"
            )
        paired = sorted(
            network
            for network, backend in self.config.delegated_conversions.items()
            if backend.url == base_url
        )
        if paired:
            effects.append(f"{', '.join(paired)} conversions lack the bridges it pairs")
        return "; ".join(effects)

    def _skip(self, base_url: str) -> None:
        """Leave a backend that could not be reached or did not answer in
        time out of merges for failure_skip_s (module docstring)."""
        self.skip_until[base_url] = _now() + self.config.failure_skip_s

    def _backend_answered(self, base_url: str) -> None:
        """The first answer after an outage ends it, logged once. Any answer
        ends a skip window too."""
        self.skip_until.pop(base_url, None)
        since = self.down_since.pop(base_url, None)
        if since is None:
            return
        logger.warning(
            "external backend %s answers again after %.0f s",
            base_url,
            time.monotonic() - since,
        )

    async def _proxy(
        self, request: Request, backend: ExternalBackendConfig
    ) -> Response:
        url = backend.url.rstrip("/") + request.url.path
        if request.url.query:
            url += "?" + str(request.url.query)
        headers = {"Accept": request.headers.get("accept", "application/json")}
        if backend.api_key:
            headers["Authorization"] = backend.api_key
        consumer = self._consumer_of(request)
        if consumer:
            headers["X-Consumer-Username"] = consumer
        body = await request.body() if request.method not in ("GET", "HEAD") else None
        if body:
            headers["Content-Type"] = request.headers.get(
                "content-type", "application/json"
            )
        try:
            backend_response = await self.client.request(
                request.method, url, headers=headers, content=body
            )
        except httpx.TransportError as exc:
            self._backend_failed(
                backend.url, f"{request.method} {request.url.path}", repr(exc)
            )
            self._skip(backend.url)
            return JSONResponse(
                {"detail": "The service for this network is unavailable."},
                status_code=503,
                headers={
                    SERVED_BY_HEADER: SERVED_BY_VALUE,
                    RETRY_AFTER_HEADER: str(UNREACHABLE_RETRY_AFTER_S),
                },
            )
        # any answer, a 503 of an overloaded backend too: it is up
        self._backend_answered(backend.url)
        headers = {SERVED_BY_HEADER: SERVED_BY_VALUE}
        # a 429 that does not say when to come back is barely better than a
        # 500; this is the only backend header the proxy carries through
        retry_after = backend_response.headers.get(RETRY_AFTER_HEADER)
        if retry_after:
            headers[RETRY_AFTER_HEADER] = retry_after
        return Response(
            content=backend_response.content,
            status_code=backend_response.status_code,
            media_type=backend_response.headers.get("content-type"),
            headers=headers,
        )

    async def _refused_while_an_eth_leg_is_unindexed(
        self, request: Request, proxied: Response
    ) -> Response:
        """Rule 1: a proxied conversions answer as it is, or a 501 while
        Cassandra lacks an eth tx it names as a leg. One lookup per eth tx
        named, all at once; an error answer, or one naming no eth tx, reads
        nothing."""
        if proxied.status_code != 200:
            return proxied
        try:
            rows = json.loads(proxied.body)
        except ValueError:
            return proxied
        txs = _eth_leg_txs(rows) if isinstance(rows, list) else []
        if not txs:
            return proxied
        indexed = await asyncio.gather(*(self._indexed(request, tx) for tx in txs))
        missing = [tx for tx, found in zip(txs, indexed) if not found]
        if not missing:
            return proxied
        logger.info(
            "%s answered 501: Cassandra lacks its %s legs %s",
            request.url.path,
            _EVM_NETWORK,
            ", ".join(missing),
        )
        return JSONResponse(
            {
                "detail": f"A leg of this conversion on {_EVM_NETWORK} is not "
                "indexed yet.",
                "code": "NOT_IMPLEMENTED",
            },
            status_code=501,
            headers={SERVED_BY_HEADER: SERVED_BY_VALUE},
        )

    async def _indexed(self, request: Request, tx_hash: str) -> bool:
        """Whether Cassandra has the eth tx `tx_hash`, in one lookup."""
        # A lookup that fails cannot prove that the leg is there, so the
        # answer is refused as if the leg were missing.
        try:
            await request.app.state.db.get_tx_by_hash(
                _EVM_NETWORK, hex_to_bytes(tx_hash)
            )
        except TransactionNotFoundException:
            return False
        except Exception as exc:
            logger.warning(
                "%s leg %s of %s could not be looked up: %r",
                _EVM_NETWORK,
                tx_hash,
                request.url.path,
                exc,
            )
            return False
        return True

    def _conversions_backend(self, request: Request) -> Optional[ExternalBackendConfig]:
        """The backend that pairs the bridges of this request's conversions
        (rule 6), or None. None too for a caller entitled to no gated
        currency: behind the gateway that is how the client opt-out arrives."""
        if request.method != "GET":
            return None
        match = _CONVERSIONS_PATH.match(request.url.path)
        if match is None:
            return None
        backend = self.config.delegated_conversions.get(match.group(1).lower())
        if backend is None or (self.gated and not self._entitled(request, self.gated)):
            return None
        return backend

    async def _offering_bridge_pairing(
        self, request: Request, call_next, backend: ExternalBackendConfig
    ) -> Response:
        """Rule 6: the conversions route may have `backend` pair the bridges
        it finds only half of; the answer passes through untouched, apart
        from naming the backend in the declined header when that failed."""
        declined: List[str] = []
        # read by the conversions route (routes/txs.py)
        request.state.pair_bridges = functools.partial(
            self._paired_bridges, request, backend, declined
        )
        response = await call_next(request)
        response.headers.update(_declined_header(declined))
        return response

    async def _paired_bridges(
        self,
        request: Request,
        backend: ExternalBackendConfig,
        declined: List[str],
    ) -> List[ExternalConversion]:
        """Rule 6: the bridge rows the backend pairs for the requested tx.
        [] when it cannot answer or fails, logged with why (_fetch_json); a
        failure is named in `declined`."""
        network, identifier = _CONVERSIONS_PATH.match(request.url.path).groups()
        # percent-encoded: the local lookup ignores whitespace in a hash
        # (bytes.fromhex), so a control character can get this far. Only the
        # bridges: the tx's swaps are the local detection's, so the backend
        # need not read or price them.
        path = (
            f"/{network.lower()}/txs/{quote(identifier, safe='')}/conversions"
            "?only_bridges=true"
        )
        try:
            doc = await self._fetch_json(
                request,
                backend.url,
                path,
                declined=declined,
                tolerated=_CANNOT_ANSWER_TX,
                timeout=self.config.delegated_conversions_timeout_s,
                log_tolerated=True,
            )
        except (ValueError, httpx.DecodingError) as exc:
            # an answer whose body cannot be read is a failed one: the half
            # bridges are served, never a 500
            self._merge_failed(declined, backend.url, path, repr(exc))
            return []
        if doc is None:
            return []
        try:
            rows = _CONVERSIONS.validate_python(doc)
        except ValidationError as exc:
            if backend.url not in declined:
                declined.append(backend.url)
            logger.warning(
                "external backend %s answered %s with malformed conversions"
                " (%d validation errors) — the bridges it pairs are MISSING",
                backend.url,
                path,
                exc.error_count(),
            )
            return []
        # the local lookup ignores whitespace in a hash (bytes.fromhex)
        tx = "".join(_tx_of(identifier).split())
        bridges = [
            row
            for row in rows
            if row.conversion_type == _BRIDGE_TX
            and tx in {_tx_of(row.from_asset_transfer), _tx_of(row.to_asset_transfer)}
        ]
        networks = {n for row in bridges for n in (row.from_network, row.to_network)}
        entitled = self._entitled(request, networks)
        return [
            row
            for row in bridges
            if row.from_network in entitled and row.to_network in entitled
        ]


def _tx_of(transfer: str) -> str:
    """The tx an asset transfer or a tx identifier names: its hash without
    the sub-tx suffix, in lower case and without 0x."""
    return transfer.partition("_")[0].lower().removeprefix("0x")


def _eth_leg_txs(rows: list) -> List[str]:
    """The distinct eth txs a conversions answer names as legs (rule 1), as
    _tx_of writes them, in the order they are named."""
    legs = (
        _tx_of(str(row.get(f"{side}_asset_transfer") or ""))
        for row in rows
        if isinstance(row, dict)
        for side in ("from", "to")
        if row.get(f"{side}_network") == _EVM_NETWORK
    )
    return [tx for tx in dict.fromkeys(legs) if tx]


def _now() -> float:
    """The clock of the skip windows; tests replace it to move time."""
    return time.monotonic()


def _declined_header(declined: List[str]) -> dict:
    """Name the backends whose rows are missing, or say nothing when none are.
    An empty header would assert "nothing declined" on every response, which is
    a claim this middleware cannot make about a backend it never called."""
    return {DECLINED_HEADER: ", ".join(declined)} if declined else {}


def _status_and_detail(response: httpx.Response) -> str:
    """A backend's answer for the log: its status, and the ``detail`` of its
    JSON body when it has one."""
    try:
        doc = response.json()
    except ValueError:
        doc = None
    detail = doc.get("detail") if isinstance(doc, dict) else None
    if detail is None:
        return f"HTTP {response.status_code}"
    return f"HTTP {response.status_code}: {detail}"


def _strip_legacy_capabilities(entry: dict) -> dict:
    """Drop a legacy ``capabilities`` field from a backend stats entry (rule 2)."""
    if "capabilities" not in entry:
        return entry
    return {k: v for k, v in entry.items() if k != "capabilities"}


def _without_tags_disabled(entry: dict) -> dict:
    """Remove ``"tags"`` from a backend capabilities entry's disabled list
    (rule 4): tag routes are answered locally, so tags ARE served here."""
    disabled = entry.get("disabled")
    if not isinstance(disabled, list) or _TAGS_CAPABILITY not in disabled:
        return entry
    return {**entry, "disabled": [d for d in disabled if d != _TAGS_CAPABILITY]}


def _merge_keyed_lists(base_entries: list, extra_entries: list, key: str) -> list:
    """Merge per-currency entry lists: base order is kept, an extra entry
    REPLACES a base entry with the same key (the backend is authoritative for
    the networks it serves), and remaining extras are appended."""
    extra_by_key = {entry[key]: entry for entry in extra_entries}
    merged = [extra_by_key.pop(entry[key], entry) for entry in base_entries]
    merged.extend(extra_by_key.values())
    return merged
