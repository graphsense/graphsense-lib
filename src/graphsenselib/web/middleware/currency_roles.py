"""Gate externally served ("extended") currencies on the caller's roles.

The gateway in front of this app (APISIX, ikn-auth-validator) sets
``X-User-Roles`` on every authenticated request, for API-key and OIDC traffic
alike: a comma-separated list of Keycloak realm roles with the ``ikn-``
prefix removed, each element percent-encoded. Clients cannot spoof it — the
gateway strips any inbound copy before injecting its own — so this app can
trust it. An absent header means no roles.

Rule: a request on currency X is allowed iff ``<prefix>X`` is in the role
set (``currency-bnb`` grants bnb). That is the only check: the grouped role
``currencies-extended`` also appears in the header, but Keycloak expands it
into every per-currency role before the token is issued, so checking the
leaves is complete and needs no bundle list here. The legacy
``X-Consumer-Groups`` header (Kong-era) plays no part in this feature.

Scope: only currencies marked as gated — by default every network served
by an external backend (``external_backends.networks``), or an explicit
``auth.gated_currencies`` list. Core currencies (btc, eth, …) stay open.

On a miss the request answers 403 ``{"detail": "currency not enabled for
this account"}``. Listings drop the gated currencies the caller lacks —
``/stats`` and ``/capabilities`` entries, ``/search`` per-currency hits and
``related_addresses`` twin rows — so the dashboard hides them instead of
discovering them through 403s.

The middleware sits OUTSIDE the external-backends middleware (added after it
in ``create_app``): a refused request never reaches a backend, and the
listing filter sees the merged answer. It stays INSIDE the CORS middleware so
the 403 carries CORS headers. ``auth.enforce_currency_roles: false`` turns
the whole gate off without a redeploy (local development has no gateway and
therefore no header).
"""

import json
import re
from typing import Dict, Optional, Set
from urllib.parse import unquote

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

from graphsenselib.web.config import CurrencyRolesConfig

DENIED_DETAIL = "currency not enabled for this account"

# listings whose per-currency entries are filtered: path -> (list field, entry key)
_LISTING_FIELDS: Dict[str, tuple] = {
    "/stats": ("currencies", "name"),
    "/search": ("currencies", "currency"),
    "/capabilities": ("networks", "network"),
}
_RELATED_ADDRESSES_PATH = re.compile(r"^/[^/]+/addresses/[^/]+/related_addresses$")
_RELATED_ADDRESSES_FIELDS = ("related_addresses", "currency")


def parse_roles(header_value: Optional[str]) -> Set[str]:
    """Split the roles header into a set: comma-separated, whitespace
    stripped, each element percent-decoded. None/empty -> no roles."""
    if not header_value:
        return set()
    return {unquote(part.strip()) for part in header_value.split(",") if part.strip()}


class CurrencyRoleMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, config: CurrencyRolesConfig, gated: Set[str]):
        super().__init__(app)
        self.config = config
        self.gated = {c.lower() for c in gated}

    async def dispatch(self, request: Request, call_next) -> Response:
        if not self.gated:
            return await call_next(request)
        roles = parse_roles(request.headers.get(self.config.roles_header))
        denied = self._denied_currency(request, roles)
        if denied is not None:
            return JSONResponse({"detail": DENIED_DETAIL}, status_code=403)
        fields = self._listing_fields(request)
        if fields is None:
            return await call_next(request)
        response = await call_next(request)
        if response.status_code != 200:
            return response
        body = b"".join([chunk async for chunk in response.body_iterator])
        doc = json.loads(body)
        list_field, key = fields
        entries = doc.get(list_field)
        if not isinstance(entries, list):
            return Response(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type,
            )
        kept = [
            entry
            for entry in entries
            if not isinstance(entry, dict) or self._allowed(entry.get(key), roles)
        ]
        return JSONResponse({**doc, list_field: kept}, status_code=200)

    def _allowed(self, currency, roles: Set[str]) -> bool:
        code = str(currency).lower() if currency is not None else ""
        if code not in self.gated:
            return True
        return f"{self.config.currency_role_prefix}{code}" in roles

    def _denied_currency(self, request: Request, roles: Set[str]) -> Optional[str]:
        """The gated currency this request addresses without the role, if any:
        the first path segment (``/bnb/...``), or ``/search?currency=bnb``."""
        first = request.url.path.strip("/").split("/", 1)[0].lower()
        if first in self.gated and not self._allowed(first, roles):
            return first
        if request.url.path == "/search":
            currency = request.query_params.get("currency")
            if currency is not None and not self._allowed(currency, roles):
                return currency.lower()
        return None

    def _listing_fields(self, request: Request) -> Optional[tuple]:
        if request.method != "GET":
            return None
        fields = _LISTING_FIELDS.get(request.url.path)
        if fields is not None:
            return fields
        if _RELATED_ADDRESSES_PATH.match(request.url.path):
            return _RELATED_ADDRESSES_FIELDS
        return None
