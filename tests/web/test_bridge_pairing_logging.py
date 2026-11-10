"""What the log says about a bridge pairing (middleware rule 6) and about
the merge that follows it (web/service/txs_service.py), so that an on-call
engineer can answer "why was tx X not paired" and "what did the merge do"
from Loki alone.

One INFO ``event=bridge_pairing`` line per pairing call, with a logfmt tail
(``| logfmt`` in LogQL): the backend asked, the identifier sent, its status,
the time it took, the rows taken, and a reason — the 501 body's ``reason``
when the backend sent one, else a gs-rest-side code. One INFO
``event=bridge_merge`` line per conversions request with half bridges.

Set up as tests/web/test_delegated_conversions.py: the real txs route and
TxsService over a fake database, the middleware in front, an httpx
MockTransport for the backend.
"""

import asyncio
import logging
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from graphsenselib.db.asynchronous.services.models import ExternalConversion
from graphsenselib.db.asynchronous.services.txs_service import TxConversions
from graphsenselib.web.middleware.external_backends import DECLINED_HEADER
from graphsenselib.web.service.txs_service import get_tx_conversions

from tests.web.test_delegated_conversions import (
    BACKEND_URL,
    BRIDGE_ROW,
    CCTP_BURN,
    CONVERSIONS,
    GATED,
    HALF_ROW,
    LOCAL_ROW,
    ROLES,
    SWAP_ROW,
    local_swap,  # noqa: F401 -- the autouse fixture of that module
    make_client,
    records_of_the_middleware,
)
from tests.web.test_pairing_middleware_fixes import merging_client, pairing_client

_FIELD = re.compile(r'(\w+)=("(?:[^"\\]|\\.)*"|\S*)')


def fields_of(record: logging.LogRecord) -> dict:
    """The logfmt tail of a record's message, after its last ``" | "``."""
    message = record.getMessage()
    _, separator, tail = message.rpartition(" | ")
    assert separator, message
    fields = {}
    for key, value in _FIELD.findall(tail):
        if value.startswith('"'):
            value = value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        fields[key] = value
    return fields


def lines_of(records, event: str):
    return [(r, fields_of(r)) for r in records if f"event={event}" in r.getMessage()]


def the_pairing_line(records):
    ((record, fields),) = lines_of(records, "bridge_pairing")
    assert record.levelno == logging.INFO
    return record, fields


# ---------------------------------------------------------------------------
# one bridge_pairing line per pairing call
# ---------------------------------------------------------------------------


def test_a_paired_tx_logs_one_bridge_pairing_line():
    client, _, _ = make_client()
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS, headers={"X-Username": "alice"})
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]
    record, fields = the_pairing_line(records)
    assert fields["backend"] == BACKEND_URL
    assert fields["network"] == "eth"
    assert fields["tx"] == CCTP_BURN
    assert fields["status"] == "200"
    assert float(fields["ms"]) >= 0
    assert fields["rows"] == "1"
    assert fields["reason"] == "paired"
    assert fields["consumer"] == "alice"
    assert "paired" in record.getMessage()


def test_a_501_logs_the_reason_the_backend_sent():
    answer = httpx.Response(
        501,
        json={"detail": "cannot pair it", "reason": "mint_not_found"},
        headers={"X-Request-Id": "adapter-7"},
    )
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert DECLINED_HEADER not in response.headers
    # a tolerated refusal alerts nothing: the pairing line and the merge line
    assert [r.levelno for r in records] == [logging.INFO, logging.INFO]
    record, fields = the_pairing_line(records)
    assert f"cannot pair /eth/txs/{CCTP_BURN}/conversions" in record.getMessage()
    assert fields["status"] == "501"
    assert fields["reason"] == "mint_not_found"
    assert fields["rows"] == "0"
    assert fields["backend_request_id"] == "adapter-7"


@pytest.mark.parametrize(
    "answer, status, reason, rows",
    [
        (httpx.Response(501, json={"detail": "cannot pair it"}), "501", "http_501", 0),
        (httpx.Response(501, json={"reason": 7}), "501", "http_501", 0),
        (httpx.Response(501, content=b"nope"), "501", "http_501", 0),
        (httpx.Response(404, json={"detail": "unknown"}), "404", "not_found", 0),
        (httpx.Response(429, headers={"Retry-After": "7"}), "429", "rate_limited", 0),
        (httpx.Response(500, json={"detail": "boom"}), "500", "http_500", 0),
        (httpx.Response(200, content=b"<html>busy</html>"), "200", "invalid_json", 0),
        (
            httpx.Response(200, json=[{"conversion_type": "bridge_tx"}]),
            "200",
            "invalid_rows",
            0,
        ),
        (httpx.Response(200, json=[]), "200", "empty", 0),
        (httpx.Response(200, json=[SWAP_ROW]), "200", "empty", 0),
        (
            httpx.Response(
                200,
                json=[
                    {
                        **BRIDGE_ROW,
                        "from_asset_transfer": f"{'33' * 32}_T1",
                        "to_asset_transfer": f"{'44' * 32}_T2",
                    }
                ],
            ),
            "200",
            "empty",
            0,
        ),
    ],
    ids=[
        "501 without a reason",
        "501 with a reason that is no string",
        "501 without json",
        "404",
        "429",
        "500",
        "not json",
        "invalid row",
        "no rows",
        "only a swap",
        "rows of another tx",
    ],
)
def test_the_reason_of_an_answer_that_pairs_nothing(answer, status, reason, rows):
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    _, fields = the_pairing_line(records)
    assert fields["status"] == status
    assert fields["reason"] == reason
    assert fields["rows"] == str(rows)
    assert float(fields["ms"]) >= 0


@pytest.mark.parametrize(
    "error, reason",
    [
        (httpx.ConnectError("refused"), "transport_ConnectError"),
        (httpx.ReadTimeout("hung"), "transport_ReadTimeout"),
    ],
)
def test_a_transport_failure_logs_its_type_and_no_status(error, reason):
    client, _, _ = make_client(fail_with=error)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    _, fields = the_pairing_line(records)
    assert fields["status"] == "-"
    assert fields["reason"] == reason
    assert float(fields["ms"]) >= 0
    # the alert stays
    assert [r.levelno for r in records if r.levelno == logging.ERROR] == [logging.ERROR]


def test_a_pairing_over_merge_timeout_s_logs_timeout():
    async def slow(request):
        await asyncio.sleep(3)
        return httpx.Response(200, json=[BRIDGE_ROW])

    client, _, _ = make_client(timeout_s=0.2, backend_answer=slow)
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS)
    _, fields = the_pairing_line(records)
    assert fields["status"] == "-"
    assert fields["reason"] == "timeout"
    assert float(fields["ms"]) >= 200


def test_a_pairing_withheld_for_entitlement_says_so():
    """The rows came (rows=1), the caller may not see every network they
    name, so none is taken."""
    to_pol = {
        **BRIDGE_ROW,
        "from_asset_transfer": f"{CCTP_BURN[2:]}_T700",
        "to_asset_transfer": f"{'22' * 32}_T1",
        "to_network": "pol",
    }
    client, _, _ = make_client(
        roles_config=ROLES,
        gated=GATED,
        backend_answer=httpx.Response(200, json=[BRIDGE_ROW, to_pol]),
    )
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS, headers={"X-User-Roles": "currency-arb"})
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    _, fields = the_pairing_line(records)
    assert fields["status"] == "200"
    assert fields["rows"] == "2"
    assert fields["reason"] == "entitlement"


def test_a_pairing_skipped_in_a_merges_skip_window_is_logged_too():
    """A skipped call used to leave no line at all; the pairing line says
    the backend was not asked."""
    client, seen = merging_client(
        httpx.Response(200, json=[BRIDGE_ROW]),
        stats_answer=httpx.ConnectError("refused"),
    )
    client.get("/stats")
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.headers[DECLINED_HEADER] == BACKEND_URL
    assert [r.url.path for r in seen] == ["/stats"]
    _, fields = the_pairing_line(records)
    assert fields["status"] == "-"
    assert fields["reason"] == "skipped"
    assert fields["rows"] == "0"


def test_the_identifier_sent_is_logged_as_sent():
    """The route may name the tx the backend is asked about; the line
    carries that identifier, percent-encoded as on the wire."""
    client, seen = pairing_client({"tx_identifier": f"{CCTP_BURN}\x0b"})
    with records_of_the_middleware() as records:
        client.get(f"/eth/txs/{CCTP_BURN}_I0/conversions")
    (call,) = seen
    assert call.url.raw_path.decode().startswith(f"/eth/txs/{CCTP_BURN}%0B/")
    _, fields = the_pairing_line(records)
    assert fields["tx"] == f"{CCTP_BURN}%0B"


def test_the_line_never_carries_the_api_key_or_url_credentials():
    client, _, _ = make_client(
        delegated_conversions={
            "eth": {"url": "https://user:s3cret@backend.test", "api_key": "key-9"}
        }
    )
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS)
    _, fields = the_pairing_line(records)
    assert fields["backend"] == "https://backend.test"
    for record in records:
        assert "s3cret" not in record.getMessage()
        assert "key-9" not in record.getMessage()


def test_a_consumer_is_omitted_when_none_was_forwarded():
    client, _, _ = make_client()
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS)
    _, fields = the_pairing_line(records)
    assert "consumer" not in fields
    assert "backend_request_id" not in fields


# ---------------------------------------------------------------------------
# one bridge_merge line per conversions request with half bridges
# ---------------------------------------------------------------------------


def context(result):
    return SimpleNamespace(
        services=SimpleNamespace(
            txs_service=SimpleNamespace(get_conversions=AsyncMock(return_value=result))
        ),
        config=SimpleNamespace(included_bridges=("cctp",), cow_protocol_swaps=False),
    )


async def merged_with_log(halves, paired, rows=(), pair=True):
    result = TxConversions(
        tuple(ExternalConversion(**{**r, "conversion_type": "swap"}) for r in rows),
        tuple(ExternalConversion(**{**r, "conversion_type": "bridge"}) for r in halves),
    )
    pairing = (
        AsyncMock(return_value=[ExternalConversion(**r) for r in paired])
        if pair
        else None
    )
    with records_of_the_middleware() as records:
        answer = await get_tx_conversions(context(result), "eth", CCTP_BURN, pairing)
    return [r.to_dict() for r in answer], lines_of(records, "bridge_merge")


@pytest.mark.asyncio
async def test_a_pairing_taken_logs_the_merge():
    local = {**LOCAL_ROW, "to_asset_transfer": f"{CCTP_BURN}_T694"}
    answer, lines = await merged_with_log([HALF_ROW], [BRIDGE_ROW], [local])
    assert answer == [BRIDGE_ROW]
    ((record, fields),) = lines
    assert record.levelno == logging.INFO
    assert fields["tx"] == CCTP_BURN
    assert fields["halves"] == "1"
    assert fields["paired_rows"] == "1"
    assert fields["accepted"] == "1"
    assert fields["ambiguous"] == "0"
    assert fields["no_match"] == "0"
    assert fields["local_dropped"] == "1"
    assert fields["kept_halves"] == "0"


@pytest.mark.asyncio
async def test_a_row_naming_no_half_is_counted_as_no_match():
    stargate = {**BRIDGE_ROW, "from_asset_transfer": f"{CCTP_BURN}_T700"}
    answer, lines = await merged_with_log([HALF_ROW], [stargate], [LOCAL_ROW])
    assert answer == [LOCAL_ROW, HALF_ROW]
    ((_, fields),) = lines
    assert fields["paired_rows"] == "1"
    assert fields["accepted"] == "0"
    assert fields["no_match"] == "1"
    assert fields["ambiguous"] == "0"
    assert fields["local_dropped"] == "0"
    assert fields["kept_halves"] == "1"


@pytest.mark.asyncio
async def test_two_rows_naming_one_half_are_ambiguous():
    answer, lines = await merged_with_log([HALF_ROW], [BRIDGE_ROW, BRIDGE_ROW])
    assert answer == [HALF_ROW]
    ((_, fields),) = lines
    assert fields["paired_rows"] == "2"
    assert fields["accepted"] == "0"
    assert fields["ambiguous"] == "2"
    assert fields["no_match"] == "0"
    assert fields["kept_halves"] == "1"


@pytest.mark.asyncio
async def test_halves_nobody_paired_log_the_merge_too():
    """No backend offered a pairing: the line says the half stayed."""
    answer, lines = await merged_with_log([HALF_ROW], [], pair=False)
    assert answer == [HALF_ROW]
    ((_, fields),) = lines
    assert fields["halves"] == "1"
    assert fields["paired_rows"] == "0"
    assert fields["kept_halves"] == "1"


@pytest.mark.asyncio
async def test_a_tx_without_halves_logs_no_merge():
    answer, lines = await merged_with_log([], [], [LOCAL_ROW])
    assert answer == [LOCAL_ROW]
    assert lines == []


def test_the_route_logs_pairing_and_merge_once_each():
    """End to end: one line each, bounded per request."""
    client, _, _ = make_client()
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS)
    assert len(lines_of(records, "bridge_pairing")) == 1
    ((_, merge),) = lines_of(records, "bridge_merge")
    assert merge["accepted"] == "1"
    assert merge["tx"] == CCTP_BURN
