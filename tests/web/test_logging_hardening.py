"""The logging of the bridge pairing must never change an answer, never
leak a credential and never break a line (round-4 review of
middleware/external_backends.py, web/service/txs_service.py, web/app.py):

- a log handler that raises on the pairing, merge or request-id records
  leaves the route's answer as it was;
- a 501 body that cannot be read for its ``reason`` (too big, too deep,
  unreadable) is ``http_501``, never a failed request;
- request ids and reason codes match whole (no trailing newline), a
  backend's ``X-Request-Id`` is validated before it is logged, and logfmt
  escapes control characters so a value stays on one line;
- every line that names a backend names its origin: no userinfo, no query;
- the bridge readers' skipped-burn lines carry the request id too.
"""

import contextlib
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from graphsenselib.db.asynchronous.services.models import ExternalConversion
from graphsenselib.db.asynchronous.services.txs_service import TxConversions
from graphsenselib.web.app import setup_logging
from graphsenselib.web.config import LoggingConfig
from graphsenselib.web.middleware.external_backends import (
    DECLINED_HEADER,
    REQUEST_ID_HEADER,
    RequestIdFilter,
    RequestIdMiddleware,
    _refusal_reason,
    logfmt,
    request_id_var,
)
from graphsenselib.web.service.txs_service import get_tx_conversions

from tests.web.test_bridge_pairing_logging import (
    lines_of,
    the_pairing_line,
)
from tests.web.test_delegated_conversions import (
    ARB_MINT,
    BRIDGE_ROW,
    CCTP_BURN,
    CONVERSIONS,
    HALF_ROW,
    LOCAL_ROW,
    local_swap,  # noqa: F401 -- the autouse fixture of that module
    make_client,
    records_of_the_middleware,
)

BACKENDS_LOGGER = "graphsenselib.web.middleware.external_backends"
BRIDGING_LOGGER = "graphsenselib.defi.bridging"
DELEGATED_LOGGER = "graphsenselib.defi.bridging.delegated"
SECRET = "TEST_SECRET"
CREDENTIALED_URL = f"https://user:{SECRET}@backend.test/?api_key={SECRET}"


@contextlib.contextmanager
def a_handler_that_raises_on(*markers, logger_name=BACKENDS_LOGGER):
    """Records go through; one whose message names a marker raises."""
    records = []

    class Faulty(logging.Handler):
        def emit(self, record):
            message = record.getMessage()
            if any(marker in message for marker in markers):
                raise RuntimeError(f"handler failed on {message[:40]}")
            records.append(record)

    logger = logging.getLogger(logger_name)
    handler = Faulty(level=logging.INFO)
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.INFO)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


# ---------------------------------------------------------------------------
# 1. a failing log call never changes an answer
# ---------------------------------------------------------------------------


def test_a_handler_failing_on_the_pairing_line_leaves_a_paired_answer():
    client, seen, _ = make_client()
    with a_handler_that_raises_on("event=bridge_pairing"):
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]
    assert len(seen) == 1


def test_a_handler_failing_on_the_pairing_line_leaves_a_refused_answer():
    answer = httpx.Response(501, json={"reason": "mint_not_found"})
    client, _, _ = make_client(backend_answer=answer)
    with a_handler_that_raises_on("event=bridge_pairing"):
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert DECLINED_HEADER not in response.headers


def test_a_handler_failing_on_the_merge_line_leaves_the_route_answer():
    client, _, _ = make_client()
    with a_handler_that_raises_on("event=bridge_merge"):
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]


def test_a_handler_failing_on_both_lines_leaves_the_route_answer():
    client, _, _ = make_client()
    with a_handler_that_raises_on("event=bridge_pairing", "event=bridge_merge"):
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]


def merge_context(result):
    return SimpleNamespace(
        services=SimpleNamespace(
            txs_service=SimpleNamespace(get_conversions=AsyncMock(return_value=result))
        ),
        config=SimpleNamespace(included_bridges=("cctp",), cow_protocol_swaps=False),
    )


@pytest.mark.asyncio
async def test_a_handler_failing_on_the_merge_line_leaves_the_merge():
    result = TxConversions(
        (), (ExternalConversion(**{**HALF_ROW, "conversion_type": "bridge"}),)
    )
    pairing = AsyncMock(return_value=[ExternalConversion(**BRIDGE_ROW)])
    with a_handler_that_raises_on("event=bridge_merge"):
        answer = await get_tx_conversions(
            merge_context(result), "eth", CCTP_BURN, pairing
        )
    assert [r.to_dict() for r in answer] == [BRIDGE_ROW]


@pytest.mark.asyncio
async def test_the_merges_own_exception_still_passes_through():
    """Containing the log must not contain the route's own failure."""
    result = TxConversions(
        (), (ExternalConversion(**{**HALF_ROW, "conversion_type": "bridge"}),)
    )
    pairing = AsyncMock(side_effect=ValueError("the pairing itself failed"))
    with a_handler_that_raises_on("event=bridge_merge"):
        with pytest.raises(ValueError, match="the pairing itself failed"):
            await get_tx_conversions(merge_context(result), "eth", CCTP_BURN, pairing)


def records_through(filter_, message, *args):
    logger = logging.getLogger("tests.logging_hardening.filter")
    logger.propagate = False
    logger.setLevel(logging.INFO)
    records = []

    class Sink(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger.addHandler(Sink())
    logger.addFilter(filter_)
    try:
        logger.info(message, *args)
    finally:
        logger.removeFilter(filter_)
        logger.handlers.clear()
    return records


def test_the_request_id_filter_contains_a_format_error():
    """A message that does not format (``%d`` of a string) used to raise out
    of the filter into the request handler; it passes now and the handler
    reports it the usual way."""
    token = request_id_var.set("req-1")
    try:
        (record,) = records_through(RequestIdFilter(), "backend %d is down", "x")
    finally:
        request_id_var.reset(token)
    assert record.request_id == "req-1"


def test_the_request_id_filter_contains_a_failing_message_object():
    """getMessage() itself failing (pytest's capture handler would re-raise
    a format error of its own, so the filter is called directly here)."""

    class Broken:
        def __str__(self):
            raise RuntimeError("cannot render")

    record = logging.LogRecord("x", logging.INFO, __file__, 1, Broken(), (), None)
    token = request_id_var.set("req-1")
    try:
        assert RequestIdFilter().filter(record) is True
    finally:
        request_id_var.reset(token)
    assert record.request_id == "req-1"


# ---------------------------------------------------------------------------
# 2. reading a 501 body for its reason cannot fail the request
# ---------------------------------------------------------------------------


def test_a_deeply_nested_501_body_leaves_the_answer_unchanged():
    depth = 10_000
    answer = httpx.Response(
        501,
        content=b"[" * depth + b"]" * depth,
        headers={"Content-Type": "application/json"},
    )
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    assert DECLINED_HEADER not in response.headers
    _, fields = the_pairing_line(records)
    assert fields["reason"] == "http_501"


def test_a_501_body_over_64_kb_is_not_parsed():
    padding = "x" * 70_000
    answer = httpx.Response(501, json={"reason": "mint_not_found", "pad": padding})
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    _, fields = the_pairing_line(records)
    assert fields["reason"] == "http_501"


def test_an_unreadable_501_body_is_http_501():
    class Unreadable(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise httpx.ReadError("gone")

    response = httpx.Response(501, stream=Unreadable())
    assert _refusal_reason(response) == "http_501"


def test_a_reason_that_fails_in_an_unexpected_way_is_http_501(monkeypatch):
    class Explodes(httpx.Response):
        def json(self, **kwargs):
            raise RecursionError("maximum recursion depth exceeded")

    response = Explodes(501, json={"reason": "mint_not_found"})
    assert _refusal_reason(response) == "http_501"


# ---------------------------------------------------------------------------
# 3. validation and escaping
# ---------------------------------------------------------------------------


async def raw_request(app, header_value: bytes):
    """One raw ASGI GET with ``X-Request-Id: header_value``, as a gateway
    could send it: no client library to reject the value."""
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/ping",
        "raw_path": b"/ping",
        "query_string": b"",
        "headers": [(b"x-request-id", header_value)],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    }
    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    return dict(start["headers"])


@pytest.mark.asyncio
@pytest.mark.parametrize("incoming", [b"valid-id\n", b"valid-id\r", b"valid-id\r\n"])
async def test_an_id_with_a_trailing_newline_is_replaced(incoming):
    seen = []

    async def app(scope, receive, send):
        seen.append(request_id_var.get())
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    headers = await raw_request(RequestIdMiddleware(app), incoming)
    echoed = headers[REQUEST_ID_HEADER.lower().encode()]
    assert echoed != incoming
    assert b"\n" not in echoed and b"\r" not in echoed
    assert seen == [echoed.decode()]


@pytest.mark.parametrize("reason", ["mint_not_found\n", "mint_not_found\r\n"])
def test_a_reason_code_with_a_trailing_newline_is_rejected(reason):
    response = httpx.Response(501, json={"reason": reason})
    assert _refusal_reason(response) == "http_501"


@pytest.mark.parametrize("backend_id", ["bad id", "x" * 65, "a=b", ""])
def test_a_malformed_backend_request_id_is_not_logged(backend_id):
    answer = httpx.Response(
        501, json={"reason": "mint_not_found"}, headers={"X-Request-Id": backend_id}
    )
    client, _, _ = make_client(backend_answer=answer)
    with records_of_the_middleware() as records:
        client.get(CONVERSIONS)
    _, fields = the_pairing_line(records)
    assert "backend_request_id" not in fields


def test_logfmt_escapes_control_characters():
    line = logfmt({"a": "x\ny", "b": "t\tz\rq", "c": "\x01", "d": "plain"})
    assert line == 'a="x\\ny" b="t\\tz\\rq" c="\\x01" d=plain'
    assert "\n" not in line and "\r" not in line and "\t" not in line


def test_logfmt_keeps_the_existing_quoting():
    assert logfmt({"a": 'say "hi"', "b": "k=v", "c": "back\\slash", "d": ""}) == (
        'a="say \\"hi\\"" b="k=v" c="back\\\\slash" d=""'
    )


@pytest.mark.asyncio
async def test_a_newline_in_the_merged_tx_stays_on_one_line():
    """The local lookup ignores whitespace in a hash, so a control
    character in the path can reach the merge; its line must not split."""
    result = TxConversions(
        (), (ExternalConversion(**{**HALF_ROW, "conversion_type": "bridge"}),)
    )
    tx_with_newline = f"{CCTP_BURN}\n_T1"
    with records_of_the_middleware() as records:
        await get_tx_conversions(merge_context(result), "eth", tx_with_newline, None)
    ((record, fields),) = lines_of(records, "bridge_merge")
    message = record.getMessage()
    assert "\n" not in message
    assert fields["tx"] == f"{CCTP_BURN}\\n_T1"


# ---------------------------------------------------------------------------
# 4. no credentials in any log record
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def every_record_of(logger_name=BACKENDS_LOGGER):
    """All records of the logger, at every level."""
    records = []

    class Sink(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger(logger_name)
    handler = Sink(level=logging.DEBUG)
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.DEBUG)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


def everything_in(record: logging.LogRecord) -> str:
    """The message and every attribute of a record, as a handler could
    print them."""
    parts = [record.getMessage(), str(record.msg), repr(record.args)]
    parts += [repr(v) for k, v in record.__dict__.items() if k not in ("msg", "args")]
    return "\n".join(parts)


def test_no_pairing_record_carries_the_backends_credentials():
    script = {
        "step": 0,
        "answers": [
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(500, json={"detail": "boom"}),
            httpx.ConnectError(f"refused: {CREDENTIALED_URL} with {SECRET}"),
            httpx.RemoteProtocolError("Server disconnected"),
            httpx.Response(200, json=[BRIDGE_ROW]),
        ],
    }

    def backend(request):
        answer = script["answers"][min(script["step"], len(script["answers"]) - 1)]
        script["step"] += 1
        if isinstance(answer, Exception):
            raise answer
        return answer

    client, seen, _ = make_client(
        delegated_conversions={"eth": {"url": CREDENTIALED_URL, "api_key": "key-9"}},
        backend_answer=backend,
    )
    with every_record_of() as records:
        answers = []
        # 429, 5xx (the outage alert), refused, then a dropped connection
        # whose retry answers (recovery), then a plain answer
        for _ in range(5):
            answers.append(client.get(CONVERSIONS))
    assert [a.status_code for a in answers] == [200] * 5
    assert [a.json() for a in answers[:3]] == [[LOCAL_ROW, HALF_ROW]] * 3
    assert [a.json() for a in answers[3:]] == [[LOCAL_ROW, BRIDGE_ROW]] * 2
    levels = {r.levelno for r in records}
    assert logging.ERROR in levels and logging.WARNING in levels
    assert any("sending it again" in r.getMessage() for r in records)
    assert any("answers again" in r.getMessage() for r in records)
    # the exception's text is never written, only its type
    assert not any("refused:" in r.getMessage() for r in records)
    (refused,) = [r for r in records if "still not answering" in r.getMessage()]
    assert "(ConnectError)" in refused.getMessage()
    assert len(records) >= 8
    for record in records:
        text = everything_in(record)
        assert SECRET not in text, text
        assert "key-9" not in text, text
        assert "api_key" not in text, text
        assert "user:" not in text, text
    assert all(
        "https://backend.test" in r.getMessage()
        for r in records
        if "backend" in r.getMessage()
    )
    for answer in answers[:3]:
        assert answer.headers[DECLINED_HEADER] == "https://backend.test"
    for answer in answers[3:]:
        assert DECLINED_HEADER not in answer.headers


def test_no_proxy_or_merge_record_carries_the_backends_credentials():
    from fastapi import FastAPI
    from starlette.responses import Response
    from starlette.testclient import TestClient

    from graphsenselib.web.config import ExternalBackendsConfig
    from graphsenselib.web.middleware.external_backends import (
        ExternalBackendMiddleware,
    )

    app = FastAPI()

    @app.get("/stats")
    async def stats():
        return Response(content=b'{"currencies": []}', media_type="application/json")

    state = {"down": True}

    def backend(request):
        if state["down"]:
            raise httpx.ConnectError("refused")
        return httpx.Response(200, json={"currencies": []})

    config = ExternalBackendsConfig(
        enabled=True,
        networks={"arb": {"url": CREDENTIALED_URL, "api_key": "key-9"}},
        failure_skip_s=0,
    )
    app.add_middleware(
        ExternalBackendMiddleware,
        config=config,
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    client = TestClient(app)
    with every_record_of() as records:
        proxied = client.get(f"/arb/txs/{ARB_MINT}")
        merged = client.get("/stats")
        state["down"] = False
        recovered = client.get("/stats")
    assert proxied.status_code == 503
    assert merged.status_code == 200
    assert merged.headers[DECLINED_HEADER] == "https://backend.test"
    assert recovered.status_code == 200
    assert any("answers again" in r.getMessage() for r in records)
    assert records
    for record in records:
        text = everything_in(record)
        assert SECRET not in text, text
        assert "key-9" not in text, text


QUERY_SECRET = "QUERY_ONLY_SECRET"
# a credential in the query only: no password whose redaction could shadow it
QUERY_ONLY_URL = f"https://backend.test/?api_key={QUERY_SECRET}"


def test_no_pairing_record_carries_a_query_only_credential_an_exception_quotes():
    """An exception's text is the transport's, and may quote the URL or its
    credential: no line carries it, only the exception's type, the origin
    and the path without its query."""
    script = {
        "step": 0,
        "answers": [
            httpx.ConnectError(f"refused token {QUERY_SECRET}"),
            httpx.ConnectError(f"still refusing token {QUERY_SECRET}"),
            httpx.RemoteProtocolError(f"Server disconnected, token {QUERY_SECRET}"),
            httpx.Response(200, json=[BRIDGE_ROW]),
        ],
    }

    def backend(request):
        answer = script["answers"][min(script["step"], len(script["answers"]) - 1)]
        script["step"] += 1
        if isinstance(answer, Exception):
            raise answer
        return answer

    client, _, _ = make_client(
        delegated_conversions={"eth": {"url": QUERY_ONLY_URL}},
        backend_answer=backend,
    )
    with every_record_of() as records:
        # refused (the outage alert), refused again (still down), then a
        # dropped connection whose retry answers (recovery)
        answers = [client.get(CONVERSIONS) for _ in range(3)]
    assert [a.status_code for a in answers] == [200] * 3
    assert [a.json() for a in answers] == [[LOCAL_ROW, HALF_ROW]] * 2 + [
        [LOCAL_ROW, BRIDGE_ROW]
    ]
    (alert,) = [r for r in records if r.levelno == logging.ERROR]
    (still_down,) = [r for r in records if "still not answering" in r.getMessage()]
    (retry,) = [r for r in records if "sending it again" in r.getMessage()]
    for record, type_name in (
        (alert, "ConnectError"),
        (still_down, "ConnectError"),
        (retry, "RemoteProtocolError"),
    ):
        message = record.getMessage()
        assert type_name in message, message
        assert "https://backend.test" in message, message
        assert "token" not in message, message
    # the path is named without its query
    assert f"/eth/txs/{CCTP_BURN}/conversions" in alert.getMessage()
    assert "only_bridges" not in alert.getMessage()
    assert len(records) >= 5
    for record in records:
        text = everything_in(record)
        assert QUERY_SECRET not in text, text
        assert "api_key" not in text, text


def test_no_proxy_or_merge_record_carries_a_query_only_credential_an_exception_quotes():
    from fastapi import FastAPI
    from starlette.responses import Response
    from starlette.testclient import TestClient

    from graphsenselib.web.config import ExternalBackendsConfig
    from graphsenselib.web.middleware.external_backends import (
        ExternalBackendMiddleware,
    )

    app = FastAPI()

    @app.get("/stats")
    async def stats():
        return Response(content=b'{"currencies": []}', media_type="application/json")

    def backend(request):
        raise httpx.ConnectError(f"refused token {QUERY_SECRET} at {request.url}")

    config = ExternalBackendsConfig(
        enabled=True,
        networks={"arb": {"url": QUERY_ONLY_URL}},
        failure_skip_s=0,
    )
    app.add_middleware(
        ExternalBackendMiddleware,
        config=config,
        client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    client = TestClient(app)
    with every_record_of() as records:
        proxied = client.get(f"/arb/txs/{ARB_MINT}")
        merged = client.get("/stats")
    assert proxied.status_code == 503
    assert merged.status_code == 200
    assert merged.headers[DECLINED_HEADER] == "https://backend.test"
    failures = [r for r in records if r.levelno >= logging.WARNING]
    assert len(failures) == 2
    for record in failures:
        assert "ConnectError" in record.getMessage()
        assert "https://backend.test" in record.getMessage()
    assert f"GET /arb/txs/{ARB_MINT}" in failures[0].getMessage()
    for record in records:
        text = everything_in(record)
        assert QUERY_SECRET not in text, text
        assert "api_key" not in text, text
        assert "token" not in text, text


def test_a_malformed_rows_warning_names_the_origin_only():
    answer = httpx.Response(200, json=[{"conversion_type": "bridge_tx"}])
    client, _, _ = make_client(
        delegated_conversions={"eth": {"url": CREDENTIALED_URL}},
        backend_answer=answer,
    )
    with every_record_of() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    (warning,) = [r for r in records if "malformed" in r.getMessage()]
    assert "https://backend.test" in warning.getMessage()
    assert SECRET not in everything_in(warning)


# ---------------------------------------------------------------------------
# 5. the request id on the bridge readers' lines too
# ---------------------------------------------------------------------------


def logging_set_up(monkeypatch, times=1):
    """setup_logging as the app runs it, its side effects undone after."""
    for name in (
        "graphsenselib.web.app",
        BACKENDS_LOGGER,
        BRIDGING_LOGGER,
        DELEGATED_LOGGER,
        "asyncio",
        "uvicorn",
    ):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "level", logger.level)
        monkeypatch.setattr(logger, "filters", list(logger.filters))
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: None)
    monkeypatch.setattr(logging.root, "handlers", [])
    app_logger = logging.getLogger("graphsenselib.web.app")
    for _ in range(times):
        setup_logging(app_logger, None, "test", LoggingConfig(level="INFO"))


def test_setup_logging_puts_the_request_id_filter_on_the_bridge_readers(monkeypatch):
    """On the logger of the module that logs the skipped-burn lines: a
    logger's filters apply to its own records only, not to a child's that
    propagate through it."""
    logging_set_up(monkeypatch, times=2)
    reader_logger = logging.getLogger(DELEGATED_LOGGER)
    installed = [f for f in reader_logger.filters if isinstance(f, RequestIdFilter)]
    assert len(installed) == 1


def test_a_skipped_burn_line_carries_the_request_id(monkeypatch):
    """Through the real reader: an unfunded burn logged under a request id
    joins the request's other lines."""
    from graphsenselib.defi.bridging.delegated import CCTP, half_bridges

    from tests.defi.test_delegated_bridges import FIXTURE, db_rows

    logging_set_up(monkeypatch)
    reader_logger = logging.getLogger(DELEGATED_LOGGER)
    records = []

    class Sink(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = Sink(level=logging.INFO)
    reader_logger.addHandler(handler)
    unfunded = [log for log in FIXTURE["v2_burn"]["logs"] if log["log_index"] != 694]
    token = request_id_var.set("req-7")
    try:
        assert half_bridges("eth", db_rows("v2_burn", unfunded), CCTP) == ()
    finally:
        request_id_var.reset(token)
        reader_logger.removeHandler(handler)
    (record,) = [r for r in records if "event=cctp_half_skipped" in r.getMessage()]
    assert record.getMessage().endswith("request_id=req-7")


@pytest.mark.parametrize(
    "body",
    [b"\xff" + QUERY_SECRET.encode(), b'{"rows": "' + QUERY_SECRET.encode()],
    ids=["malformed utf-8", "truncated json"],
)
def test_no_record_quotes_an_unreadable_body(body):
    """A decoding error quotes the bytes it could not read, and a backend's
    body may hold anything: only the error's type is logged."""
    client, _, _ = make_client(
        delegated_conversions={"eth": {"url": QUERY_ONLY_URL}},
        backend_answer=httpx.Response(200, content=body),
    )
    with every_record_of() as records:
        answer = client.get(CONVERSIONS)
    assert answer.json() == [LOCAL_ROW, HALF_ROW]
    assert records
    for record in records:
        assert QUERY_SECRET not in record.getMessage()
        assert QUERY_SECRET not in str(record.__dict__)
