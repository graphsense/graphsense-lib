import json
import logging

import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from graphsenselib.web.app import create_app
from graphsenselib.web.config import GSRestConfig


class HTTPClientShim:
    """Shim providing async-style API for httpx.AsyncClient.

    Provides await-based response methods for backward compatibility with existing tests.
    """

    def __init__(self, httpx_client: AsyncClient):
        self._client = httpx_client

    async def request(
        self, path: str = None, method: str = "GET", json=None, headers=None, **kwargs
    ):
        url = path or kwargs.get("url", "/")
        response = await self._client.request(
            method=method, url=url, json=json, headers=headers
        )
        return HTTPResponseShim(response)

    async def get(self, path: str, headers=None, **kwargs):
        return await self.request(path=path, method="GET", headers=headers, **kwargs)

    async def post(self, path: str, json=None, headers=None, **kwargs):
        return await self.request(
            path=path, method="POST", json=json, headers=headers, **kwargs
        )


class HTTPResponseShim:
    """Shim providing async-style API for httpx.Response."""

    def __init__(self, httpx_response):
        self._response = httpx_response

    @property
    def status(self) -> int:
        return self._response.status_code

    async def read(self) -> bytes:
        return self._response.content

    async def text(self) -> str:
        return self._response.text

    async def json(self):
        return self._response.json()

    @property
    def headers(self):
        return self._response.headers


class BaseTestCase:
    """Base test case for FastAPI tests using httpx."""

    config: dict = None  # Set by conftest.py
    app = None  # Will be set during setup

    @pytest_asyncio.fixture(autouse=True)
    async def setup_client(self):
        """Set up the test client before each test."""
        logging.getLogger("uvicorn.error").setLevel("ERROR")
        logging.getLogger("uvicorn.access").setLevel("ERROR")

        # Create FastAPI app with the test config
        fastapi_app = create_app(
            config=GSRestConfig.from_dict(self.config),
            validate_responses=True,
        )

        # Use LifespanManager to properly trigger startup/shutdown events
        async with LifespanManager(fastapi_app) as manager:
            # Store app state for test access
            self.app_state = fastapi_app.state
            self._fastapi_app = fastapi_app

            transport = ASGITransport(app=manager.app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as httpx_client:
                self.client = HTTPClientShim(httpx_client)
                self._httpx_client = (
                    httpx_client  # Keep reference for direct access if needed
                )
                yield

    async def requestOnly(self, path, body, **kwargs):
        headers = {
            "Accept": "application/json",
            "Authorization": kwargs.get("auth", "x"),
        }
        response = await self.client.request(
            path=path.format(**kwargs),
            method="GET" if body is None else "POST",
            json=body,
            headers=headers,
        )
        return (response, (await response.read()).decode("utf-8"))

    async def requestWithCodeAndBody(self, path, code, body, **kwargs):
        headers = {
            "Accept": "application/json",
            "Authorization": kwargs.get("auth", "x"),
        }
        response = await self.client.request(
            path=path.format(**kwargs),
            method="GET" if body is None else "POST",
            json=body,
            headers=headers,
        )
        content = (await response.read()).decode("utf-8")
        self.assertEqual(code, response.status, "response is " + content)
        if code != 200:
            return
        return json.loads(content)

    def request(self, path, **kwargs):
        return self.requestWithCodeAndBody(path, 200, None, **kwargs)

    def assertEqual(self, a, b, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert a == b, msg
        else:
            assert a == b

    def assertNotEqual(self, a, b, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert a != b, msg
        else:
            assert a != b

    def assertTrue(self, x, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert x, msg
        else:
            assert x

    def assertFalse(self, x, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert not x, msg
        else:
            assert not x

    def assertIsNone(self, x, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert x is None, msg
        else:
            assert x is None

    def assertIsNotNone(self, x, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert x is not None, msg
        else:
            assert x is not None

    def assertIn(self, a, b, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert a in b, msg
        else:
            assert a in b

    def assertNotIn(self, a, b, msg=None):
        """Backward compatibility with unittest-style assertions."""
        if msg:
            assert a not in b, msg
        else:
            assert a not in b

    def assertEqualWithList(self, a, b, *keys):
        keys = iter(keys)
        key = next(keys)
        pa = a
        pb = b
        aa = a[key]
        bb = b[key]
        while not isinstance(aa, list):
            key = next(keys)
            pa = aa
            pb = bb
            aa = aa[key]
            bb = bb[key]
        listkey = next(keys)

        def fun(x):
            return x[listkey]

        pa[key] = sorted(pa[key], key=fun)
        pb[key] = sorted(pb[key], key=fun)

        assert a == b
