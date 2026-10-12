"""Tagstore CLI connections must fail or recover, not hang, when the server
side of a pooled connection is gone (e.g. after a VPN outage)."""

import asyncio
import os
import socket

from sqlalchemy import text

from graphsenselib.tagpack.tagstore import TagStore
from graphsenselib.tagstore.db.database import (
    enable_tcp_keepalive,
    get_db_engine_async,
)


def _keepalive_opts(sock):
    return (
        sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE),
        sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE),
        sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL),
        sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT),
        sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT),
    )


def test_async_engine_sets_tcp_keepalive(db_setup):
    engine = enable_tcp_keepalive(
        get_db_engine_async(db_setup["db_connection_string_async"])
    )

    async def run():
        try:
            async with engine.connect() as conn:
                raw = await conn.get_raw_connection()
                sock = raw.driver_connection._transport.get_extra_info("socket")
                return _keepalive_opts(sock)
        finally:
            await engine.dispose()

    assert asyncio.run(run()) == (1, 60, 15, 4, 120_000)


def test_pre_ping_replaces_connection_killed_on_server(db_setup):
    engine = enable_tcp_keepalive(
        get_db_engine_async(
            db_setup["db_connection_string_async"], pool_pre_ping=True, pool_size=1
        )
    )

    async def run():
        try:
            async with engine.connect() as conn:
                pid = (await conn.execute(text("SELECT pg_backend_pid()"))).scalar()
            # The pooled connection's server side goes away while it is idle.
            killer = get_db_engine_async(db_setup["db_connection_string_async"])
            async with killer.connect() as k:
                await k.execute(text(f"SELECT pg_terminate_backend({pid})"))
            await killer.dispose()
            async with engine.connect() as conn:
                return pid, (
                    await conn.execute(text("SELECT pg_backend_pid()"))
                ).scalar()
        finally:
            await engine.dispose()

    old_pid, new_pid = asyncio.run(run())
    assert new_pid != old_pid


def test_sync_tagstore_sets_tcp_keepalive(db_setup):
    store = TagStore(db_setup["db_connection_string"], "public")
    try:
        sock = socket.socket(fileno=os.dup(store.conn.fileno()))
        try:
            assert _keepalive_opts(sock) == (1, 60, 15, 4, 120_000)
        finally:
            sock.close()
    finally:
        store.conn.close()
