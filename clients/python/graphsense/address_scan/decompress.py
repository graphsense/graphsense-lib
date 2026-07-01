# AUTO-GENERATED — DO NOT EDIT.
# Synced from src/graphsenselib/convert/address_scan/decompress.py via
# clients/python/scripts/sync_address_scan.py. Edit the source and re-run
# `make -C clients/python sync-address-scan`.
"""Transparently unwrap compressed containers before scanning.

Addresses often hide inside compressed data (.gz dumps, zip exports, or raw
zlib/gzip blobs embedded in a binary). We unwrap known containers, recurse
(so .tar.gz works), and optionally carve embedded streams. A byte budget guards
against decompression bombs.

Supported: gzip, zlib, bz2, xz, zip, tar (recursively), GraphSense ``.gs`` save
files (LZW + base64 + JSON, decoded via
:func:`graphsenselib.convert.gs_files.parser.lzw_unpack`), and -- with
``carve=True`` -- zlib/gzip streams embedded anywhere in a binary.
"""

from __future__ import annotations

import base64
import bz2
import io
import lzma
import re
import struct
import tarfile
import zipfile
import zlib
from typing import Iterator

from graphsense.gs_files.parser import lzw_unpack

MAX_DEPTH = 6
DEFAULT_BUDGET = 1024 * 1024 * 1024  # 1 GiB of decompressed output per file
GS_AUTO_MAX = 64 * 1024 * 1024  # only auto-probe .gs on smallish files


def _decode_gs(data: bytes, name: str) -> "bytes | None":
    """Return the inner JSON bytes of a GraphSense ``.gs`` file, or None.

    Headerless format, so we validate structurally: size is a non-zero multiple
    of 4, the first LZW code is a base-dictionary char (<256), and the payload
    base64-decodes to something that starts like JSON. That combination makes
    false positives on arbitrary binaries essentially nil.
    """
    if not data or len(data) % 4 != 0:
        return None
    forced = name.lower().endswith(".gs")
    if not forced and len(data) > GS_AUTO_MAX:
        return None  # avoid an expensive LZW pass on large non-.gs blobs
    codes = struct.unpack(f"<{len(data) // 4}I", data)
    if codes[0] >= 256:
        return None
    try:
        payload = base64.b64decode(lzw_unpack(list(codes)), validate=True)
    except Exception:
        return None
    return payload if payload.lstrip()[:1] in (b"[", b"{") else None


def _bounded(decompressor: object, data: bytes, limit: int) -> bytes:
    """Streaming-decompress ``data``, stopping once ``limit`` bytes are produced."""
    out = bytearray()
    step = 1 << 20
    try:
        for pos in range(0, len(data), step):
            out += decompressor.decompress(data[pos : pos + step])  # type: ignore[attr-defined]
            if len(out) >= limit:
                return bytes(out[:limit])
        flush = getattr(decompressor, "flush", None)
        if callable(flush):
            out += flush()
    except Exception:
        pass  # truncated/garbage tail -> keep whatever decoded so far
    return bytes(out[:limit])


def _is_zlib(data: bytes) -> bool:
    return (
        len(data) > 2 and (data[0] & 0x0F) == 8 and ((data[0] << 8) | data[1]) % 31 == 0
    )


def _decompress_layer(
    data: bytes, name: str, budget: list[int]
) -> Iterator[tuple[str, bytes]]:
    """Yield (label, bytes) for each container format ``data`` unwraps into."""
    if budget[0] <= 0:
        return
    limit = budget[0]

    if data[:2] == b"\x1f\x8b":  # gzip
        out = _bounded(zlib.decompressobj(zlib.MAX_WBITS | 16), data, limit)
        if out:
            budget[0] -= len(out)
            yield f"{name} :: gzip", out
    elif _is_zlib(data):  # raw zlib
        out = _bounded(zlib.decompressobj(), data, limit)
        if out:
            budget[0] -= len(out)
            yield f"{name} :: zlib", out
    elif data[:3] == b"BZh":  # bzip2
        out = _bounded(bz2.BZ2Decompressor(), data, limit)
        if out:
            budget[0] -= len(out)
            yield f"{name} :: bz2", out
    elif data[:6] == b"\xfd7zXZ\x00":  # xz
        out = _bounded(lzma.LZMADecompressor(), data, limit)
        if out:
            budget[0] -= len(out)
            yield f"{name} :: xz", out

    if data[:4] in (b"PK\x03\x04", b"PK\x05\x06"):  # zip
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                for info in zf.infolist():
                    if info.is_dir() or budget[0] <= 0:
                        continue
                    with zf.open(info) as fh:
                        out = fh.read(budget[0] + 1)[: budget[0]]
                    budget[0] -= len(out)
                    yield f"{name} :: zip[{info.filename}]", out
        except Exception:
            pass
    elif tarfile.is_tarfile(io.BytesIO(data)):  # tar (incl. plain, not gz here)
        try:
            with tarfile.open(fileobj=io.BytesIO(data)) as tf:
                for member in tf:
                    if not member.isfile() or budget[0] <= 0:
                        continue
                    fh = tf.extractfile(member)
                    if fh is None:
                        continue
                    out = fh.read(budget[0] + 1)[: budget[0]]
                    budget[0] -= len(out)
                    yield f"{name} :: tar[{member.name}]", out
        except Exception:
            pass

    gs = _decode_gs(data, name)  # GraphSense .gs (LZW + base64 + JSON)
    if gs is not None and budget[0] > 0:
        gs = gs[: budget[0]]
        budget[0] -= len(gs)
        yield f"{name} :: gs/lzw", gs


def _carve_zlib(
    data: bytes, name: str, budget: list[int]
) -> Iterator[tuple[str, bytes]]:
    """Best-effort: find gzip/zlib streams anywhere in ``data`` and inflate them."""
    seen = 0
    for m in re.finditer(rb"\x1f\x8b\x08|\x78[\x01\x9c\xda]", data):
        if budget[0] <= 0 or seen >= 200:
            break
        off = m.start()
        wbits = zlib.MAX_WBITS | 16 if data[off] == 0x1F else zlib.MAX_WBITS
        out = _bounded(zlib.decompressobj(wbits), data[off:], budget[0])
        if len(out) >= 16:  # ignore trivially short / bogus inflations
            budget[0] -= len(out)
            seen += 1
            yield f"{name} :: carved@{off}", out


def iter_streams(
    data: bytes, name: str, carve: bool, budget: list[int], depth: int = 0
) -> Iterator[tuple[str, bytes]]:
    """Yield the raw file plus every decompressed stream, recursively."""
    yield name, data
    if depth >= MAX_DEPTH:
        return
    for label, inner in _decompress_layer(data, name, budget):
        yield from iter_streams(inner, label, carve, budget, depth + 1)
    if carve:
        for label, inner in _carve_zlib(data, name, budget):
            yield from iter_streams(inner, label, carve, budget, depth + 1)
