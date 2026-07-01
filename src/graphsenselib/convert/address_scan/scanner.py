"""Scan streams for crypto addresses and build a structured report.

The report is a plain dict (JSON-serialisable); the CLI renders it either as
JSON or human-readable text. Keeping report-building separate from rendering
makes both the ``--json`` output and the tests trivial.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict

from .decompress import iter_streams
from .detectors import DETECTORS, TX_DETECTORS


def scan(
    text: str, tx_hashes: bool = False
) -> tuple[dict[str, Counter], dict[str, Counter]]:
    """Return (validated, rejected) counters keyed by detector label.

    ``rejected`` holds regex matches dropped by the checksum/heuristic validator
    (the bulk of them in a DB dump: hashes, ids, hex blobs). With ``tx_hashes``,
    format-level (unverifiable) tx-hash candidates are added too.
    """
    detectors = DETECTORS + (TX_DETECTORS if tx_hashes else [])
    found: dict[str, Counter] = defaultdict(Counter)
    rejected: dict[str, Counter] = defaultdict(Counter)
    for label, pattern, validator in detectors:
        for match in re.findall(pattern, text):
            if validator is None or validator(match):
                found[label][match] += 1
            else:
                rejected[label][match] += 1
    return found, rejected


def context_lines(text: str, addr: str, width: int = 50) -> str:
    i = text.find(addr)
    snippet = text[max(0, i - width) : i + len(addr) + width]
    return snippet.replace("\n", " ").replace("\r", " ")


def _group_obj(counter: Counter, text: str, limit: int, context: bool) -> dict:
    items = []
    for value, n in counter.most_common(limit or None):
        entry = {"value": value, "count": n}
        if context:
            entry["context"] = context_lines(text, value)
        items.append(entry)
    return {
        "unique": len(counter),
        "occurrences": sum(counter.values()),
        "shown": len(items),
        "items": items,
    }


def build_report(
    paths: list[str],
    *,
    decompress: bool = True,
    carve: bool = False,
    max_decompressed_mb: int = 1024,
    tx_hashes: bool = False,
    context: bool = False,
    hide_rejected: bool = False,
    rejected_limit: int = 20,
) -> dict:
    """Scan every path and return a JSON-serialisable report dict."""
    global_seen: set[str] = set()  # distinct addresses across all files/streams
    global_tx: set[str] = set()  # distinct tx-hash candidates
    report: dict = {"files": []}

    for path in paths:
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError as exc:
            report["files"].append({"path": path, "error": str(exc)})
            continue

        if not decompress:
            streams = [(path, raw)]
        else:
            budget = [max_decompressed_mb * 1024 * 1024]
            streams = list(iter_streams(raw, path, carve, budget))

        file_entry: dict = {
            "path": path,
            "bytes": len(raw),
            "stream_count": len(streams),
            "streams": [],
        }
        for label, sdata in streams:
            # latin-1 maps every byte 0x00-0xFF to one code point, losslessly.
            # ASCII runs (all address encodings are ASCII) survive intact, and
            # non-text bytes become non-matching separators -> works on binaries.
            text = sdata.decode("latin-1")
            results, rejected = scan(text, tx_hashes=tx_hashes)
            for kind, counter in results.items():
                target = global_tx if kind.startswith("TX-hash") else global_seen
                target.update(counter)
            if not results and (hide_rejected or not rejected):
                continue  # keep decompressed-but-empty streams quiet

            stream_obj: dict = {
                "stream": label,
                "bytes": len(sdata),
                "validated": {
                    k: _group_obj(c, text, 0, context) for k, c in results.items()
                },
            }
            if not hide_rejected and rejected:
                stream_obj["rejected"] = {
                    k: _group_obj(c, text, rejected_limit, context)
                    for k, c in rejected.items()
                }
            file_entry["streams"].append(stream_obj)
        report["files"].append(file_entry)

    report["summary"] = {
        "unique_valid_addresses": len(global_seen),
        "unique_tx_hash_candidates": len(global_tx),
    }
    return report


def render_text(report: dict, echo) -> None:
    """Render ``report`` as human-readable text via the ``echo`` callable."""

    def print_items(group: dict) -> None:
        for it in group["items"]:
            echo(f"    {it['value']}  x{it['count']}")
            if "context" in it:
                echo(f"        … {it['context']} …")
        hidden = group["unique"] - group["shown"]
        if hidden > 0:
            echo(f"    … {hidden} more (raise --rejected-limit to see them)")

    for f in report["files"]:
        if "error" in f:
            continue  # errors are reported to stderr by the caller
        echo(
            f"\n=== {f['path']} ({f['bytes']:,} bytes, {f['stream_count']} stream(s)) ==="
        )
        for stream in f["streams"]:
            if stream["stream"] != f["path"]:
                echo(
                    f"\n  ── stream: {stream['stream']} ({stream['bytes']:,} bytes) ──"
                )
            for kind, group in stream["validated"].items():
                echo(
                    f"\n  [{kind}] {group['unique']} unique / "
                    f"{group['occurrences']} occurrences"
                )
                print_items(group)
            if "rejected" in stream:
                echo("\n  --- rejected candidates (failed checksum/heuristic) ---")
                for kind, group in stream["rejected"].items():
                    echo(
                        f"\n  [{kind}] {group['unique']} unique rejected / "
                        f"{group['occurrences']} occurrences"
                    )
                    print_items(group)
        if not any(s["validated"] for s in f["streams"]):
            echo("  no valid crypto addresses found")

    summary = report["summary"]
    echo(
        f"\nTotal unique valid addresses across all files: "
        f"{summary['unique_valid_addresses']}"
    )
    if summary["unique_tx_hash_candidates"]:
        echo(
            f"Total unique tx-hash candidates (unverified): "
            f"{summary['unique_tx_hash_candidates']}"
        )
