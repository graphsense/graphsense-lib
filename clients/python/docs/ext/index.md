# graphsense.ext — convenience wrapper

A small, hand-written layer on top of the generated `graphsense` client that
removes boilerplate for the most common calls and bundles related data
(address + cluster + tag summary) in parallel.

Available as part of the base `graphsense-python` install — no extra
dependency group required.

## Install

```sh
pip install graphsense-python
```

## Quickstart

```python
from graphsense.ext import GraphSense

gs = GraphSense(api_key="...", currency="btc")

# Single, boilerplate-free lookup
addr = gs.lookup_address(
    "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    with_tags=True,
    with_cluster=True,
    with_tag_summary=True,
)
print(addr.data.address, addr.data.cluster)
print([t.label for t in addr.tags.address_tags])
print(addr.tag_summary.best_label)

# The generated API is still there for everything we don't wrap.
tx = gs.raw.txs.get_tx("btc", "a1b2...")
```

## Config resolution

In order of precedence (first match wins):

1. The explicit `api_key=` / `host=` arguments to `GraphSense(...)`.
2. Environment variables, tried in this order:
   - API key: `GRAPHSENSE_API_KEY` → `IKNAIO_API_KEY` → `GS_API_KEY` →
     `API_KEY` (legacy, for compatibility with the generator's own examples).
   - Host: `GRAPHSENSE_HOST` → `IKNAIO_HOST` → `GS_HOST`.
   - Currency (CLI only): `GS_CURRENCY`.

Fully-qualified names (`GRAPHSENSE_*`, `IKNAIO_*`) are preferred because
they are less likely to collide with other tooling in a shared shell;
`GS_*` is a shorter alias.

## Why a `Bundle`, not attribute injection?

The generated pydantic models use `validate_assignment=True` and do not set
`extra="allow"`, so attaching auxiliary data (like tags) to the model would
either be rejected or silently lost on a re-assignment. Instead, every
`lookup_*` method returns a lightweight `Bundle`:

```python
bundle.data              # the generated primary model (e.g. Address)
bundle.tags              # AddressTags or None
bundle.cluster           # Cluster or None
bundle.tag_summary       # TagSummary or None
bundle.to_dict()         # flat JSON-friendly dict of data + auxiliaries
```

## Threading model

Auxiliary calls fan out through a `ThreadPoolExecutor`. The size is
controlled via `GraphSense(max_workers=...)`. Each call reuses the same
`ApiClient` and its connection pool, which is thread-safe.

## Deprecation awareness

When the server returns the RFC 8594 `Deprecation` / `Sunset` headers, a
one-shot warning is written to `stderr` per (method + path). Pass
`quiet_deprecation=True` to silence it.

## See also

- [`GraphSense` reference](GraphSense.md)
- [`lookup_*` bundler matrix](bundlers.md)
- [CLI guide](../cli/index.md) — uses this layer under the hood
