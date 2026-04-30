# `graphsense.ext` — convenience wrapper (Python)

A hand-written layer on top of the auto-generated `graphsense` client.
Ships with the base install (no extra dependencies).

```python
from graphsense.ext import GraphSense

gs = GraphSense(api_key="...", currency="btc")

# Bundle related calls in one line
addr = gs.lookup_address(
    "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    with_tags=True, with_cluster=True, with_tag_summary=True,
)

# Full generated API at your fingertips, auto-tracks regeneration
txs = gs.raw.txs.list_address_txs("btc", "1A1z...")
```

Full reference:

- [ext/index.md](docs/ext/index.md) — quickstart, configuration
- [ext/GraphSense.md](docs/ext/GraphSense.md) — class reference
- [ext/bundlers.md](docs/ext/bundlers.md) — which `with_*` flag maps to which endpoint

## Why?

- **Less boilerplate.** No more `Configuration` → `ApiClient` → `AddressesApi`
  stair-step just to get one address.
- **Commonly-paired calls in parallel.** Tags, cluster, and tag-summary fan
  out via a small thread pool instead of serial round-trips.
- **Non-deprecated paths only.** The cluster lookup uses
  `ClustersApi.get_cluster`, not the deprecated `get_address_entity`.
- **Survives regeneration.** Lives under `graphsense/ext/` which is
  pinned in `.openapi-generator-ignore`. `.raw` is introspective, so new
  generated API groups show up automatically.
