# graphsense MCP

This submodule exposes a curated subset of the graphsense REST API as a
[Model Context Protocol](https://modelcontextprotocol.io) server so LLM
clients (Claude Code, Claude Desktop, Cursor, custom agents) can query
graphsense directly. It also forwards requests to the proprietary external
`search_neighbors` service so consumers have a single endpoint.

## Deployment model

The MCP is **mounted inside the existing FastAPI app** — there is no
separate `mcp serve` process. Run the usual web stack and `/mcp` is there:

```bash
uv run uvicorn --factory graphsenselib.web.app:create_app --port 8000
# REST at /, MCP at /mcp
```

Auto-attach happens in `create_app`, `create_app_from_dict`, and
`create_spec_app` at the end of their setup, via `_maybe_attach_mcp`. If
the `[mcp]` extra isn't installed, the import fails silently and the REST
app continues as normal. Set `GS_MCP_ENABLED=false` to disable the mount
explicitly.

**Transport**: streamable-http, `stateless_http=True` by default. Stateful
mode holds SSE long-polls open and makes uvicorn hang on shutdown; none of
our tools need server-initiated push notifications, so stateless is the
right default. Flip with `GS_MCP_STATELESS_HTTP=false` if a future tool
needs it.

## Design principles

Every tool must earn its place by either **being structurally distinct**
or by **collapsing a common chain**. Two corollaries:

1. **Curate, don't auto-expose.** FastAPI has 44 routes; we surface 16
   (17 when `search_neighbors` is configured). MCP tool schemas are
   loaded into the LLM's tool-selection context in some clients; even
   where clients lazy-load (Claude Code does), a tighter surface reduces
   "which-tool-should-I-pick?" ambiguity.
2. **Consolidate when endpoints are always chained.** If an LLM needs
   four round-trips to answer a common question, merge them into one
   tool that returns the merged JSON. The win is round-trips, not tokens.
3. **Don't consolidate what's merely similar.** Over-consolidation
   hides legitimate optionality. We kept `lookup_tx_details` and `list_tx_flows`
   distinct because the LLM has real reasons to choose one over the
   other (UTXO vs account-model semantics).

## Curation mechanism

The positive-list YAML at `curation/tools.yaml` is the source of truth.
Anything not listed is excluded.

Three categories:

- `include:` — **passthroughs**. Keyed by FastAPI `operation_id`. The
  handler is auto-generated from the OpenAPI schema by
  `FastMCP.from_fastapi`. An optional `description` override replaces the
  FastAPI docstring with LLM-tuned wording; an optional `tags:` list is
  added as MCP tags (with `gs_` prefix).
- `consolidated_tools:` — **hand-written** `@mcp.tool` wrappers. Their
  `replaces:` list hides the underlying op_ids from auto-exposure.
- `external_tools:` — forward to a different HTTP service. Currently
  just `search_neighbors`.

Validation runs at boot via `validate_against_app`:

- Every `include` key must exist on the FastAPI app.
- Every `replaces` entry must exist on the FastAPI app.
- No op_id may appear in both `include` and a consolidated
  `replaces` list (consolidation supersedes passthrough).

CI gate: `uv run graphsense-cli mcp validate-curation` exits non-zero on
drift.

## Response shape conventions

All consolidated-tool responses go through a `_slim()` transform before
returning (see `tools/consolidated.py`). It rewrites graphsense's
`{"fiat_values": [{"code":"eur","value":X}, ...], "value": N}` money
objects into a flat `{"native": N, "eur": X, "usd": Y, ...}` shape,
eliminating the repeated `code` / `value` keys and the array wrapper.
Typical savings: ~40% of the response body for address / tx lookups
where most fields are value conversions. fastmcp already serializes
dicts to compact JSON (no whitespace), so that lever is free.

## The 16-tool surface (17 when `search_neighbors` is configured)

### Orientation (5)

Small, cheap passthroughs the LLM uses to figure out what's available.

| Tool | Purpose |
|---|---|
| `get_statistics` | Per-network snapshot: heights, freshness, assets |
| `search` | Cross-network free-text lookup |
| `list_supported_tokens` | Token catalog for a network |
| `list_taxonomies` + `list_concepts` | Tag vocabulary |

### Block-level (3)

Kept as separate passthroughs — each is used in a different narrative
context. `get_block_by_date` in particular is the timestamp→height
bridge.

`get_block`, `get_block_by_date`, `list_block_txs`

### Transaction-level (1 passthrough + 1 consolidation)

| Tool | Notes |
|---|---|
| `list_tx_flows` | Account-model (ETH family) internal-transfer list — kept as a passthrough because its response shape is distinct from the tx body. |
| `lookup_tx_details` | **Consolidation** — replaces `get_tx` + `get_tx_io` + `get_spending_txs` + `get_spent_in_txs` + `get_tx_conversions`. Calls `/txs/{hash}` with `include_io` / `include_nonstandard_io` / `include_io_index` always on, so UTXO txs come back with full (including non-standard) inputs/outputs and their positional indices in one shot; account-model txs come back with the usual sender/receiver/value fields. Optional `include_upstream` (backward trace) and `include_downstream` (forward trace) append trace lists. Optional `include_heuristics=True` asks graphsense to compute all UTXO heuristics (change-address detection + CoinJoin identification — wasabi/whirlpool/joinmarket variants). Optional `include_conversions=True` appends `conversions`: graphsense's internal term covering both DEX swaps and bridge txs under one unified schema (`conversion_type: "dex_swap" \| "bridge_tx"`, plus from/to address + asset + amount). ⚠️ The consolidation uses `upstream` / `downstream` names because the underlying graphsense endpoints `/spending` and `/spent_in` are named counter-intuitively (`/spending` is backward, `/spent_in` is forward). |

`list_tx_flows` is kept separate because its response shape is unrelated
to the tx body and because account-model flow analysis has different
intent than UTXO tx inspection.

### Rates / actor metadata (2)

`get_exchange_rates` and `get_actor` — low-token, high-value
passthroughs that LLMs naturally chain with others.

### Address / cluster / neighbors (4 consolidations)

Note on terminology: graphsense has both "entity" and "cluster" endpoints, but `entities_service.get_entity` literally delegates to `clusters_service.get_cluster`. They're aliases. The MCP surface exposes only **cluster** to avoid conceptual duplication — entity op_ids are still hidden via `replaces` in the curation YAML, but the word "entity" does not appear in any tool name, kwarg, or response key.

| Tool | Replaces | Why |
|---|---|---|
| `lookup_address` | `get_address` + `get_address_entity` + `get_tag_summary_by_address` + `list_related_addresses` | "Tell me about this address" is the single most common question. The tag_summary call passes `include_best_cluster_tag=true` (UI parity): when the address has no direct tag, the cluster's best tag is folded into the digest. Optional `include_cross_chain_addresses` uses the pubkey-relation endpoint to find the same address on BCH/LTC/... from a BTC lookup (and vice-versa). |
| `lookup_cluster` | `get_entity` + `get_cluster` + `list_address_tags_by_entity` + `list_address_tags_by_cluster` | Cluster-level equivalent. Tag context is intentionally NOT included — call `lookup_address`/`list_tags_by_address` on a member address. |
| `list_tags_by_address` | `list_tags_by_address` | Raw per-tag detail with `include_best_cluster_tag=true` defaulted on (UI parity) — the cluster's best tag appears on the last page if the address has no direct tag. |
| `list_neighbors` | `list_address_neighbors` | Address-level only. Cluster-level neighbors are deliberately not exposed — follow counterparty graphs at the address level (on-chain fact) rather than the cluster level (inference stacked on top). |
| `list_txs_for` | `list_address_txs` + `list_address_links` | Address-level only. Pass `neighbor=<addr>` to switch to the links endpoint (txs between two addresses). Cluster-/entity-level tx listings are deliberately not exposed — same rationale as `list_neighbors`. |

### External (1)

`search_neighbors` — forwards to the proprietary graph-search service
with async task polling. Only registered when
`GS_MCP_SEARCH_NEIGHBORS__BASE_URL` is set. API key is optional — leave
`api_key_env` unset to talk to an unauthenticated backend.

## What we deliberately don't expose

Listed because "why isn't X a tool?" is as interesting as "why is it?":

- **`bulk_csv` / `bulk_json`** — unbounded-size output, bad for LLM
  context.
- **`get_tx_conversions`** — niche bridge-tx conversions; re-add if a
  use case emerges.
- **`list_related_addresses`** — niche heuristic, adds noise.
- **`report_tag`** — write operation; LLMs shouldn't be reporting tags
  autonomously.
- **`search_cluster_neighbors` / `search_entity_neighbors`** —
  cluster-level neighbor search; redundant with the address-level
  `list_neighbors` and we deliberately keep counterparty traversal at
  the address level.
- **`list_cluster_neighbors` / `list_entity_neighbors`** — cluster-level
  neighbor listing. Address clustering is a heuristic; surfacing it as
  a first-class traversal primitive encourages the LLM to reason on
  inferred edges instead of on-chain fact. Use `list_neighbors` on an
  address instead.
- **`list_cluster_txs` / `list_entity_txs`** — cluster-level transaction
  listing. Same rationale as the cluster-neighbor exclusion: traverse
  at the address level. Use `list_txs_for` on an address instead.
- **`list_cluster_links` / `list_entity_links`** — cluster-level
  "txs between two clusters" endpoint. Not useful without a cluster-
  level counterparty graph, which we also don't expose.
  (`list_address_links` is still reachable via `list_txs_for` with
  `neighbor=<addr>`.)
- **`get_cluster`, `list_cluster_addresses`, `list_address_tags_by_cluster`** —
  at the currency level, `lookup_cluster` covers the useful surface
  without duplicating the entity/cluster mental model for the LLM.
- **`get_actor_tags`** — almost always redundant with `lookup_address` /
  `lookup_cluster` tag surfaces; re-add if it proves useful.

All filtered out at boot by absence from the positive-list. Trivial to
re-enable later.

## How to modify the surface

### Add a passthrough

1. Confirm the FastAPI `operation_id`:
   `uv run graphsense-cli web openapi | jq '.paths[][].operationId'`
2. Add an entry under `include:` in `curation/tools.yaml` with an
   LLM-tuned `description` and useful `tags:`.
3. `uv run graphsense-cli mcp validate-curation` — should stay green.

### Add a consolidation

1. Write a `register_<name>(mcp, app, stack)` function in
   `tools/consolidated.py` that declares an `@mcp.tool` and dispatches
   to the FastAPI app via `_make_client(app)` (ASGI in-process).
2. Add an entry under `consolidated_tools:` in the YAML with `name`,
   `replaces`, and `module`.
3. Make sure every op_id in `replaces` also exists on the FastAPI app
   and is not already under `include:` — the validator will fail
   otherwise.

### Add an external forward

1. Write a module under `tools/` exposing a
   `register(mcp, config, stack)` function. Use the `stack` to attach
   the httpx client's lifecycle.
2. Add a nested config class on `GSMCPConfig` for its base URL, auth
   env var, etc.
3. Add an entry under `external_tools:` in the YAML.
4. Wire the registration in `tools/__init__.py::register_custom_tools`.

### Drop a tool

1. Remove its entry from `include:` or `consolidated_tools:`.
2. If consolidated, delete the `register_*` function too.
3. Run the validator.

## Context cost

All 16 tool schemas (17 when `search_neighbors` is configured) fit in roughly
300–500 tokens when serialized for transport. Claude Code lazy-loads
(only the tools the LLM picks get read into context), so the observed
cost is usually a few hundred tokens total. Other clients vary —
Claude.ai custom connectors eager-load, so the full surface cost matters
there.

The dominant cost scaling factor is **descriptions, not count** — long
docstrings and verbose schemas are the biggest levers. Keep descriptions
action-oriented and under ~3 sentences.

## Tests

```bash
uv run pytest tests/mcp/ tests/cli/test_mcp_cli_integration.py -v
```

Covers:

- Config env-var parsing (including nested `SearchNeighborsConfig`).
- Curation YAML loading + drift detection + `include`/`replaces` overlap.
- Route filter and description-override logic.
- `SearchNeighborsClient` polling loop, timeout, HTTP-error
  translation, and three auth variants (no env, env unset, env set).
- In-process integration with `fastmcp.Client` against `create_spec_app`
  — asserts the curated tool set is exposed and replaced endpoints are
  absent.
- CLI `validate-curation` happy path.

## Runtime layout

```
src/graphsenselib/mcp/
  __init__.py              Public surface: GSMCPConfig, attach_to_fastapi, ...
  config.py                GSMCPConfig + SearchNeighborsConfig (pydantic-settings, env_prefix GS_MCP_)
  curation.py              CurationFile model + YAML loader + drift validator
  routes.py                make_route_map_fn, make_component_fn for FastMCP
  server.py                build_mcp, attach_to_fastapi (lifespan composition)
  cli.py                   Click group for `graphsense-cli mcp validate-curation`
  tools/
    __init__.py            register_custom_tools dispatcher
    consolidated.py        Hand-written @mcp.tool wrappers (ASGI in-process)
    search_neighbors.py    External proprietary forward with polling
  curation/
    tools.yaml             The positive list — source of truth for the surface
```
