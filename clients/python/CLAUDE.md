# Guidance for Claude / future authors (clients/python)

Short, load-bearing notes for anyone editing this package — human or LLM.

## Terminology: prefer "network" over "currency" in new code

The REST API and the generated client use the parameter name `currency`
throughout (e.g. `AddressesApi.get_address(currency, address)`). This is
accurate only in a narrow sense — BTC and ETH are more meaningfully
different *networks*, not currencies, and tokens blur the line further.

**Rule**: in hand-written code (`graphsense/ext/`, `graphsense/cli/`, tests,
docs) prefer `network` as the term for identifiers like `btc`, `eth`, `trx`.
Use it for new CLI flags, options, parameter names on newly-introduced
functions, documentation, and in user-facing strings.

**Exception**: do NOT rename existing `currency` parameters on the generated
API or on public convenience methods that mirror it (`gs.lookup_address(...,
currency=...)`). Those follow the REST API for consistency; renaming them
would break user code and would desync from the generator.

**Examples**:
- ✅ New CLI flag `--network-col` (consistent with the new terminology).
- ✅ New helper `parse_input_with_network()` → returns `[(network, id), ...]`.
- ✅ Context field `network_jq`, `network_col`.
- ✅ Doc copy: "per-row network extraction".
- ❌ Do NOT rename `GraphSense.lookup_address(currency=...)`; callers rely on it.
- ❌ Do NOT rewrite `graphsense.api.*Api.get_address(currency=...)`; it's generated.

## Regeneration contract

All hand-written code lives under paths listed in `.openapi-generator-ignore`:

- `graphsense/ext/*` — the high-level `GraphSense` facade, selectors, I/O,
  output writers, deprecation hook.
- `graphsense/cli/*` — the `graphsense` CLI.
- `tests/*` — regression tests.
- `docs/ext/*`, `docs/cli/*` — hand-written docs.
- `README_CLI.md`, `README_EXT.md`, `CLAUDE.md` (this file).
- `compat/*` — OpenAPI v7 backward-compat patches.
- `templates/*` — custom Mustache templates.

Anything outside those trees is overwritten by `make generate-openapi-client`.
Never put sticky edits in `graphsense/api/` or `graphsense/models/` — patch
them in `compat/patch_compat.py` instead (see existing patterns there).

## The `gs.raw` escape hatch

`GraphSense.raw` is populated by introspecting `graphsense.api.*Api` classes
at instantiation time. New endpoints added by the generator appear
automatically with no client-code changes; removed ones disappear.

The CLI's `graphsense raw <group> <method> ...` tree is built the same way.
That's the main reason the CLI survives regeneration — don't hand-mirror the
API surface in convenience commands unless you also want to maintain it
across regenerations.

## Deprecated endpoints

`EntitiesApi` (and related `entity`-prefixed fields) are deprecated; use
`ClustersApi` / `cluster` instead. The ext layer hides deprecated API groups
from `GraphSense.raw` by default (opt-in with `show_deprecated=True` or
`GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS=1` env var). A response carrying an RFC 8594 `Deprecation`
header triggers a one-shot stderr warning — see `graphsense/ext/deprecation.py`.

## Quality gates

Before landing changes, run from `clients/python/`:

```sh
make lint         # ruff check + format --check
make type-check   # ty on graphsense/ext + graphsense/cli
make test-ci      # pytest with -W error
```

CI (`.github/workflows/run_tests_client.yaml`) runs all of the above plus
`make test-compat` across python 3.9–3.13.
