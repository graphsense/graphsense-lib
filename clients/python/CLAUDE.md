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
- `graphsense/gs_files/*` — **vendored** from `src/graphsenselib/convert/gs_files/`;
  see "Vendored `gs_files`" below.
- `graphsense/address_scan/*` — **vendored** from
  `src/graphsenselib/convert/address_scan/`; see "Vendored `address_scan`" below.
- `tests/*` — regression tests.
- `docs/ext/*`, `docs/cli/*` — hand-written docs.
- `README_CLI.md`, `README_EXT.md`, `CLAUDE.md` (this file).
- `compat/*` — OpenAPI v7 backward-compat patches.
- `templates/*` — custom Mustache templates.
- `scripts/*` — local utility scripts (e.g. `sync_gs_files.py`).

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

## Vendored `gs_files`

The `graphsense/gs_files/` package is **not hand-written**: it is a
verbatim copy of `src/graphsenselib/convert/gs_files/` from this repo,
synced by `scripts/sync_gs_files.py`. We vendor (rather than depend on
`graphsenselib`) so the published `graphsense-python` package stays
small and standalone. The source module is pure stdlib, which makes
vendoring cheap.

**Rules**:

- Edit the source (`src/graphsenselib/convert/gs_files/`), never the
  vendored copy. Each vendored file carries an `AUTO-GENERATED — DO NOT
  EDIT` header.
- Run `make sync-gs-files` to refresh; `make check-gs-files` is the
  drift check used by CI and the repo's pre-commit hook.
- `cli.py` from the source is intentionally excluded — the client has
  its own `rich_click`-based wrapper at `graphsense/cli/gs.py` that
  integrates with the global `-f/-o/--input/--input-format` plumbing.
- Public API: import `from graphsense.gs_files import decode_gs,
  structure, summarize, to_jsonable, GsBuilder, ...` (mirrors
  `graphsenselib.convert.gs_files`).

## Vendored `address_scan`

The `graphsense/address_scan/` package is **not hand-written**: it is a copy of
`src/graphsenselib/convert/address_scan/` synced by
`scripts/sync_address_scan.py`. It powers `graphsense file scan-for-addresses`
— scanning text/SQL files and compressed containers for crypto addresses.

Unlike `gs_files`, the source is *not* fully standalone: in graphsenselib its
`detectors.py` uses `graphsenselib.utils.address` and `decompress.py` uses
`graphsenselib.convert.gs_files.parser`. To keep the client dependency-free the
sync script **rewrites** those two imports:

- `graphsenselib.utils.address` → `graphsense.address_scan.validators`, a
  **stdlib-only** reimplementation of the validators (base58check, bech32,
  pure-Python keccak for EIP-55, ripple base58check for XRP). graphsenselib
  itself keeps using its lib-backed `utils/address.py`; the two are pinned
  together by `tests/convert/address_scan/test_validators_crosscheck.py` in the
  main repo. **Do not** point graphsenselib at the stdlib module — it exists
  only for the vendored client.
- `graphsenselib.convert.gs_files.parser` → `graphsense.gs_files.parser`
  (already vendored).

**Rules**:

- Edit the source (`src/graphsenselib/convert/address_scan/`), never the
  vendored copy (each carries an `AUTO-GENERATED — DO NOT EDIT` header).
- Run `make sync-address-scan` to refresh; `make check-address-scan` is the
  drift check used by CI and the repo's pre-commit hook. If you add/remove a
  cross-module import in the source, update `REWRITES` in
  `scripts/sync_address_scan.py`.
- `cli.py` from the source is excluded — the client's wrapper is
  `graphsense/cli/scan.py`.

## Deprecated endpoints

`EntitiesApi` (and related `entity`-prefixed fields) are deprecated; use
`ClustersApi` / `cluster` instead. The ext layer hides deprecated API groups
from `GraphSense.raw` by default (opt-in with `show_deprecated=True` or
`GRAPHSENSE_CLIENT_SHOW_DEPRECATED_ENDPOINTS=1` env var). A response carrying an RFC 8594 `Deprecation`
header triggers a one-shot stderr warning — see `graphsense/ext/deprecation.py`.

## Quality gates

Before landing changes, run from `clients/python/`:

```sh
make lint            # ruff check + format --check
make type-check      # ty on graphsense/ext + graphsense/cli
make test-ci         # pytest with -W error
make check-gs-files  # vendored gs_files copy is in sync
```

CI (`.github/workflows/run_tests_client.yaml`) runs all of the above plus
`make test-compat` across python 3.9–3.13.
