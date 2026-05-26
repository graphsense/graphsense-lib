# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Version Tracks

- `vX.Y.Z` tags track **Library** releases.
- `webapi-vA.B.C` tags track **Web API + Python client** releases.

Use one changelog file, but separate entries by track in each release window.

## [2.13.5] - Unreleased

### Library (v2.13.5)

#### Changed
- **`build_pathfinder_file` returns a `download_url` when a file store is configured.** The tool stashes the `.gs` file and returns a short-lived, unguessable download link in `structured_content`, usable on MCP hosts that drop embedded binary resources. The embedded resource is still sent unless `file_store.embed_resource` is false; files over `file_store.max_file_bytes` raise a `ToolError`. The tool is now async. Backwards compatible — without `file_store` config the only change is an additive `download_url: null` field.
- **`build_pathfinder_file` label guidance.** The `label` field descriptions and docstring now tell the model not to restate attribution tags or transaction date/value (the Pathfinder UI shows those) and to reserve `label` for case context. Docstring only; `.gs` encoding unchanged.
- **`build_pathfinder_file` switched to a tidy-tree layout that keeps subtrees clustered under their parent.** `apply_hierarchical_layout` previously placed every node by BFS-centring its hop level, so a parent's children could spread to opposite extremes of their column and branches crossed each other. The layout now builds a BFS spanning tree (first-discoverer = parent) and walks it post-order: leaves take consecutive rows, internal nodes sit at the midpoint of their first and last child — every descendant stays on the same side as its ancestor. Sibling order still follows spec order. Each tx is then snapped to the mean y of the addresses it endpoints, so single-edge txs sit on the straight line between them; a tx shared by multiple edges averages all of them. Row step is now global (max label-line count) rather than per-column, since per-column variation would break the subtree alignment.
- **Hierarchical and columnar layout constants match the Pathfinder UI defaults** (`Config/Pathfinder.elm`: `nodeXOffset=4`, `nodeYOffset=2.5`). `_HIER_Y_STEP` and `GsBuilder._ROW` both moved from `3.0` to `2.5`, so a fresh `.gs` file lays out at the same density the UI would use if the nodes were added manually.

#### Fixed
- **`build_pathfinder_file` docstring no longer claims the model receives the `.gs` bytes.** The bytes travel in the MCP resource channel the model cannot read; the old wording made models on hosts that drop the embedded resource fabricate base64 and `data:` URLs. Docstring only.
- **MCP tool selection for "what currencies are supported?".** Weaker MCP hosts (observed on Mistral Le Chat "Work" mode) picked `list_supported_tokens` instead of `get_statistics` and iterated it per-network, seeing mostly-empty token lists (BTC/LTC/BCH have no in-network tokens), then concluding "platform supports an extremely wide range of currencies". `tools.yaml` descriptions for the two tools now disambiguate explicitly — `get_statistics` is labelled as THE source for "what currencies / blockchains / networks does the platform support", and `list_supported_tokens` carries a negative cross-reference plus a note that an empty token list does NOT mean the network is unsupported. `instructions.md` carries the same hint in the "Tool selection" section. Pure description change; tools, schemas and REST surface are unchanged.
- **Tagpack / actorpack `!include` now resolves against the repo root when called without an explicit `header_dir`.** `TagPack.load_from_file` and `ActorPack.load_from_file` used to register the pyyaml-include constructor with `base_dir=header_dir`; when `header_dir` was `None` (the single-file validation path) `!include header.yaml` resolved against CWD, so `gs tagpack validate <file>` only worked when run from the tagpack repo root. A new helper `find_pack_root` walks up from the file's directory (≤ 3 ancestors) looking for either a directory named `packs` (the tagpack-repo-root convention) or a directory containing `.git`; the first match becomes the include `base_dir`. Explicit `header_dir` arguments still take precedence; when no root is found within the bound, the loader falls back to pyyaml-include's CWD-relative default, so any pre-existing caller that depended on that behaviour keeps working.

### Web API + Python client (webapi-2.13.5)

#### Added
- **Optional Redis-backed file store and `/download/{token}` route.** New `web/file_store.py` (`RedisFileStore`, a reusable `FileStore` protocol) holds files as TTL'd Redis hashes keyed by a 256-bit CSPRNG token. New `FileStoreConfig` on `GSRestConfig`: `enabled` (default false), `redis_url`, `download_path` (`/download`), `ttl_s` (1800), `max_file_bytes` (5 MiB), `base_url`, `key_prefix`, `embed_resource`. When enabled, the route is a plain Starlette route — excluded from the OpenAPI spec and outside the API-key dependencies, so the token is the only credential; URLs derive from `X-Forwarded-*`/`Host` with a `base_url` override. Multi-worker safe; disabled by default, fully backwards compatible.

#### Fixed
- **Slack exception notifications now cover MCP tool failures.** The Slack handler is also attached to the `graphsenselib.mcp` logger tree (siblings, not children of `graphsenselib.web.app`, so handler propagation didn't reach them), and a new `ErrorLoggingMiddleware` (`mcp/error_logging.py`) registered on the FastMCP instance calls `logger.exception(...)` on any unhandled tool / resource / prompt exception before re-raising. Expected protocol errors (`ToolError`, `ResourceError`, `PromptError`) pass through silently — they're contract, not incidents.

## [2.13.4] - 2026-05-20

### Library (v2.13.4)

#### Added
- **New MCP tool `build_pathfinder_file` produces a `.gs` save file from an investigation agent's findings.** The agent passes addresses, transactions, and aggregated edges accumulated via `lookup_address` / `list_neighbors` / `list_txs_for`; the tool returns the `.gs` bytes as an MCP embedded resource (`BlobResourceContents`, MIME `application/octet-stream`, URI `file:///<filename>.gs`) so clients can hand it to the user as a downloadable attachment without feeding the blob through the model — `structured_content` carries only `{filename, summary}` (layout, counts, warnings). Layout is automatic: a new BFS-hierarchical layout (`apply_hierarchical_layout` in `src/graphsenselib/convert/gs_files/encoder.py`) runs whenever at least one node is flagged `starting_point=true` — anchors at column 0, every other node placed by hop distance with txs as stepping stones, within-level order following the spec (so writing the most relevant nodes first puts them near the top of their column); otherwise the columnar `GsBuilder` defaults apply. The docstring spells out the join semantics (a tx renders only when listed in `txs` AND referenced from `agg_edges.tx_ids`) with a worked example, and `summary.warnings` (`_collect_warnings`) flags four common authoring mistakes that render an empty/abstract graph: edges with no `txs`, edges missing `tx_ids`, `tx_ids` referencing hashes not in `txs`, and `a`/`b` endpoints not in `addresses` (advisory only, unknown-id lists truncated to ten). Input boundary uses the same conservative currency/id guards as elsewhere in `src/graphsenselib/mcp/tools/consolidated.py`. Registered in `src/graphsenselib/mcp/curation/tools.yaml` under `consolidated_tools` with `replaces: []` — net new surface, no existing tool or endpoint changed.

#### Changed
- **MCP hides the deprecated `entity` and `status` fields on every address, cluster, and raw-tag response.** Both fields are already flagged `deprecated: true` in the OpenAPI schema (`Address.entity`, `Address.status`, `Entity.entity`, `NeighborEntity.entity`, `AddressTag.entity`) and the REST surface dual-emits `entity` alongside the preferred `cluster` alias for backwards compatibility; surfacing both via MCP made the LLM double-read or pick the wrong one. `entity` and `status` are now added to `_LEGACY_ADDRESS_FIELDS`, `entity` to `_LEGACY_CLUSTER_FIELDS`, and a new `_LEGACY_TAG_FIELDS = {"entity"}` is applied to each row in `list_tags_by_address` (`src/graphsenselib/mcp/tools/consolidated.py`). REST endpoints and OpenAPI schemas are unchanged — the deprecation markers continue to advertise the field as legacy for non-MCP consumers.

#### Fixed
- **Type-checker warnings: `namedtuple("Row", ...)` calls in `tests/schema/test_apply_migrations.py` now use names that match their variables (`TransformedRow`, `RawRow`).** Pure rename — no behavioural change; clears the two `mismatched-type-name` diagnostics from `uv run ty check`.

### Web API + Python client (webapi-2.13.4)

No changes.

## [2.13.3] - 2026-05-18

### Library (v2.13.3)

#### Added
- **`tagpack sync` keeps a persistent git repo cache instead of re-cloning every run.** Previously each repo in the sync list was cloned afresh into a throwaway temp directory. Synced repos are now kept under a stable cache directory and refreshed with a `git fetch` (`_sync_repo`/`_repo_workdir` in `src/graphsenselib/tagpack/cli.py`); only changed objects are downloaded. The refresh is authoritative — `fetch` + `checkout` + `reset --hard` + `clean -fdx` — so a reused checkout is identical to a fresh clone, and a missing/corrupt/wrong-remote cache is transparently re-cloned. New `--repo-cache-dir` option overrides the location (defaults to a `graphsense_tagstore_sync_repos` folder in the system temp directory). Non-breaking: no existing option changed and sync results are unaffected.

#### Fixed
- **Delta-update hardened against Cassandra-outage corruption mid-flush.** `DbWriterMixin.apply_changes` now also retries `Unavailable` and `NoHostAvailable` (previously only `WriteTimeout`/`OperationTimedOut`), so a recoverable database outage is waited out — bounded by `stop_after_attempt` — instead of aborting a half-flushed batch and leaving the keyspace inconsistent; statements are bound once with literal values, so retries are idempotent. Additionally, pure auto-resume runs (no explicit `--start-block`/`--end-block`) now refuse to continue when `summary_statistics` points at a block with no matching `delta_updater_history` row — a sign the last bookkeeping write was torn — turning a silent double-count/skip into an actionable stop. New `TransformedDb.delta_updater_history_has_block` point-read backs the guard, which can be bypassed with `--disable-safety-checks`. Changes in `src/graphsenselib/db/analytics.py` and `src/graphsenselib/deltaupdate/deltaupdater.py`.

### Web API + Python client (webapi-2.13.3)

#### Added
- **API docs now describe the MCP (Model Context Protocol) interface.** A non-technical "AI assistant access (MCP)" section was added to the API description shown at the top of the Swagger UI / ReDoc pages (`API_DESCRIPTION` in `src/graphsenselib/web/app.py`). It explains, for a general audience, that the same deployment exposes an MCP endpoint at the `/mcp` path which lets AI assistants query GraphSense in natural language, and how to connect one. Docs-only and additive — no endpoint, schema, or generated-client behaviour changed.

## [2.13.2] - 2026-05-18

### Library (v2.13.2)

#### Changed
- **`.gs` save-file parser: `entity` renamed to `cluster` across the exposed interface.** `graphsenselib.convert.gs_files` now exports `GraphCluster` instead of `GraphEntity`; the structured dataclasses use `GraphData.clusters` (was `.entities`) and `GraphCluster.cluster_id` (was `.entity_id`). The `convert gs-files decode` JSON output emits `clusters`/`cluster_id`, and `summary` reports `n_clusters` (was `n_entities`). Breaking for downstream consumers of the structured output or the public dataclasses. The vendored copy in the `graphsense-python` client (`graphsense.gs_files`, used by `graphsense gs`) is synced to match.

#### Fixed
- **`tagpack insert` resolved each file's last-commit time with a separate full-history `git log` walk.** `get_uri_for_tagpack` called `list(repo.iter_commits(paths=file))` per tagpack — `O(files x history)` — and used only the newest commit. The most recent commit time of all files is now resolved in a single `git log --name-only` pass (`get_last_commit_times` in `src/graphsenselib/tagpack/tagpack.py`), and the per-file fallback uses `max_count=1` so `git rev-list` stops at the first match. Same results, applied to both `tagpack insert` and `actorpack insert`.

### Web API + Python client (webapi-2.13.2)

#### Changed
- **`graphsense gs` CLI: `.gs` parser output renamed `entity` → `cluster`** (vendored `gs_files` module synced from graphsenselib). `decode` emits `clusters`/`cluster_id`; `summary` reports `n_clusters`. See the Library entry above.

## [2.13.1] - 2026-05-13

### Library (v2.13.1)

#### Changed
- **MCP: `labels` removed from the top level of every address, cluster, and neighbor response.** The upstream REST surface attaches a quick-aggregate `labels` field alongside the structured `tag_summary`, which conflicted with `tag_summary.labels` (the renamed `label_summary`) and caused LLMs to double-count or mis-attribute tags. `labels` now appears in MCP output only inside `tag_summary` (from `lookup_address` / `list_tags_by_address`). Implemented by extending `_LEGACY_ADDRESS_FIELDS` and `_LEGACY_CLUSTER_FIELDS` in `src/graphsenselib/mcp/tools/consolidated.py`; the existing neighbor strip is unchanged.

#### Fixed
- **Tagpack reinsert was slow (~8 s per pack) due to a missing FK index.** `tag.tagpack` (FK to `tagpack.id` with `ON DELETE CASCADE`) had no index, so every `DELETE FROM tagpack` in the `--update`/`force_insert` path triggered a sequential scan of the entire `tag` table to resolve the cascade. On an 80 M-row tag table a single delete spent ~7.9 s inside the `tag_tagpack_fkey` trigger; 8 k packs took the better part of a day. Added `index=True` on `Tag.tagpack_id` in `src/graphsenselib/tagstore/db/models.py`. Existing deployments must also create the index on the live DB, e.g. `CREATE INDEX CONCURRENTLY IF NOT EXISTS tag_tagpack_idx ON tag (tagpack);`.
- **`TagStore` read-only helpers leaked an `idle in transaction` connection for the entire dispatcher run.** `get_ingested_tagpacks`, `get_ingested_actorpacks`, and `get_actor_alias_mapping` issued SELECTs through a `psycopg2` connection with `autocommit=False` and never committed or rolled back, leaving the main process holding a transaction snapshot for hours during a tagpack insert. That pinned the DB-wide `xmin` horizon (blocking autovacuum cleanup on every table) and would have stalled `CREATE INDEX CONCURRENTLY` indefinitely. Each helper now calls `self.conn.rollback()` before returning.

### Web API + Python client (webapi-2.13.0)

No changes.

## [2.13.0] 2026-05-13

### Library (v2.13.0)

#### Added
- **PySpark Delta Lake → Cassandra bulk-ingest transformation** (`src/graphsenselib/transformation/`). New CLI `graphsense-cli transformation run --env <env> --currency <c>` reads raw blockchain data from Delta Lake tables and writes it to a Cassandra raw keyspace via the `spark-cassandra-connector`. Supports BTC/LTC/BCH/ZEC (UTXO) and ETH/TRX (account) schema types; UTXO transformation derives `transaction_spending`, `transaction_spent_in`, `block_transactions`, and `tx_prefix` lookups from base transactions, account transformation handles varint binary columns. Options include `--start-block`, `--end-block`, `--create-schema`, `--raw-keyspace` override, `--delta-lake-path` override, `--local` (Spark local mode), `--debug-write-audit` (per-Spark-partition row counts and PK skew), and `--patch` for account-chain incremental runs (rejected for UTXO because spend tables are not window-local). Two-phase locking: phase 1 pins a top-block snapshot under the delta-ingest lock to avoid tearing concurrent ingest, phase 2 holds the transformed-keyspace lock for the Spark run (ingest is not blocked once phase 1 releases). New `[transformation]` extra (`pyspark>=3.5,<4.0`), separate `Dockerfile.transformation`, and Java JRE baked into the main Docker image so the main entrypoint can launch Spark without a sidecar.
- **One-off UTXO address clustering CLI** `graphsense-cli transformation cluster --env <env> --currency <c>` (`src/graphsenselib/transformation/clustering.py`). Reads transactions via point/range queries in `--chunk-size`-block chunks (default 1000), feeds them to the Rust clustering engine, and streams the resulting mapping back to `fresh_address_cluster` / `fresh_cluster_addresses` in the transformed keyspace. No PySpark dependency. Options: `--start-block`, `--end-block` (auto-detected from raw keyspace if omitted), `--concurrency` (default 100), `--write-chunk` (default 100 000). Gated behind `GRAPHSENSE_FRESH_CLUSTERING_ENABLED`; the prior PySpark clustering path was retired in favour of this one.
- **`graphsense-clustering` Rust crate** (`rust/gs_clustering/`, PyO3 + maturin) shipped as abi3 PyPI wheels. Public Python surface: `Clustering` class with `process_transactions`, `get_mapping`, `rebuild_from_mapping`, `get_diff`. New `[clustering]` extra in `pyproject.toml` (`graphsense-clustering>=0.1.0`); local checkouts build the crate from source via an `editable` `tool.uv.sources` entry.
- **Incremental fresh clustering inside UTXO delta update.** When `GRAPHSENSE_FRESH_CLUSTERING_ENABLED=true`, `run_fresh_clustering` runs once per update range (not per batch), reads only affected clusters with dense ID remapping, uses real exchange rates and address IDs from the transformed keyspace, and writes to the `fresh_*` tables. CQL moved to `TransformedDb`; raw CQL removed from update logic. Disabled by default — runtime behaviour matches develop with the env var unset (no writes, no reads, no Rust import).
- **Fresh-clustering schema and migrations.** New UTXO transformed tables `fresh_address_cluster` and `fresh_cluster_addresses`; new `fresh_cluster_id` field on the address API endpoint. Transformed-keyspace migrations are now applied on startup (`GraphsenseSchemas().apply_migrations(..., keyspace_type="transformed")`); the first transformed migration `transformed_utxo_0_to_1` ships in this release.
- **Raw UTXO tx schema additions**: `sequence`, `version`, `lock_time` projected from Delta to Cassandra and surfaced via the new transformation pipeline. New `is_rbf_signaled` BIP125 predicate in `graphsenselib.utils`.
- **Auto-catch-up of diverged sinks before forward run** (`src/graphsenselib/ingest/`). When a mixed `--sinks delta --sinks cassandra` append finds the registered sinks at different highest blocks, the runner now executes a single-sink `IngestRunner` over `[laggard_h+1, target]` for each laggard (sharing source/transformer instances and the outer lock stack) before falling through to the forward run. Regression coverage in `tests/regressions/` for catch-up-vs-sync-from-start equivalence, merge-boundary chain-truth/equivalence for ETH, and replaying a `trx_raw` mid-chunk gap.
- **`--patch` mode for account-chain transformation** (also surfaced on `ingest from-node` via `merge` write-mode and shared `_run_auto_compact` helper). Lifts the empty-keyspace guard so the transformation can extend or repair an existing account / account_trx raw keyspace via PK-upsert writes; rows outside `[start-block, end-block]` are untouched. Account chains only — UTXO is rejected because spend-link tables are computed over the full block range loaded by Spark. New `--auto-compact` / `--auto-compact-last-n` options on `ingest from-node` mirror the soon-to-be-deprecated `ingest delta-lake ingest` flags.
- **Per-resource locking** across ingest, delta-update, and transformation (`src/graphsenselib/utils/locking.py`). Lock keys that previously mixed reader and writer identity (compound `{raw_ks}_{transformed_ks}`, currency-based `delta_ingest_{currency}`) are replaced with locks keyed on the actual mutated resource. New `delta_ingest_lock_name(delta_lake_path, currency)` helper makes the delta-side lock derivable from the path so transformation and ingest agree on the key without sharing config.
- **`ingest_complete` marker write-ordering and rename.** The bootstrap-marker state table introduced in 2.11.0 is now written as the **last** PySpark transformation write, so its presence is an atomic "this keyspace is usable" signal even if the run is aborted mid-stream. The table itself is renamed from `bootstrap_marker` → `ingest_complete`, the constant and row builder are centralised in `src/graphsenselib/db/`, and the configuration seed now uses the target keyspace name (not the prefix).
- **Transformation startup banner** logging env, currency / schema type, delta source (with bucket + endpoint for S3 paths), target keyspace (with `(override)` marker), Cassandra nodes, block range, pinned top block, Spark mode (`local[*]` vs cluster), and patch flag — printed before the Spark session opens so cluster runs are diagnosable from the driver log alone.
- **Per-partition write audit** (`--debug-write-audit`) prints per-Spark-partition row counts and partition-key skew before each Cassandra write to diagnose stragglers; adds one shuffle per write. Cassandra write metrics emitted on completion.
- **Curve `TokenExchange` events added to swap detection** (`src/graphsenselib/datatypes/abi.py`). Adds the four canonical event variants (StableSwap and CryptoSwap, plus their underlying variants) tagged `["curve", "swap"]`, so Curve pool swaps (3pool, tricrypto, …) are no longer resolved as `UNKNOWN`. Regression test covers a USDC→USDT 3pool swap.
- **UTXO delta-update cross-version regression test suite** under `tests/regressions/`. Ingests a BTC range, runs PySpark Delta Lake → Cassandra transformation, then runs the UTXO delta-updater with the local checkout and a reference release (default v2.12.3) into separate transformed keyspaces and diffs the result. Captures per-side wall time and works against arbitrary previous releases via the `RELEASE_REF` env var. Shared `lib/` package, conftest fixture factories, and slimmer per-module test files factored out across the regressions tree.
- **MCP (Model Context Protocol) server** mounted inside the existing FastAPI app at `/mcp` (override via `GS_MCP_PATH`). LLM clients (Claude Code, Claude Desktop, Cursor, custom agents) can query graphsense directly without a separate process. Auto-attached in `create_app`, `create_app_from_dict`, and `create_spec_app` via `_maybe_attach_mcp`; silent no-op when the `[mcp]` extra is not installed. Transport: streamable-http, `stateless_http=True` by default (set `GS_MCP_STATELESS_HTTP=false` to opt in to stateful). Disable entirely with `GS_MCP_ENABLED=false`. Implementation in `src/graphsenselib/mcp/`.
- **Curated MCP tool surface** driven by a positive-list YAML at `src/graphsenselib/mcp/curation/tools.yaml`. Out of FastAPI's 44 routes, 17 are surfaced (18 with `search_neighbors` configured): 11 passthroughs (`get_statistics`, `search`, `get_block`, `get_block_by_date`, `list_block_txs`, `list_tx_flows`, `get_exchange_rates`, `list_supported_tokens`, `get_actor`, `list_taxonomies`, `list_concepts`), 6 hand-written consolidated tools that collapse common chains (`lookup_address`, `lookup_cluster`, `lookup_tx_details`, `list_neighbors`, `list_txs_for`, `list_tags_by_address`), and an optional external forward to the proprietary `search_neighbors` service. Curation drift is caught at boot and via the CI gate `graphsense-cli mcp validate-curation`.
- **`graphsense-cli mcp validate-curation`** — CI-friendly subcommand that validates the curation YAML against the live FastAPI app (uses the minimal spec app, no DB required) and exits non-zero on drift.
- **Pathfinder deep-link instructions** for MCP clients. Server-side `instructions` (the MCP analogue of a system prompt) are sourced from `curation/instructions.md` and substituted with the configured `pathfinder_base_url` (default `https://app.iknaio.com`) so LLMs can build links like `{base}/pathfinder/btc/address/<addr>`. Override via `GS_MCP_INSTRUCTIONS` / `GS_MCP_INSTRUCTIONS_FILE` / `GS_MCP_PATHFINDER_BASE_URL`.
- **External request routing** for the MCP fan-out wrappers. By default, consolidated tools dispatch in-process via httpx `ASGITransport`; set `GS_MCP_INTERNAL_BASE_URL` to route fan-out calls through a real HTTP client so each call traverses upstream middleware. Originating MCP request headers are forwarded on every internal call in both modes.
- **New `[mcp]` extra** in `pyproject.toml` (`fastmcp>=3.2,<4.0`, `pyyaml>=6.0`, transitively pulls `[web]`). Also added to the `[all]` extra.

#### Changed
- **`block` table is now written last** in every ingest path (`src/graphsenselib/ingest/`). `get_highest_block()` reads `MAX(block_id)` from the block table as the resume marker, so a mid-chunk crash (e.g. Cassandra coordinator timeout) could otherwise advance the marker past partially-written side tables. The transformer dicts previously emitted `block` first; sinks now write it after all dependent tables land.
- **Delta auto-compact scoped to recent partitions.** `optimize.compact` now accepts a `last_n_partitions` argument and forwards it as a `partition_filters` predicate, so weekly auto-compact only rewrites partitions that could plausibly have received writes since the last run. Older raw-data partitions are immutable and no longer touched. `deltalake` bumped to **1.5.1**.
- **`graphsense-cli transformation run --s3-config NAME` is now required for S3 delta paths.** The transformation CLI no longer derives S3 credentials from the delta sink's `s3_config` field or the top-level `s3_credentials` fallback; users pick a named entry from `s3_configs` explicitly. Missing/unknown names raise an error listing the available choices.
- **Spark app name renamed** to `graphsense-bulk-ingest-{currency}-{env}` (`src/graphsenselib/transformation/factory.py`) so cluster dashboards group the new transformation runs separately from the Scala lineage.
- **Docker image**: main runtime image shrunk from **5.3 GB → 2.1 GB** while gaining the Rust clustering crate and the Java JRE needed for PySpark. Regression tests now use the main `Dockerfile` directly; the previous separate test image was retired.
- **Dependencies refreshed** (`uv.lock`); pyproject.toml constraints bumped where appropriate. See "Dependencies" below.

#### Fixed
- **`get_latest_tx_id_before_block` could restart `_next_tx_id` at 0 on a non-empty keyspace.** When the immediately preceding `block_transactions` row was missing but the `block` table had advanced past that gap, the function returned `-1` and the next allocation silently overwrote existing tx_ids. Now distinguishes a fresh keyspace from a gap in `block_transactions` and refuses to allocate at 0 when prior data is present.
- **`apply_migrations` used the wrong PK column for transformed-config tables.** The version-bump UPDATE was built with `WHERE id = …`, but transformed configuration tables key on `keyspace_name` (only raw configurations have `id`). The first transformed migration (`transformed_utxo_0_to_1`) blew up with `AttributeError: 'Row' object has no attribute 'id'`; now selects the correct PK column per keyspace type.
- **Legacy ingest UDT shape and `lock_time` naming** reconciled with the new schema fields.
- **`access_list.storageKeys` → `storage_keys`** in the PySpark transformation output (Cassandra column name).
- **Transformation runs** previously read S3 credentials from the wrong sink config; now resolved from the per-sink `s3_config` reference, with Spark packages aligned to the iknaio cluster defaults and all Cassandra nodes passed from config (not just the first).

#### Performance
- **`run_fresh_clustering` rewritten with targeted point reads** and dense ID remapping — reads only the clusters affected by the update range instead of scanning the full transformed keyspace.
- **Spark transformation throughput**: Arrow-optimized UDFs enabled, transaction writes repartitioned by partition key (not range), Cassandra writes tuned with parallel table writes, `SinglePartition` bottleneck in `tx_id` computation eliminated. Net: per-partition write audit shows balanced shards on production-sized BTC runs.

### Web API + Python client (webapi-2.13.0)

#### Added
- **`graphsense gs` CLI group** for reading `.gs` save files (Pathfinder / Graph dashboards) without installing `graphsenselib`. Subcommands: `txs FILE` and `addresses FILE` emit a uniform `{"network", "id"}` shape that pipes directly into `lookup-tx` / `lookup-address` (via the standard `--address-jq '[].id' --network-jq '[].network'` selectors), enabling one-line re-hydration of every reference in a saved graph. `decode FILE` (optionally `--raw`) and `summary FILE` round out the group. Records are deduped by `(network, id)` by default; `--no-dedupe` retains repeats.
- **`graphsense.gs_files` Python API** — pure-stdlib decoder/encoder for `.gs` files, vendored from `src/graphsenselib/convert/gs_files/` so the standalone `graphsense-python` package picks up the reader without adding `graphsenselib` as a runtime dependency. Public surface mirrors the source: `decode_gs`, `structure`, `summarize`, `to_jsonable`, `GsBuilder`, plus the typed dataclasses (`PathfinderData`, `GraphData`, …).
- **Sync tooling for the vendored module.** `clients/python/scripts/sync_gs_files.py` copies the source verbatim with a `DO NOT EDIT` header on each file; `make -C clients/python sync-gs-files` writes, `make -C clients/python check-gs-files` is the drift check. A repo-level pre-commit hook (`sync-gs-files`) runs the write step automatically when either the source dir, the vendored copy, or the sync script changes. `cli.py` is excluded from the sync — the client wires its own `rich_click`-integrated CLI in `graphsense/cli/gs.py` so it inherits the global `-f / -o / -d / --input` plumbing.

#### Changed
- `clients/python/.openapi-generator-ignore` now also covers `graphsense/gs_files/*` and `scripts/*` to keep the vendored copy and sync utilities out of the generator's overwrite path.
- **`ext.client.lookup_address` never folds the best address tag into the cluster.** The convenience client now passes `include_best_address_tag=False` when fetching the parent cluster so the cluster summary is not contaminated by the address-level best tag of the address being looked up.
- **`ext.io`** input/output plumbing cleaned up (jq selector behaviour, error handling, and dedup logic exercised by new tests in `tests/test_ext_io.py`).

### Dependencies

#### Changed
- See commit `43aa309` (`update dependencies`) and the follow-up bump in this release window. `uv.lock` regenerated.

## [2.12.6] 2026-05-11

### Library (v2.12.6)

#### Fixed
- **Erigon 3.4 emits `blockTimestamp` on per-transaction RPC objects, which the field validator rejected.** `validate_rpc_fields` in `src/graphsenselib/ingest/rpc_eth.py` raised `Unknown RPC fields ['blockTimestamp'] in transaction` and aborted ingestion against nodes on the new release. `parse_transaction_json` already receives `block_timestamp` from the enclosing block, so the per-tx copy is redundant — it is added to `_TX_BLACKLIST` rather than to the parsed key set. Logs already carried the same field in newer Erigon releases and were already blacklisted; receipts (`eth_getTransactionReceipt`) and `eth_getBlockReceipts` were verified against `erigon/3.4.1/linux-amd64/go1.25.10` and need no change. Fix in commit `d8b5d5f` (`cover all rpc fields of erigon 3.4`).

### Web API + Python client (webapi-2.12.0)

No changes.

## [2.12.5] 2026-05-08

### Library (v2.12.5)

#### Changed
- **UTXO delta-update halves its relation read fan-out.** The address- and cluster-relation phases in `src/graphsenselib/deltaupdate/update/utxo/update.py` used to fire two batches of point reads per edge — one against `{address,cluster}_incoming_relations` and one against `{address,cluster}_outgoing_relations` — to look up the same edge from both sides. Both rows carry identical payload (`no_transactions`, `estimated_value`); only the partition keys differ, and those are derived from the address/cluster ids the updater already holds. `prepare_relations_for_ingest` in `src/graphsenselib/deltaupdate/update/generic.py` now reads only the incoming row and writes the merged result to both tables; Cassandra UPSERT covers the (asserted-impossible) case where the outgoing row was missing for an existing incoming row. Mirror of the account-side fix landed in v2.12.x (`DU: build outrelations from inrelations instead of querying`, ed8fea0). Net effect: ~50 % fewer relation point reads per UTXO delta-update batch across **both** address and cluster phases.

#### Fixed
- **`address_outgoing_relations.no_transactions` (and `cluster_outgoing_relations.no_transactions`) silently failed to increment on the update branch.** In `prepare_relations_for_ingest` (`src/graphsenselib/deltaupdate/update/generic.py`), the update path wrote `outr.no_transactions + delta.no_transactions` to the incoming row but only `outr.no_transactions` to the outgoing row — the delta was dropped on the outgoing side. Long-standing: present since the initial commit of the file (`d7818eb`, "delta updater version 2"). New-edge inserts were unaffected (they wrote `delta.no_transactions` to both sides correctly), so the drift accumulated only when an existing edge received additional transactions in a later batch. Outgoing-side `no_transactions` therefore reflected the count at the edge's first appearance, not the running total. As a side effect of the read-symmetry refactor above, both writes now derive from the same `inr.no_transactions + delta.no_transactions` expression and stay in sync. Backfill of historical drift is **not** included; rows correct themselves whenever the edge is touched again, but stale values otherwise persist.

### Web API + Python client (webapi-2.12.0)

No changes.

## [2.12.4] 2026-05-08

### Library (v2.12.4)

#### Changed
- **Gunicorn worker `timeout` raised 30 → 300 s** in the Dockerfile. Wide BTC txs with `?include_heuristics=all` legitimately need more than 30 s when the tagstore is cold; the previous limit silently SIGKILL'd the worker mid-request and APISIX returned 502 around 59 s (its own default route timeout retrying once on the upstream RST).
- **`TagsService.get_tag_summaries_by_subject_ids` now logs per-phase timings** (`pg_tags`, `cassandra_cluster_ids`, `pg_best_cluster`, `digest`, `total`) at DEBUG and emits a `WARNING` when total ≥ 10 s. Future regressions in this hot path are pinpointable from logs without a profiler attach.
- **`tagpack-tool sync` now logs per-phase wall-clock at INFO.** Each sub-step (init, per-repo clone / actorpack / tagpack insert, remove duplicates, refresh views, quality metrics, cluster-mapping staleness check, cluster-mapping import) is bracketed by start/done lines via a `_timed_phase` context manager, plus a final total. Operators can now see where time goes on multi-repo runs without instrumenting by hand.

#### Fixed
- **`TagstoreDbAsync.get_best_cluster_tags_for_clusters` shipped every cluster_definer tag back to Python** (regression introduced in v2.12.1's pool-exhaustion fix). The batched SQL builder dropped the `LIMIT 1` from the singleton query and reduced in Python, which is fine when each cluster has a handful of cluster_definer tags, but pathological for a heavily-tagged cluster: with `joinedload(Tag.concepts)` (a collection), the result set grows as `cluster_tag_count × concepts_per_tag` for *each* requested cluster. Observed: **298 s for one cluster** on a wide BTC tx whose 78 inputs all mapped to the same heavily-tagged cluster (timing line: `pg_best_cluster=298.149s` out of `total=298.633s`). Rewritten as two queries: (1) `SELECT DISTINCT ON (cluster_id) cluster_id, tag_id ... ORDER BY cluster_id, confidence.level DESC` picks the winner per cluster at the DB layer with no joinedloads (result-set bounded by `len(cluster_ids)`), (2) hydrate Tag + relationships only for the winning tag_ids. Same external contract — parity tests in `tests/web/test_tag_summaries_batch_parity.py` continue to pass. Affects both call sites: `get_tag_summaries_by_subject_ids` (CoinJoin FP-suppression on wide UTXO txs) and `entities_service.list_entity_neighbors` with `include_labels=true`.

### Build / packaging

#### Fixed
- **GHCR package description shows "No description provided"** despite the Dockerfile setting `LABEL org.opencontainers.image.description`. Once buildx publishes an attestation manifest list (the default in build-push-action v5+, visible as "OS / Arch 2" on the GHCR page), the UI reads the description from the **manifest annotation**, not from the image-config LABEL. Fix in `.github/workflows/github-packages-publish.yaml`: set `DOCKER_METADATA_ANNOTATIONS_LEVELS=manifest,index` on `docker/metadata-action`, and pass both `labels: ${{ steps.meta.outputs.labels }}` and `annotations: ${{ steps.meta.outputs.annotations }}` to `docker/build-push-action`. Description text is sourced automatically from the GitHub repo description. Also bumped `docker/metadata-action` 5.0.0 → 5.10.0 (gains `outputs.annotations` + the `DOCKER_METADATA_ANNOTATIONS_LEVELS` env, both added in 5.5.0) and `docker/build-push-action` 5.0.0 → 6.19.2 (gains the `annotations` input, added in 5.1.0; v6 is a non-breaking bump that adds workflow-level build summaries).

### Web API + Python client (webapi-2.12.0)

No changes.

## [2.12.3] 2026-05-08

### Library (v2.12.3)

#### Changed
- **Cluster-mapping staleness check is now per-network.** Sampling switched from a global `LIMIT N` (which was dominated by BTC's heavy-hitter clusters and effectively starved other chains) to `ROW_NUMBER() OVER (PARTITION BY network)`, so each eligible network gets up to `--staleness-sample-size` / `--cluster-staleness-sample-size` rows independently. The auto-rerun gate now triggers when the **maximum** per-network divergence rate ≥ threshold (was: weighted overall rate), so drift on smaller chains is no longer hidden by a clean BTC sample. Total Cassandra read cost grows from `sample_size` to `N × sample_size`.

#### Fixed
- **`LabelSummary.concepts` order is now deterministic** (`sorted(...)` instead of `list(set(...))`). The previous `set`-derived ordering was hash-dependent and could differ between Python versions, causing `TagSummary` equality comparisons to flake on 3.10 vs 3.11.
- **Resource files missing from Docker image** (regression introduced in v2.12.2 when `.git/` was removed from the build context). With `include-package-data = true` but no VCS root, setuptools_scm's file finder returned an empty list, so the wheel shipped zero `*.yaml` / `*.csv` / `*.sql` / `*.proto` resources — taxonomy loading (`concepts.yaml`, `countries.csv`, `confidence.csv`) and schema loading (`*.sql`) blew up at container startup. Fixed by declaring an explicit `[tool.setuptools.package-data]` table in `pyproject.toml` so file inclusion no longer depends on a present `.git`. Verified: a no-git build now ships the same 35 data files as the with-git build.

### Build / packaging

#### Added
- **CI guard for image resource files** (`.github/workflows/docker-build.yml`). After the existing smoke build, the workflow now `docker run`s an importlib probe inside the tagged image that asserts every package whose data files load at runtime (`graphsenselib.tagpack.db`, `graphsenselib.tagpack.conf`, `graphsenselib.schema.resources`, `graphsenselib.schema.resources.migrations`, `graphsenselib.tagstore.db`, `graphsenselib.ingest.resources`) and exercises the production `_load_taxonomies(...)` code path that crashed in 2.12.2. Catches future packaging regressions at the deployed-artifact layer on every push.

### Web API + Python client (webapi-2.12.0)

No changes.

## [2.12.2] 2026-05-07

### Library (v2.12.2)

#### Added
- **Batched tag-summary lookup** for the CoinJoin/Wasabi-1.x exchange-FP-suppression heuristic. New tagstore facade `TagstoreDbAsync.get_tags_by_subjectids(subject_ids, groups, network=None)` runs a single `Tag.identifier IN (:ids)` query and returns `Dict[subject_id, List[TagPublic]]`. New service method `TagsService.get_tag_summaries_by_subject_ids(network, subject_ids, tagstore_groups, include_best_cluster_tag=False)` returns `Dict[subject_id, TagSummary]` using ≤2 Postgres queries (one for direct tags; one for cluster-definer tags via `get_best_cluster_tags_for_clusters` when requested). Cluster-id resolution runs upfront against Cassandra (separate pool) so no fan-out hits the tagstore pool.

#### Changed
- **`_any_input_is_exchange` heuristic** now calls the batched path, replacing the previous per-address `gather_bounded` over `tags_service.get_tag_summary_by_address`. Postgres traffic per heuristic check drops from `2N+1` to `≤2` queries regardless of the number of inputs.
- **`CoinJoinDbCallbacks.get_tag_summary` renamed to `get_tag_summaries`** with batched signature `(currency, [subject_ids]) -> Dict[subject_id, TagSummary]`. The only caller (`txs_service`) is updated; external code constructing `CoinJoinDbCallbacks` directly must follow.

### Build / packaging

#### Changed
- **Docker version computation moved to the host.** The Dockerfile no longer COPYs `.git/` into the image. setuptools_scm now reads `SETUPTOOLS_SCM_PRETEND_VERSION_FOR_GRAPHSENSE_LIB`, computed on the host/runner where the full worktree and tags are available. `make build-docker` and both GitHub Actions workflows (`docker-build.yml` smoke test, `github-packages-publish.yaml` deploy) compute & pass the build-arg. No `fallback_version` — builds without the arg fail loudly rather than ship a sentinel-versioned image. Fixes images being labelled `2.13.0.dev0+gdb0370179.dYYYYMMDD` even when built from a clean release tag, caused by the previous selective-COPY pattern leaving the in-container git index reporting deleted tracked files.

### Web API + Python client (webapi-2.12.0)

No changes.

## [2.12.1] 2026-05-07

### Library (v2.12.1)

#### Added
- **Cluster mapping staleness check** for `tagpack-tool`: a sample of mapped addresses (biased toward large clusters via `gs_cluster_no_addr`) is compared against the current clustering in the graph datastore, and a full cluster-mapping rerun is triggered only when divergence crosses a threshold. New flags:
  - `tagpack-tool sync --auto-rerun-cluster-mapping-with-env <env>` (with `--cluster-staleness-sample-size`, default 2000, and `--cluster-staleness-threshold`, default 0.05).
  - `tagpack-tool tagstore insert-cluster-mappings --auto-rerun-if-stale` (with `--staleness-sample-size` / `--staleness-threshold`).
  - New diagnostic command `tagpack-tool tagstore check-cluster-mapping-staleness --use-gs-lib-config-env <env>` prints a per-network divergence table without writing to the DB.

  The existing `--rerun-cluster-mapping-with-env` and `--run-cluster-mapping-with-env` flags are unchanged. Eth-like networks (ETH/TRX) are skipped by the check since `cluster_id == address_id` and drift is not possible.

- **`max_concurrency` field on `TagStoreReaderConfig`** (env: `GRAPHSENSE_TAGSTORE_READ_MAX_CONCURRENCY`) caps the number of concurrent Postgres-touching coroutines per gs-rest request. Defaults to `max(2, pool_size // 3)` so a single wide request leaves headroom for concurrent traffic; can be overridden per-deployment. A `model_validator` rejects configs where `pool_size + max_overflow < max_concurrency` at config load. The active value is read at runtime via `get_tagstore_max_concurrency()` and registered on REST startup via `set_active_tagstore_config()`.

#### Changed
- **`TagStoreReaderConfig.pool_timeout` default lowered from 300 → 10 seconds.** The previous 5-minute default turned slow tagstore queries into request-time deadlocks; 10 seconds fails fast and surfaces real saturation.

#### Fixed
- **gs-rest Postgres pool exhaustion** (root cause of the 2026-05-04 incident). Every wide tagstore-touching code path now bounds its `asyncio.gather` fan-out via a shared `gather_bounded` helper using `TagStoreReaderConfig.max_concurrency`. Sites covered:
  - `_any_input_is_exchange` heuristic (`/<currency>/txs/{hash}?include_heuristics=all` on wide BTC txs)
  - `_add_labels` (every `list_*_neighbors include_labels=true` request)
  - `list_address_neighbors` per-neighbor `get_address` gather when `include_actors=true`
  - BFS fan-out in `clusters_service` (`recursive_search`, `bfs`)
  - Per-neighbor `db.get_entity` fan-out in `list_entity_neighbors`
- **Per-call `AsyncSession` amplification** in `entities_service.list_entity_neighbors`: replaced N×3 per-neighbor tagstore calls with three batched queries (`get_best_cluster_tags_for_clusters`, `get_nr_tags_for_clusters`, `get_actors_for_clusters`) sharing one Postgres session. Per-request session demand for `pagesize=100&include_actors=true` drops from ~300 to 1.
- **Per-call session reuse** in `entities_service.get_entity`: three sequential tagstore calls now share one `AsyncSession` (was 3).

### Web API + Python client (webapi-2.12.0)

No changes.

## [2.12.0] 2026-04-07

### Library (v2.12.0)

#### Added
- **`.gs` tx-graph encoder**: new `convert/gs_files` encoder/CLI to produce `.gs` files from transactions, used to render tx-graphs on the dashboard.
- **tx_id mismatch safety check** in async Cassandra access to surface inconsistencies early.
- **`tagpack-tool sync` locking**: optional file/Redis lock (per target DB) to prevent conflicting concurrent sync runs. Disable with `--no-lock`.
- **`tagpack-tool insert` repo logging**: the final "Processed N/M TagPacks…" message and Slack failure notification now include the repo/folder name.



#### Changed
- **Versioning**: documented dev-version scheme (new `VERSIONING.md`), reworked GitHub Actions publish workflows (PyPI + GitHub Packages) and CI tagging.
- **FastAPI** dependency upgraded.
- **Cassandra retries**: more robust retry handling in both sync and async drivers.

#### Fixed
- Block-range logging restored for `ingest --info`.
- Port config docs in environment / Cassandra settings.
- PostgreSQL session fan-out issues in tagstore-backed entity, tag, and cluster services (removes per-call `AsyncSession` amplification on BFS-style queries).
- Remaining gaps in the entity → cluster transition (REST models, addresses route/service, generated Python client `Cluster`/`NeighborCluster`/`NeighborEntity` models).

### Web API + Python client (webapi-2.12.0)

#### Added
- **Python client CLI MVP** (`graphsense` command): `raw` mirror of the OpenAPI surface, convenience commands, bulk command, output formatting/IO pipes, ext client/bundlers, full docs (`docs/cli/*`, `docs/ext/*`) and a dedicated test workflow.
- Improved CLI ergonomics: `rich-click` based help (coloring, option grouping), help shown when no args are given, improved error handling, more convenience commands and tests.

#### Changed
- Documentation now advertises `uv` as the recommended install path.
- Patched remaining gaps in the entity → cluster transition (Python client `Cluster`/`NeighborCluster`/`NeighborEntity` models).

#### Fixed
- CI workflow: correct tagging of `latest` for GitHub Packages publish.

## [2.11.0] 2026-04-29

### Library (v2.11.0)

#### Added
- **New ingest pipeline**: replaced ethereum-etl and bitcoin-etl with direct batch RPC for all chains.
- **Dual-sink pipeline**: `from-node --sinks delta --sinks cassandra` ingests to both Delta Lake and Cassandra in a single pass.
- **TRX gRPC source**: replaced HTTP-based TRX ingestion with native gRPC for higher throughput.
- **UTXO prevout resolution**: verbosity 3 support for BTC/BCH; `getrawtransaction`-based input resolution for LTC/ZEC. Removes the Cassandra dependency on ingest and enables input resolution for Delta Lake ingest.
- **ETH Pectra fields**: `requestsHash`, `authorizationList`, `y_parity`, `parentBeaconBlockRoot`, `uncles`, `creationMethod`.
- **EIP-2930/4844 Cassandra fields**: `access_list` stored in Delta and Cassandra schema.
- **Named S3 configs**: per-sink S3 references via `s3_configs` in `graphsense.yaml`.
- **Config validation**: warn on unknown keys at all nesting levels instead of failing; optimal `source_max_workers` defaults per currency; new `source_max_workers` knob for tuning RPC concurrency.
- **Sink-level locking**: independent locks for Delta and Cassandra sinks; single lock for ingest+compact.
- **Sink divergence detection**: refuse to ingest when Delta and Cassandra sinks have diverged.
- **`ingest_complete` marker**: bootstrap-marker state table for keyspace auto-discovery.
- **Node-restart resilience**: HTTP RPC and Tron gRPC retries now tolerate up to ~5 minutes of node downtime.
- `ingest` module added to ty type-checking scope.

#### Changed
- UTXO addresses stored as plain text instead of custom binary encoding in delta lake. (breaking, needs delta lake re-intest from node)
- Delta Lake writes and compaction use ZSTD level 5 compression.
- Reduced Delta pre-compaction file sizes by ~10× and lowered output cache limit.
- Increased Cassandra driver heartbeat timeout to avoid spurious retries.
- Tag summary: lower weight on `darkweb` and `unknown` tags; more emphasis on high-confidence tags.
- `semver-check` now accepts full SemVer 2.0 prerelease and build-metadata identifiers.
- Registry pattern for `dump.py`, decoupled transform/sink boundary.
- Obfuscation plugin RESt: easier toggle flags for debugging
- Replaced the `cashaddress` dependency with a local implementation.

#### Performance
- Significantly sped up Tron and Ethereum ingest (parallelized source I/O, chunk-level pipelining, gRPC instead of REST for Tron, faster hex/bytes conversions, in-place sorts, merged transform passes).

### Web API + Python client (webapi-2.11.0)

#### Added
- New `/{currency}/clusters/...` endpoints (`get_cluster`, `list_cluster_addresses`,
  `list_cluster_neighbors`, `list_cluster_links`, `list_address_tags_by_cluster`,
  `list_cluster_txs`, `search_cluster_neighbors`) that supersede the
  corresponding `/entities/...` endpoints. Both sets return identical data;
  new integrations should use `/clusters/...`.
- New `cluster` field on `Address`, `Cluster`/`Entity`, and `AddressTag` response
  models. Dual-emitted alongside the existing `entity` field.
- New `Cluster`, `NeighborCluster`, `NeighborClusters`, `ClusterAddresses` types
  in the generated Python client (subclasses of the `Entity*` types, so both
  are usable during the deprecation window).
- RFC 9745 `Deprecation: true` response header, RFC 8594 `Sunset` response
  header (per-route sunset dates) on the `/entities/...` endpoints, and a
  `Link` header with `rel="deprecation"` on every deprecated route. Clients
  can detect these without parsing the OpenAPI schema.
- Written deprecation policy in the API description (visible in `/docs` and
  in the generated spec).

#### Deprecated
- `/{currency}/entities/...` endpoints — use `/{currency}/clusters/...` instead.
- `entity` field on `Address`, `Cluster`, `NeighborEntity`, and `AddressTag` —
  use `cluster` instead.
- `status` field on `Address` — legacy field, no replacement.

All deprecated surfaces continue to work; see the "Deprecation policy" section
of the API description for the support window.

## [2.10.7] 2026-04-17

### Library (v2.10.7)

#### Fixed
- loading tags with invalid tron addresses failed with unhandled error


## [2.10.6] 2026-04-17

### Library (v2.10.6)

#### Changed
- improved tag validation output (stdout)

#### Fixed
- reduced false positive rate for coinjoin detection module.
- fixed disalignment of tag validation and db uniqueness constraints.


## [2.10.5] 2026-04-16

### Library (v2.10.5)

#### Fixed
- Tagpack validation now catches duplicate tags that would later violate the tagstore unique constraint after network/address normalization.
- Malformed BCH CashAddr values no longer abort processing during normalization; they are reported as warnings in validation and insert paths.

### Web API + Python client (webapi-2.10.0)
no changes

## [2.10.3] 2026-04-15

### Library (v2.10.3)

#### Fixed
- Thorbridge issue with unsuppored return shape (9a645b5557accbe5f6ba139ea637dc9315a20d9bdfedebf642a429ace19d45da)
- Swap issue with unspecified dst (b42ba68eb68bc4cff3b0f1069fd413912cc1ec0296e3e95f2c38d03bde337ced)

## [2.10.4] 2026-04-15

### Library (v2.10.4)

#### Fixed
- fix swap detection regression

### Web API + Python client (webapi-2.10.0)
no changes

## [2.10.3] 2026-04-15

### Library (v2.10.3)

#### Fixed
- Thorbridge issue with unsuppored return shape (9a645b5557accbe5f6ba139ea637dc9315a20d9bdfedebf642a429ace19d45da)
- Swap issue with unspecified dst (b42ba68eb68bc4cff3b0f1069fd413912cc1ec0296e3e95f2c38d03bde337ced)

### Web API + Python client (webapi-2.10.0)
no changes

## [2.10.2] 2026-04-15

### Library (v2.10.2)

#### Changed
- Improved retry handling for delta updates
- Sorted CoinJoin consensus sources by descending confidence for deterministic heuristics output
- Updated utxo heuristic parameters

#### Fixed
- Fixed loading all conversions when input is `root_trace`
- Fixed loading environment variables in Web subsystem (Tagstore parameters)
- thorchain nodes changed, more resilient http requests.

### Web API + Python client (webapi-2.10.0)
no changes

(The `/clusters/...` rename and `/entities/...` deprecation that previously
appeared here were merged after `webapi-v2.10.0` was tagged and ship in
`webapi-2.11.0`; see the [2.11.0] entry for details.)

## [2.10.1] 2026-04-03

### Library (v2.10.1)

#### Fixed
- Performance issues on large coinjoin txs e.g. 698a08f9d9fae6a4fde83501efd989e2b7392bbf9354ce60b921295315434a90
- Fixed heuristics caused errors on coinbase txs (no inputs)

### Web API + Python client (webapi-2.10.0)
no changes

#### Fixed
- Fixed Python client documentation examples and bad user input handling

## [2.10.0] 2026-04-02

### Library (v2.10.0)

#### Added
- Coinjoin detection heuristics for UTXO transactions
- Change address detection heuristics with configurable `include_heuristic` option (`all_change`)
- Currency safeguards for heuristics to prevent applying heuristics on unsupported networks
- Exchange tagging check for coinjoin heuristics
- Default values for ingest data configurations
- GitHub action to run examples on a regular basis
- Strict actor mapping in tagpack tool validation

#### Changed
- Tagpack tool validate by default checks actor taxonomy
- Updated dependencies

#### Fixed
- Fixed `TypeError: can't compare offset-naive and offset-aware datetimes` in exchange rates
- Fixed bad user input handling on IO access REST API

### Web API + Python client (webapi-2.10.0)

#### Added
- Heuristics for UTXO transactions (coinjoin and change detection)

#### Changed
- Removed extensions from Swagger/OpenAPI spec
- Internal service headers no longer exposed in REST API

#### Fixed
- Fixed Python client documentation examples and bad user input handling


## [2.9.12] 2026-03-25

### Library (v2.9.12)

#### Fixed
- fixed unhandled exception in only_ids parsing
- fixed loading of slack exception notification topics.

### Web API + Python client (webapi-2.9.9)

#### Fixed
- fixed (internal) header explicitly exposed in report tag endpoint


## [2.9.11] 2026-03-23

### Library (v2.9.11)
no changes

### Web API + Python client (webapi-2.9.8)

#### Fixed
- Fixed handling of "body" parameter in bulk requests of python client
- Fixed `_preload_content=False` being silently ignored in bulk requests, causing `FileNotFoundError` when streaming CSV into pandas


## [2.9.10] 2026-03-13

### Library (v2.9.10)

#### Fixed
- Fixed handling of swaps to unknown networks in utxo

### Web API + Python client (webapi-2.9.6)
no changes

## [2.9.9] 2026-03-12

### Library (v2.9.9)

#### Added
- REST startup now supports optional Tagstore schema initialization via `GSREST_ENSURE_TAGSTORE_SCHEMA_ON_STARTUP` when the Tagstore database has not been initialized yet.

#### Changed
- REST configuration can now be provided via the `web` section in `.graphsense.yaml`.
- REST `direction` query parameters remain optional but now only accept `in` or `out` when provided.
- REST API startup no longer fails when `gs-tagstore` is an optional dependency now. If it fails or is not configured a dummy tag provider is added.
- Swap queries for thorchain no longer raise errors on UTXO networks.

#### Fixed
- `graphsense-cli db block get-nr --date` no longer fails with `TypeError: can't compare offset-naive and offset-aware datetimes` when using the documented `%Y-%m-%d %H:%M:%S` input format.
- `graphsense-cli db block get-nr --date` now also accepts timezone-aware input in `%Y-%m-%d %H:%M:%S%z` format (e.g. `+00:00`).
- Testcontainer-based tests now work in Podman setups.
- Cross-chain fork handling now uses the correct address for cross-chain pubkey lookup.

### Web API + Python client (webapi-2.9.6)
#### Changed
- direction parameter for /txs endpoints are now an enum (in, out) instead of a string.
- /entities/{entity}/search is now deprecated.

## [2.9.8] 2026-02-26

### Library (v2.9.8)

#### added
- OpenAPI style-able docs with logo and better descriptions.
- Copilot repository instructions

#### changed
- `setuptools_scm` versioning scheme now uses `only-version` to support semver-style prerelease tags like `vX.Y.Z-dev.N` and avoid build-time `.dev` bump errors.
- Improved api documentation text.


### Web API + Python client (webapi-2.9.5)
no changes


## [2.9.7] 2026-02-25

### Library (v2.9.7)

#### changed
- CLI config loading now treats `web`, `tagpack-tool`, and `tagstore` as optional-config command groups, allowing these commands to run without a valid `.graphsense.yaml`.
- Top-level command detection in CLI config loading now skips global options (including `--config-file`) before resolving command-specific loading behavior.

#### added
- Integration tests for `graphsense-cli web openapi`, `graphsense-cli tagpack-tool --version`, and `graphsense-cli tagstore version` to verify behavior without a loaded GraphSense config file.

### Web API + Python client (webapi-2.9.5)
no changes

## [2.9.6] 2026-02-23

### Library (v2.9.6)

#### fixed
- Tagstore cluster mapping import: normalize pandas/numpy scalar values before PostgreSQL batch insert, preventing SQL errors like `psycopg2.errors.InvalidSchemaName: schema "np" does not exist` when `np.float64` values are present.
- Tagpack GraphSense query execution: replace warning-only handling of failed concurrent Cassandra statements with tenacity retries and hard-fail after retry exhaustion.

### Web API + Python client (webapi-2.9.5)
no changes

## [2.9.5] 2026-02-19

### Library (v2.9.5)

#### added
- Add git lfs to docker image

#### changed
- Docker image now bakes DuckDB `httpfs` extension and delta update loads `httpfs` reliably in containerized runs.~

#### fixed
- Tagstore ingest: Fix duplicate removal with different contexts

### Web API + Python client (webapi-2.9.5)

#### changed
- Changed versioning scheme

## [2.9.4] 2026-02-18

### fixed
- Fixed datetime-related regression in monitoring

## [2.9.3] 2026-02-18

### added
- Added Redis to ingest dependency group

## [2.9.2] 2026-02-18

### added
- Added Redis-based locking support via new config options `use_redis_locks` and `redis_url`

### fixed
- Fixed import chain in conversions to avoid unintended dependency from tagpacks to swaps by moving swap import to runtime

## [2.9.1] 2026-02-16

### fixed
- Fixed Python client PyPI publish action variable name

## [2.9.0] 2026-02-16

### added
- Added Python 3.12 and 3.13 support
- Added FastAPI-based graphsense-REST API module
- GSREST_DISABLE_AUTH env var to skip API key auth in openapi.json
- Added slow running regression tests

### changed
- Dropped support for Python 3.9 (EOL)
- web: Upgraded openapi generator v5 -> v7
- web: Added option to config web via graphsense.yaml
- Updated dependencies: ruff, pandas, requests, pyarrow, deltalake

### fixed
- Fix pagination page string conversion bug

## [2.8.19] 2026-02-05

### added
- UTXO receive detection for THORChain bridges
- `get_address_tx_range` for faster `list_address_txs`

### changed
- less strict tag deduplication procedure.
- updated Readme to promote uv use
- removed unused dependency aiohttp
- updated dependencies
- dropped calender versioning scheme, only semvar from now
- improved THORChain bridge matching
- improved `get_address` speed

### fixed
- token_currency detection for THORChain ETH→token swaps
- UTXO tx lookup in match_sending_transactions


## [25.11.18/2.8.18] 2026-01-28

### added
- Thorchain Bridges: Support ETH direct vault deposits where memo is in tx input data (no router logs)
- Thorchain Bridges: Validate deposit addresses by checking single outgoing neighbor is a known router
- Thorchain Bridges: Add script_hex exposure in TxValue for UTXO OP_RETURN memo parsing
- Thorchain Bridges: Use DB-first approach for UTXO networks with Thornode API fallback for older blocks

## [25.11.17/2.8.17] 2026-01-26

### added
- Faster delta updater by translating in-relations to out-relations and no balance and relation queries for new addresses.

## [25.11.16/2.8.16] 2026-01-26

### changed
- Move dependencies from tagpacks to tagstore

## [25.11.15/2.8.15] 2026-01-26

### changed
- Move dependencies from tagpacks to tagstore

## [25.11.14/2.8.14] 2026-01-26

### added
- Optional dependency group for tagstore. Tagstore remains included in tagpacks

## [25.11.13/2.8.13] 2026-01-23

### changed
- Delta updater: Restructured Cassandra and Delta Lake queries

### added
- More logging in delta updater

## [25.11.12/2.8.12] 2026-01-15

### changed
- Actorpacks: Move aliases to context field
- Improved logging in tagpack-tool: Send errors to slack; cassandra retries are now warnings, not errors

### fixed
- Tagpack insertion is now atomic - removed early commit

### added
- Support for aliases on tagpack insert
- Faster tagpack reading and validation
- --use-pyyaml flag to use legacy reader for tagpack insert and validate

## [25.11.11/2.8.11] 2025-12-17
### fixed
- Tagpack actor validation

## [25.11.10/2.8.10] 2025-12-15
### fixed
- better retry on ingest (also for prepared statements)
### changed
- cash table column lookups for requests for 10 minutes

## [25.11.9/2.8.9] 2025-12-11
### fixed
- Fix unexpected behavior of resolve_tx_id_range_by_block where min_height was ignored when larger than the current highest block. List address transaction now return an empty response if min_height>current_highest_block.

## [25.11.8/2.8.8] 2025-12-11
### added
- add concept of deposit_wallet

## [25.11.7/2.8.7] 2025-12-10
### changed
- add flag to disable strict data checks, useful for debugging and testing

## [25.11.6/2.8.6] 2025-12-10
### changed
- Account ingest now always resyncs the last to batches, for easier error recovery.
### fixed
- fix traces not found handling.

## [25.11.5/2.8.5] 2025-12-5
### fixed
- bch spurious btc like address in tx f39592c35da4260b06baa47f62a181fe95b3d7b45b5205879552b4b22c852abf

## [25.11.4/2.8.4] 2025-11-24
### fixed
- http connection issues delta lake

## [25.11.3/2.8.3] 2025-11-20
### Changes
- added fee field to accountTxs

## [25.11.2/2.8.2] 2025-11-19
### Changes
- removed unknown network warnings in tagpack validation
- actor recommendation without db connection
- new flag --auto-compact for automatic delta lake compaction on a schedule
### fixed
- delta lake connection issues when using when using union_by_name=True


## [25.11.1/2.8.1] 2025-11-13
### fixed
- fixed parsing error in enum for tag inheritance marker

## [25.11.0/2.8.0] 2025-11-06
### changed
- Tags: tag summary only propagates actors from high confidence tags
- Search: search now support more config options to select what to include
- Search: search for more address patterns eg. addresses with 0x33d0 short prefixes and postfixes, e.g. 0x33d0...8f65
- Chore: better retry handling and logging
### added
- Tags: tags are now derived from other chains if the addresses are derived from the same pubkey
- Tags: tag type attribute which is not used tag summary actor propagation (e.g. for tags like sanction lists this is useful)
- Tags: introduced attribute tags and improve tag summary actor inheritance
- Tags: new tag concepts funder, deployer, white_list, black_list, gov_white_list
- Tags: tag summary now supports transformation before digest computation (for e.g. redacting private information)
- Tags: option to avoid data leaks to slack for tag notifications
- Schema: new fields to utxo raw keyspace script_hex for inputs and outputs txinwitness for inputs
- Schema: new fields for account raw keyspace vrs (signature data)
- Schema: added migration support for cassandra schemas
- Ingest: credential support for grpc endpoints
### fixed
- error for swaps where graph is not weakly connected
- error for bridge txs with nonsensical affiliate fee data


## [25.09.7/2.7.7] 2025-10-03
### fixed
- handling of thorchain bridges that target an unsupported asset. (e.g. cd884dafc0e2294be028dfc41d3a7d043e0a36b94b112339993d753f50b27677)


## [25.09.6/2.7.6] 2025-10-03
### fixed
- handling of exotic tron transactions (on addr, TMNS5BrLWVYiNDSgHvxmuojoPEhq9cPddM and tx f0b31777dcc58cbca074380ff6f25f8495898edba2da0c43b099b3f276ae3d74)


## [25.09.5/2.7.5] 2025-10-02
### fixed
- logging instead of exception for unknown wormhole bridegeing strategy

## [25.09.4/2.7.4] 2025-09-30
### fixed
- add exponential backoff for ingest retries.

## [25.09.3/2.7.3] 2025-09-16
### fixed
- allow datetime values in lastmod of tagpack and tag, instead of only date

## [25.09.2/2.7.2] 2025-09-15
### changed
- added better handling for thorchain bridge
- updated btc/eth-etl dependencies (added new fields txinwitness, vrs)

### added
- code to compute pubkey from vrs eth
- typing checks via ty


## [25.09.1/2.7.1] 2025-09-05
### changed
- added retry logic for bridging requests

## [25.09.0/2.7.0] 2025-09-04
### added
- added services layer form gs-rest
- added tagpack-tool and gs-tagstore-cli functionality (See Readme)
### changed
- improved swaps and bridge decoding support


## [25.08.0/2.6.0] 2025-08-07
### added
- bridging support to conversions endpoint
- moved database access to gslib from gs-rest

### changed
- renamed swap extra dependencies to conversions
- support for python 3.11

### fixed
- uniform tx id handling in rest-interface

## [25.07.3/2.5.3] 2025-07-08
### added
- add optional environment to slack logging handler
- add default_environment to gs_config

## [25.07.2/2.5.2] 2025-07-04
### added
- Slack logging handler

## [25.07.1/2.5.1] 2025-06-26
### added
- monitoring monitor-raw-ingest cli command

## [25.07.0/2.5.0] 2025-06-25
### added
- some utility functions to harmonize with gs-rest
- added support for cassandra user and password authentication
### changed
- improved algo for swap detection and analysis
- added optional dependencies swaps, ingest, all

## [25.06.0/2.4.11] 2025-06-02
### added
- event signatures/decoding for swaps and trading pair creation
### changed
- change from pyScaffold -> uv, black; isort; flake8 -> ruff

## [25.03.2/2.4.10] 2025-03-28
### changed
- higher default timeout to avoid errors on big inserts

## [25.03.2/2.4.10] 2025-03-28
### changed
- higher default timeout to avoid errors on big inserts

## [25.03.1/2.4.9] 2025-03-14
### changed
- better retry handling on big inserts

## [25.03.0/2.4.8] 2025-03-07
### changed
- updated dependencies, goodconf, pydantic etc.
### added
- database tests via testcontainer
- testing of exchange rates import
- vcr for tests with web dependencies
- ruff instead of flake8, black and isort

## [25.01.0/2.4.7] 2025-01-02
### fixed
- Delta updater now marks contract addresses for eth and tron

### changed
- Updated deltalake dependency to 0.22.3

## [24.08.5/2.4.6] 2024-12-11
### fixed
- parse address for anchor output

## [24.08.5/2.4.5] 2024-12-11
### fixed
- allow anchor script type in btc-like currencies

## [24.08.4/2.4.4] 2024-11-11
### fixed
- delta update failed after erigon 3 update, missing reward traces

## [24.08.3/2.4.3] 2024-11-1
### fixed
- handle no tx > int32 max for trx (truncate)

## [24.08.2/2.4.2] 2024-10-31
### fixed
- allow null values in binary columns for delta tables

## [24.08.1/2.4.1] 2024-08-22
### fixed
- cleanup of print and log statements

## [24.08.0/2.4.0] 2024-08-20
### changed
- removed ingest to-csv, replacement is export to delta lake, which is more efficient
- renamed delta lake-commands ingest dump-rawdata -> ingest delta-lake ingest; ingest optimize deltalake -> ingest delta-lake optimize
- removed fs-cache helper for trx and eth delta-update, now uses delta lake directly
- removed typechecked dependency, removed disk-cache dependency

## [24.07.7/2.3.7] 2024-07-16
### fixed
- tron delta-dump: fix missing transferto_address in some tron traces

## [24.07.6/2.3.6] 2024-07-15
### fixed
- tron delta-dump freezes on grpc asyncio requests
- safer handling of ctrl-c on delta-dumps

## [24.07.5/2.3.5] 2024-07-08
### fixed
- increase timeout limit for s3 requests from the default 30s to 300s

## [24.07.4/2.3.4] 2024-07-08
### added
- Allow optimizing single delta table
### fixed
- Add timeout for grpc calls to fix freezing of trx ingest

## [24.07.3/2.3.3] 2024-07-02
### fixed
- limit compaction parallelism delta lake

## [24.07.2/2.3.2] 2024-07-02
### fixed
- evaluating tables to fix in optimize deltalake step, remove direct boto3 dep.

## [24.07.1/2.3.1] 2024-07-02
### fixed
- passing s3 credentials to boto3

## [24.07.0/2.3.0] 2024-07-02
### added
- Write raw data to delta tables on s3 or local using graphsense-cli dump-rawdata
- graphsense-cli optimize-deltalake to optimize tables of a currency (vacuum and/or compact)

## [24.02.10/2.2.10] 2024-06-17
### fixed
- Fixing release tag issue

## [24.02.9/2.2.9] 2024-06-17
### fixed
- numpy 2.0.0 problem (numpy.dtype size changed error)
### added
- cryptocompare exchange rates to have a free version again (graphsense-cli exchange-rates cryptocompare)
- graphsense-cli trace event to print prettyfied event logs for tron and eth

## [24.02.8/2.2.8] 2024-05-28
### fixed
- fixed coingecko z-cash currency key to fetch exchange rates
### added
- coinmarketcap allow configuration of api key for pro api (free is not available anymore)

## [24.02.7/2.2.7] 2024-05-28
### added
- graphsense-cli exchange-rates coingecko to allow fetching exchange rates via coingecko pro api

## [24.02.6/2.2.6] 2024-04-10
### fixed
- csv export with new version of ethereum etl 2.4

## [24.02.5/2.2.5] 2024-04-08
### fixed
- performance problem (timeouts) on fetching transactions per block for utxo currencies.

## [24.02.4/2.2.4] 2024-03-19
### fixed
- tron delta update: missing tx_hash for traces in deployment txs.

## [24.02.3/2.2.3] 2024-03-11
### changed
- Changed number of backoff blocks used in ingestion to avoid spurious data (mostly lowered)

## [24.02.2/2.2.2] 2024-03-06
### fixed
- gracefully handle inconsistencies in address relations

## [24.02.1/2.2.1] 2024-03-04
### fixed
- Warning instead of exception on ingest filelock timeout

## [24.02.0/2.2.0] 2024-03-04
### changed
- full delta updates for tron and ethereum
- block tx table to long format instead of Cassandra lists (breaking)
### fixed
- off by one error in utxo delta updates

## [24.01.2/2.1.2] 2024-02-07
### changed
- change consistency level Cassandra, consistency_level=LOCAL_QUORUM, serial_consistency_level=LOCAL_SERIAL

## [24.01.1/2.1.1] 2024-02-07
### fixed
- address.first_tx_id and last_tx_id should be long type

## [24.01/2.1.0] 2024-01-09
### added
- ingest now works stores additional details/tables in raw keyspace (tx_type, fees)
- new field for address table, zero value tx stats (eth and trx)
- graphsense-cli config get --path function to access config values for scripting
### changed
- more robust retry handling on ingest
### fixed
- minor bug with system.exit handling and slack notifications
- timestamp micro instead of milliseconds bug trx transactions

## [23.09/2.0.0] 2023-11-21
### fixed
- new pk for summary stats to avoid duplicate entries. Breaking: needs recreation of table

## [23.09/1.8.3] 2023-11-07
### fixed
- ingest default config to raw keyspace on create to avoid problems.

## [23.09/1.8.2] 2023-10-24
### fixed
- handle error missing quotes field on coinmarketcap exchange rates ingest

## [23.09/1.8.1] 2023-10-06
### fixed
- handle zcash shielded inputs in import

## [23.09/1.8.0] 2023-10-02
### Added
- added flag forward-fill-rates to allow transform even if no current rates are available (last rate avail is used)

## [23.09/1.7.6] 2023-10-06
### fixed
- fix performance degradation on because of inefficient config lookups

## [23.09/1.7.5] 2023-10-02
### fixed
- (critical) delta update only inserts coinbase txs

## [23.09/1.7.3] 2023-09-21
### fixed
- setup automatic pypi publish with github actions

## [23.09/1.7.1] 2023-09-20
### Added
- ingest/delta update test script to setup a fully functional Cassandra instance for development (script/dev-ingest.sh)
### Fixed
- fixed bug on empty output list on coinbase txs.

## [23.06/1.7.0] 2023-09-12
### Added
- delta updater support for pseudo coinbase address

## [23.06/1.6.1] 2023-09-11
### Fixed
- inconsistent db state after write timeout -> added retry logic for delta updater on write timeouts

## [23.06/1.6.0] 2023-08-18
### Added
- ingest for utxo now creates new tables for transaction references

## [23.06/1.5.0] 2023-06-12
### Added
- added cli ingest command (ingest from-node) for ethereum-like currencies [#6](https://github.com/graphsense/graphsense-ethereum-etl/issues/6)
- added cli ingest command to export node data to csv
- added cli ingest commands (ingest from-node) for btc-like currencies [#4](https://github.com/graphsense/graphsense-bitcoin-etl/issues/4)
- add ingest to parquet files as ingest output option, additional to cassandra [#2](https://github.com/graphsense/graphsense-lib/issues/2)
- alpha support for transaction-monitoring [#4](https://github.com/graphsense/graphsense-lib/issues/4)
- compatibility with tron data in raw keyspaces [#3](https://github.com/graphsense/graphsense-lib/issues/3)

### Fixed
- delta updater bug with zero value and zero fee txs in btc

## [23.03/1.4.0] 2023-03-29
### Added
- added cli command graphsense-cli db logs get-decodeable-logs to decoded logs in a given block range.
- added all event definitions to decode all USDT event logs

## [23.01/1.3.0] 2023-01-30
### Added
- added keyspace name to monitoring output
- slack notifications and cli notify endpoint
- exception notification via slack
- bash completion file generation
- enable specifying a config file (allowing mulitple configs)
- initial support for decoding eth logs
- functions to efficiently find the closest block to a given date and vice versa

### Fixed
- delta updater fixed skipped blocks
- error when data is up to date
- getting highest block with exchange rates

## [22.11/1.2.0] 2022-11-23
### Added
- Delta updater v2 for utxo currencies
- Config flag to disable delta updater
- Simple monitoring of database state
- Colorized output
- More readable logger format

### Changed
- Changed schema files to reflect the current version of the graphsense db

## [1.1.0] 2022-10-11
### Changed
- Initial release
