# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Version Tracks

- `vX.Y.Z` tags track **Library** releases.
- `webapi-vA.B.C` tags track **Web API + Python client** releases.

Use one changelog file, but separate entries by track in each release window.

## [2.15.0] - unreleased

### Library

#### Added
- **New `convert scan-for-addresses` CLI command scans text/SQL files (and compressed containers) for cryptocurrency addresses.** Extraction is deliberately permissive; the real filter is checksum validation (reusing `utils/address.py:validate_address`), which drops the many address-shaped false positives in DB dumps (hashes, session ids, base64 blobs). Validates BTC (legacy/bech32), LTC (legacy/bech32), ETH (EIP-55), TRX, ZEC (transparent) and XRP; reports XMR as candidates. Compressed inputs (gzip/zlib/bz2/xz/zip/tar, recursively, plus GraphSense `.gs` files) are unwrapped transparently, with `--carve` to inflate zlib/gzip streams embedded in binaries and a decompression-bomb budget. Opt-in `--tx-hashes` reports 64-hex tx-hash *candidates* (format-only, not checksum-verifiable — the command warns that SHA-256 digests/tokens are picked up too). Supports `--json`, `--context`, and rejected-candidate listing. The `convert` command group is now also reachable as `file` (e.g. `graphsense-cli file scan-for-addresses ...`). Adds `validate_xrp_address` (and `xrp`) to `utils/address.py:validate_address`.
- **MCP `build_pathfinder_file` can now return an `open_url` deep link that opens the built graph directly in Pathfinder (feature-flagged, off by default).** The Pathfinder dashboard's `?import=<id>` loader takes the file store's opaque download token and fetches `<REST>/download/<id>` itself, so alongside `download_url` the tool mints `{pathfinder_base_url}/pathfinder?import=<token>` (base URL from the existing `GS_MCP_PATHFINDER_BASE_URL` setting, default `https://app.iknaio.com`). Gated behind the new `GS_MCP_PATHFINDER_OPEN_URL_ENABLED` flag (default `false`): when off, the link is not minted, the `open_url` key is omitted from the structured content, and the feature is not advertised anywhere — the open-url passages of the tool description and of the bundled server instructions (marked with `<!-- feature:pathfinder-open-url -->` comments) are stripped at registration/handshake time. When on, the link is included in the structured content, the text fallback block, the tool description and the server instructions; it only needs the stored token, so it survives `download_url` link-building failures. Requires the file store to be enabled and the dashboard to point at the same REST host.
- **SunSwap V3 `PoolCreated` events are now decoded, restoring DEX-pair detection on TRON.** SunSwap V3 is TRON's main concentrated-liquidity DEX and a Uniswap V3 fork, but its factory appends a trailing `poolLength` field to the `PoolCreated` event. That extra parameter changes the event signature and therefore its `topic0`, so the log did not match the existing Uniswap V3 `PoolCreated` entry and was silently skipped. A dedicated ABI entry for the SunSwap V3 `topic0` (`0x20a108fa…`) is added to `log_signatures` in `datatypes/abi.py`; `token0`/`token1`/`fee` remain indexed and the extra `poolLength` field is decoded but unused, so `get_pair_from_decoded_log` (which keys on the `PoolCreated` name) now picks up SunSwap V3 pairs.
- **Unpegged tokens (no USD/EUR/ETH `peg_currency`) are supported and priced from a new per-token exchange-rate track.** Such tokens now flow through ingest and the API instead of raising in the value-conversion path (`map_rates_for_peged_tokens`) or the ingest price computation (`get_prices`), which previously rejected anything not pegged to ether/euro/usd (limiting the set to stablecoins and wrapped native coins). Mirroring the native rate pipeline, the `rates … <provider> ingest` commands (and the pre-ingest hook run by `ingest`) fetch daily prices for every unpegged token in `token_configuration` across **all** providers (coingecko, coinmarketcap, cryptocompare; coindesk is BTC-only and skipped), preferring the token's **contract address** and falling back to its ticker. Prices land in a new raw `token_exchange_rates(asset, date)` table; the delta-update maps them to per-block rows in a transformed `token_exchange_rates(asset, block_id)` table and uses them to price stored aggregates (`total_tokens_received`/`_spent`, relation `token_values`) and per-tx token values / balances. Tokens with no fetched rate carry their raw on-chain amount with empty/zero fiat (the retained fallback). `peg_currency` may now be null on `token_configuration`; `--no-token-rates` skips fetching. Historical aggregates reflect real fiat only for newly (re)processed blocks — a backfill/re-transform fills older data.

#### Changed
- **The `chainside-btcpy` dependency is gone; the native output-script parser is now the only `parse_script` implementation.** The btcpy-free parser ran in production shadow mode against btcpy for a month (2026-06-03 → 2026-07) with the one divergence found fixed (the 2.14.1 LTC nonstandard-script incident) and no further mismatches, so the legacy btcpy parser, the shadow comparator, and the `chainside-btcpy` package (6 years unmaintained, LGPLv3) are removed from the `ingest` extra. Parser behaviour is unchanged — the native implementation is byte-identical to btcpy on every probed and fuzzed input, quirks included. Dropping btcpy also frees its `ecdsa==0.13` pin, letting `ecdsa` float from the 6-years-stale 0.13 to current 0.19.x (`ecdsa` is only the `fast=False` fallback behind coincurve in pubkey validation).
- **`setuptools` is no longer a runtime dependency, and the `<80.9` version caps are gone.** The caps only silenced the `pkg_resources` deprecation warning; nothing imports `pkg_resources` (or `setuptools`) at runtime anymore — our code was ported to `importlib.resources` long ago and the remaining consumers (grpcio-tools, sentry-sdk) only use it on Python < 3.9, below this project's floor. Verified against setuptools 83: test suite, warning-free import, and a wheel build whose payload is identical to the previously pinned build. The build backend and the `testing`/`dev` groups keep an uncapped `setuptools`. *Correction:* one runtime consumer was missed — Spark jobs. pyspark 3.5 imports `distutils.version` at runtime, and on Python ≥ 3.12 `distutils` only exists through setuptools' shim, so dropping it broke `createDataFrame(pandas_df)` in the Python-3.13 Docker image (`ModuleNotFoundError: No module named 'distutils'`). `setuptools` is back as a dependency of the `transformation` extra (next to pyspark) until pyspark ≥ 4 drops the distutils import.
- **Token support scales to hundreds–thousands of configured tokens (was limited to a handful), switchable via one config flag.** Several serving paths previously expanded *every* supported token into a separate concurrent Cassandra query per request (address/entity tx listing, neighbor links, per-address balances). Now the fan-out is bounded to the tokens an address *actually* used — read direction-aware from the address row's `total_tokens_received`/`total_tokens_spent` maps (new `get_address_token_assets` helper) — per-address balances are fetched in a single partition read (`currency` is a clustering column of the `balance` table), and the sync `get_token_configuration` no longer caps at `LIMIT 100`. A new database option `fanout_bounding_and_links_precheck_enabled` (default `true`, env `GS_CASSANDRA_ASYNC_FANOUT_BOUNDING_AND_LINKS_PRECHECK_ENABLED`) gates this bounding plus the links relations pre-check (immediate empty result when no directed edge exists; early stop once all `no_transactions` edge txs are found); set it `false` to restore the previous unbounded/full-scan behavior if the aggregates are incomplete (e.g. an outdated transform). Independently, the serving path now warns when an address's token maps reference a token **not** in `token_configuration` (invisible to the per-token queries either way), pointing operators at the real fix — adding the token.
- **RPC UTXO ingest now aborts on unresolved inputs instead of silently writing them null.** When a spent input cannot be resolved to a value/address after the output cache and `getrawtransaction` — which in practice only happens on an already-misconfigured node (missing `txindex=1` or pruned) — the exporter previously wrote the input with null value/addresses, silently corrupting fees, balances, address relations and clustering downstream. It now raises with a descriptive message naming a sample offending tx, so the misconfiguration surfaces at ingest instead of as corrupt data later. A new config flag `fail_on_unresolved_inputs` (**default `true`**) controls this; set it `false` to restore the old warn-and-write-null behavior. It is superseded by `fill_unresolved_inputs=true`, which fills dummy values (value=0, `nonstandard` address) instead — if that is set, ingest does not abort. Coinbase inputs (no `spent_transaction_hash`) never trigger it.

#### Fixed
- **The account (ETH/TRX-style) delta-updater no longer inflates total supply by the gas fees of failed transactions.** The miner is credited the fee for every transaction including reverted ones (`txFeeDebits`, unguarded), but the payer was only debited `if tx.from_address in address_hash_to_id` — and a sender that appears *only* in failed transactions is absent from the successful-trace address set, so its debit was silently dropped and the fee minted from nowhere. Such fee-only senders are now added to the address set (getting an id + a zero-stat address row with the `-1` tx sentinel, mirroring miner rewards) so their gas-fee debit lands and the fee balances. This is a forward-only delta-updater correction: the addresses it introduces will diverge from the Spark-transformed keyspace until the corresponding Spark fix is applied via a full re-transform. The REST API already tolerates these zero-tx (`first_tx_id = -1`) address rows.
- **`address_id` resolution is now cached, cutting redundant Cassandra point-lookups across the read path.** `CassandraDb.get_address_id` backs `get_address`, `get_address_entity_id`, `list_address_txs_ordered` and the links path, so the same address was re-resolved several times per request (and repeatedly across requests for hot addresses). It now uses a hit-only `@alru_cache`: assigned `address_id`s are immutable so hits are cached, while misses raise an internal sentinel (`_AddressIdNotFound`) so a not-yet-ingested address is never cached and is always re-resolved. Reduces coordinator load and tail latency more than average single-request latency.
- **`/{addresses,entities}/{id}/links?neighbor=…` timeouts on large Ethereum/TRON addresses are reduced in two edge cases** (large-address links queries in general can still be slow). The links query (re)discovers the directed edge `id → neighbor` by paging the smaller node's `address_transactions` history. `CassandraDb.list_links` now first point-looks-up the edge in the `*_outgoing_relations` tables (reusing `list_neighbors(..., targets=[neighbor])`), which helps exactly where the old behavior was pathological: (1) when no direct edge exists — e.g. two exchange-scale addresses with no shared txs — the query returns immediately instead of walking the full history into the 30 s timeout, and (2) once all of the edge's `no_transactions` txs have been found, paging stops instead of scanning the rest of the partition. Queries whose edge txs sit deep in a large history behave as before. The cluster/entity links path also gains the `asyncio.wait_for` request-timeout wrapper the address path already had (reusing `address_links_request_timeout`), so slow cluster-links queries fail fast instead of hanging.
- **`links?neighbor=…&token_currency=…` restricts the counterparty fetch to the requested token.** In `list_links` the `second`-side fetch previously pulled the native asset alongside the token for every page, which (a) let non-token events leak into a token-filtered result — e.g. the native contract-call trace when the neighbor is the token contract, which passed the address-only filter but is not a token transfer — and (b) multiplied the per-page fan-out against a possibly huge counterparty partition. It now fetches only the requested token's rows; a genuine token transfer still produces a token row on the second side so no real link is dropped. (Non-token queries are unchanged.)
- **Listing address transactions on account-model networks no longer 500s while the delta-updater is writing the current batch.** A tx id read from `address_transactions` could not yet be resolved in `transaction_ids_by_transaction_id_group` — the updater shards its data writes without cross-table ordering, so mid-batch readers can see one table before the other — and `list_txs_by_ids_eth` crashed with `TypeError: 'NoneType' object is not subscriptable` (seen in prod on `/eth/addresses/<a>/txs?direction=out&order=desc`). Such in-flight txs are now dropped from the response (with a warning log) and appear on the next request. To avoid masking real data inconsistencies, the tolerance is tightly scoped: summary statistics are committed strictly after all data writes, so a legitimately in-flight tx always lies **above** the last committed block — misses at or below it still raise, now as a `DBInconsistencyException` naming the tx ids and table instead of a bare `TypeError`. Result alignment with the requested ids is preserved via `None` placeholders so `normalize_address_transactions` cannot silently pair wrong tx data with wrong ids.
- **Sporadic `CRC mismatch on header …` failures during synchronous reads are now retried automatically.** The error is a client-side protocol-v5 segment checksum failure in the Python driver (upstream, unresolved: [CASSANDRA-19971](https://issues.apache.org/jira/browse/CASSANDRA-19971) / PYTHON-1337), typically triggered under load on lz4-compressed responses — not data corruption on the node. `CrcException` is added to `TRANSIENT_DB_ERRORS`, so `CassandraDb._execute_with_backoff` rides it out like the other transient errors; since the driver defuncts the affected connection, the retry runs on a fresh one and typically succeeds on the first attempt. Previously the error surfaced straight to callers (e.g. `db logs get-dex-pairs` logging `Failed to process block …` and deferring the block to the next run).
- **`CassandraDb.execute(keyspace=…)` no longer depends on native protocol v5 to target the right keyspace.** The statement-level keyspace flag is silently dropped by the driver on protocol < v5, which would have sent bare-table-name queries (notably the schema migration runner's `UPDATE configuration …`) to whatever keyspace the session had active. `execute` now also switches the session keyspace for the duration of the call whenever it differs from the requested one, making the behavior protocol-version-independent.
- **`db logs get-dex-pairs` no longer hangs at shutdown (and no longer drops trailing result batches).** The writer's stop sentinel was sent by the last reader to shut down, but `multiprocessing.Queue` is only FIFO per producing process — each process flushes items through a background feeder thread — so the sentinel could overtake another reader's still-buffered final batch. The writer then exited early ("Got None, writer out.") while that batch was stuck in a now-unread, full pipe; the owning reader could never flush it, its `join()` blocked forever, and the batch was silently lost. The main process now joins all readers first (the writer is still consuming, so every feeder thread can drain) and only then sends the writer sentinel itself; the reader-count bookkeeping (`MPCounter`) is gone from the command.
- **`db logs get-dex-pairs` now works end to end instead of failing on every block and crashing the writer.** Three bugs were compounding: (1) `get_pair_from_decoded_log` read the raw Cassandra `Row` (a namedtuple) with string subscripts (`log_raw["tx_hash"]`), raising `tuple indices must be integers or slices, not str` for every log — fixed to attribute access (`log_raw.tx_hash` / `log_raw.log_index`); (2) the writer's shutdown path called `logger.log("…")` without a level, crashing with `TypeError` — fixed to `logger.info`; (3) per-block errors killed the whole worker (and, once made non-fatal, exposed a deadlock because no shutdown sentinels were ever enqueued). Readers now log-and-continue per block, one `None` sentinel is sent per worker so `join()` terminates, and failed blocks are threaded back to the writer and left un-marked so a re-run retries only them.
- **Transaction identifiers with an unknown sub-transaction type prefix (e.g. `<hash>_Q1`) returned 500.** The sub-transaction parser raised a bare `Exception`, bypassing the invalid-input handling of every endpoint that accepts a tx hash; it now raises `ValueError`, which surfaces as a 400 with a proper message.
- **`tagpack`/`actorpack insert` no longer crashes on a detached-HEAD checkout that is not a tag.** Building the backlink URI resolved the tree name via `repo.active_branch`, with a fallback that only handled tag checkouts — on any other detached HEAD (e.g. GitHub Actions checking out a PR merge commit) it crashed with `AttributeError: 'NoneType' object has no attribute 'name'`, which is why the tagstore test fixtures failed on pull-request CI runs while push builds passed. The tree name now falls back to the commit sha, which is equally valid in a `/tree/<name>/` URL.
- **Tagpacks that combine header-level `concepts` with per-tag `category`/`abuse` no longer cross-contaminate concepts between tags.** `Tag.__init__` appended into the shared header-level `concepts` list in place, so every tag in such a pack accumulated the union of all tags' category/abuse values, producing wrong `tag_concept` rows on ingest. The list is now copied per tag. Tagpacks ingested previously under this pattern need re-ingest.
- **UTXO delta-updater summary statistics no longer inflate quadratically in per-tx (`TX`) application mode.** `get_bookkeeping_changes` accumulates its deltas into the statistics row in place and runs once per tx, but was handed the running totals instead of the current tx's deltas, so a 3000-tx block added ~4.5M to `no_transactions`/relation counters instead of 3000. It now receives per-tx deltas; `BATCH` mode was already correct.
- **Undecodable ERC-20/TRC-20 `Transfer` logs are logged instead of silently dropped.** The token-transfer decoder swallowed every exception and returned `None` with no signal (an in-code `TODO` admitted it), hiding data loss; it now emits a warning with the log index and token address.
- **UTXO watchpoints fire in the correct direction and honour value thresholds.** Received outputs were classified as outgoing flows, so `on_incoming` never fired and `on_outgoing` fired on receipts for all UTXO chains; outputs are now incoming. The JSON watchpoint provider also read a misspelled `value_lgt` key, silently discarding `value_gt` thresholds — it now reads `value_gt` (falling back to the old key for existing configs).
- **Slack webhook posts no longer hang without a timeout.** `requests.post` in the Slack utility had no timeout and is called synchronously on error paths (including the API 500 handler on the event loop), so a blackholed webhook host could stall workers during an error storm. Both posts now use a bounded connect/read timeout.
- **`find_highest_block_with_exchange_rates` no longer crashes on non-monotonic block timestamps at the UTC day boundary.** Block timestamps in UTXO chains are not strictly monotonic (miner clock skew up to ~2h), so right after midnight UTC a block can map to the new — still rate-less — calendar day while a *later* block still maps to the previous day (observed 2026-07-08: block 957103 stamped 00:00:56 UTC without rates, block 957104 stamped before midnight with rates). The frontier search assumed a clean present→absent boundary and died with a bare `AssertionError`, blocking the delta updater. It now lowers the frontier to the highest block below which rates are contiguous (logged as a warning), so `forward_fill_rates` covers the skewed blocks; a gap wider than the 200-block skew window (a real hole: missing rate day or missing blocks) raises a descriptive `ValueError` instead.
- **Exchange-rate import treats "DB already at/past the fetch window" as a no-op instead of an error exit.** When the newest rate in the raw `exchange_rates` table lies beyond the end date (which defaults to yesterday, the last complete day) — e.g. after manually forward-filling today's rate — all four providers (cryptocompare, coindesk, coingecko, coinmarketcap) aborted with `Error: start date after end date.` and a non-zero exit, breaking update pipelines. The resume case now logs "nothing to fetch" and exits cleanly; an explicitly passed start date after the end date remains a hard error.
- **The batch trace exporter no longer retries `trace_block` up to 225 times against a dead endpoint.** `_fetch_traces_for_blocks` wrapped `make_batch_request` — which already retries transient failures 15× with exponential backoff and session reset — in a second 15× retry loop, nesting to 15×15 attempts and hammering an unreachable node for over an hour before failing. The redundant outer loop is removed; the single inner retry is strictly more robust.
- **The TRON gRPC exporter now warns when it fabricates receipts for transactions missing `TransactionInfo`.** Such txs were silently recorded with `status=1` (success), zero fee/energy, and no logs or traces — indistinguishable from a genuine free successful tx. A per-block warning now reports how many receipts were fabricated so the data loss is visible.
- **`tagpack` cluster-mapping no longer marks unmapped addresses as mapped.** `finish_mappings_update` flagged every not-yet-mapped address in a network as `is_mapped=true` even when its mapping was skipped/failed (e.g. the address is not yet in the graph keyspace), permanently excluding it from future incremental runs. It now flags only addresses that actually received a row in `address_cluster_mapping` (join-based), and self-heals once the mapping exists.
- **Legacy sync-`TagStore` quality/actor CLI queries work again.** Four commands (`quality list_addresses_with_low_quality`, `quality list_labels_without_actor`, `quality list_addresses_with_actor_collisions`, `actorpack list_address_actor`) referenced columns dropped from the schema (`tag.address`, `tag.category`, `address_quality.address`) — two crashed, two swallowed the error into "Operation failed" and silently returned nothing. Ported to the current schema (`identifier`, `tag_concept`, `tag.actor`; the wrong `t.label = a.id` actor join is now `t.actor = a.id`).
- **The MCP endpoint no longer returns `421 Misdirected Request` behind a reverse proxy on FastMCP ≥ 3.4.3.** FastMCP 3.4.3 added DNS-rebinding protection that validates the request `Host` against an allowlist defaulting to localhost only, so a proxied public host (`api.iknaio.com`, …) is rejected with 421 on every request — while the mounting code passed no allowlist. A new config option `GS_MCP_ALLOWED_HOSTS` (comma/space-separated; FastMCP matches with `fnmatch`, so `*` and `*.example.com` wildcards work) is now threaded into `http_app(allowed_hosts=…)`. It defaults to `*.iknaio.com`, `*.ikna.io` so the hosted deployments work out of the box; override it for other deployments. The wiring is version-safe: on FastMCP ≤ 3.4.2 (which doesn't validate the Host) the argument is skipped cleanly.
- **Bulk endpoints no longer bypass private-tag obfuscation.** The obfuscation plugin skips `/bulk` paths and bulk responses stream via `StreamingResponse`, which never passes through the plugin's response hook, so `POST /bulk.{csv,json}/list_tags_by_address` returned raw `label`/`source`/`tagpack_uri`/`actor` for private tags that the equivalent non-bulk endpoint redacts. The bulk handler now applies the same obfuscation hooks to each result before flattening — private-tag fields are blanked while the row set (and counts) stay identical.
- **`list_txs` on TRON no longer 500s when a trace is not found.** The TRX branch of `normalize_address_transactions` lacked the ETH branch's guard and used an unassigned `trace` on a trace-not-found row, raising `UnboundLocalError` for the whole request; it now mirrors the ETH handling (marks the row as an error, value 0).
- **Address/entity tx listings no longer 500 under the default config when a page contains multiple sub-transfers of one tx.** With `strict_data_validation` (default on) the assert compared the unique-tx count to the page row count, tripping whenever one tx contributed several rows (e.g. multiple token transfers). It now validates against the deduplicated tx-id set.
- **`list_neighbors` no longer misattributes counterparty addresses when an address row is missing.** It zipped a filtered address list against the unfiltered relation rows, so one absent row (possible while the delta-updater is writing) shifted every subsequent neighbor onto the wrong address string. Addresses are now keyed by id.
- **Tag digest `inherited_from` works, and its relevance scoring no longer re-sorts per label.** The `inherited` flag was AND-ed with an initial `False`, so it could never become true and `inherited_from` was always `null`; it now correctly reports `cluster` when every contributing tag is inherited. The word-counter sort in the relevance loop is hoisted out of the per-label loop.
- **Cluster label lookups no longer bleed across networks.** `get_labels_by_clusterid` resolves a cluster to its addresses via `address_cluster_mapping`, whose `gs_cluster_id` is only unique per network, but applied no network predicate — so a numeric cluster id matched cluster #N on every chain and returned the labels of unrelated same-numbered clusters (and could not use the `(network, gs_cluster_id)` index). The cluster→address resolution is now scoped to the requested network; tag matching itself stays network-agnostic by design. Label search additionally escapes `%`/`_` wildcards in user input and paginates with a deterministic order.
- **The file-store download token is no longer written to application logs.** `url_for` logged the full download URL (including the bearer token) at WARNING on every mint; the token is now redacted and the diagnostic lowered to DEBUG.

### Web API + Python client (webapi-2.15.0)

#### Added
- **New currency-less `POST /graph/summary` (beta).** The request body carries a list of mixed-network references (`GraphTxRef`/`GraphAddressRef`, each naming its own `network`), so a single call can summarize transactions and addresses spanning several chains. Fiat is not selected by a request parameter: every monetary value is emitted for both `eur` and `usd` via the shared `Values`/`Rate` pattern. The response carries a `txs` and/or `addresses` block (one per requested reference type), each holding an `overall` rollup aggregating across all its references plus a `networks` list with one entry per distinct network. Unknown references do not fail the request: they are dropped and reported per network in a machine-readable `nodes_not_found` note (`items` carries the refs); 404 only when fewer than 2 of a list's references exist. Duplicate references (including spelling variants of one node) collapse and are reported per network in a `duplicates_collapsed` note; sub-transaction identifiers (`<hash>_T1`) are rejected with a 400 since their legs are aggregated under the base hash anyway. Value totals are gross (UTXO txs contribute their full output sum, change included), documented as such in the schema. `total_fee` is always known on UTXO networks (`0` for an all-coinbase set); on account chains `null` means fee data was unavailable for at least one tx (partial sums are never emitted). Marked **beta** (`x-beta` in the OpenAPI spec): the contract may still change without a deprecation cycle.
- **New `POST /graph/compare` (beta).** Currency-less like `/graph/summary` (references carry their own `network`), and each compared item now echoes back its `network` field. Remains BTC-only for now; a non-btc reference is rejected with a 400 that names the offending network(s). The verdict exposes only the categorical `relation` tier — the internal numeric `confidence`/`score_total`, and likewise the per-signal `weight`, stay backend-only until calibrated against ground-truth data. Verdict notes are structured `{code, message}` objects with a closed code vocabulary (like the summary notes), and `linkage_hits` lists the fired linkage gates as the machine-readable positive counterpart of `discriminator_hits`. Sub-transaction identifiers (`<hash>_T1`) are rejected with a 400. Signal `per_tx` observations are natively typed (booleans for flag signals, an integer for `tx_version`, categorical strings for bucket signals, sorted string/integer lists for overlap and id sets — `null` = not derivable, empty list = computed but empty) instead of stringified encodings. The per-request work cap (20k combined inputs/outputs) is enforced on cheap header reads before any input/output data or heuristics are fetched. A 404 enumerates every missing tx hash, so a stale request is fixable in one round. Script types come from the ingest-time `address_type` classification stored on every raw v3 input/output (address-prefix inference remains only as a fallback for keyspaces without the column), so P2PK, multisig and OP_RETURN outputs now classify correctly and nonstandard outputs are part of the fingerprint. Marked **beta** (`x-beta` in the OpenAPI spec): the contract may still change without a deprecation cycle.
- **`graphsense file scan-for-addresses` brings the address scanner to the standalone `graphsense-python` client.** The scanner package is vendored from `src/graphsenselib/convert/address_scan/` via `clients/python/scripts/sync_address_scan.py` (pre-commit `sync-address-scan` + `make check-address-scan` guard drift), mirroring the existing `gs_files` vendoring. To keep the client dependency-free, the sync rewrites the two graphsenselib imports: address validation points at a new **stdlib-only** `graphsense/address_scan/validators.py` (base58check, bech32, pure-Python keccak for EIP-55, ripple base58check for XRP), and `.gs` decoding reuses the vendored `graphsense.gs_files.parser`. graphsenselib itself continues to use its lib-backed `utils/address.py`; the two validator implementations are pinned together by `tests/convert/address_scan/test_validators_crosscheck.py`.

#### Changed
- **`pagesize`, bulk `num_pages` and graph `depth` are now bounded to prevent one-request denial-of-service.** `pagesize` was uncapped, bulk `num_pages` was unbounded and consumed recursively (large values raised `RecursionError` mid-stream, delivering a truncated-but-200 body), and `depth=0` disabled the BFS depth stop entirely (graph-wide walk bounded only by the request timeout). These now carry validated bounds (`pagesize ≤ 5000`, `1 ≤ num_pages ≤ 100`, `1 ≤ depth ≤ 7`), and the BFS depth stop is `>=` so an out-of-range value can never disable it.

## [2.14.7] - 2026-06-30

### Library

#### Fixed
- **TRON factory-deployed contracts are now flagged with `is_contract=true`.** Contract detection for TRON only recognized contracts deployed by a top-level `CreateSmartContract` transaction (via `receipt_contract_address`). A contract deployed by a *factory* contract — where the top-level transaction is a `TriggerSmartContract` and the new contract appears only as an internal `create` trace — was never flagged (e.g. the PEPE TRC20 token `TMacq4TDUw5q8NFBwmbY4RLXvzvG5JTkvi`). `is_contract_trace` now detects TRON `create` traces (the adapter remaps the internal-tx `note` onto `call_type`), and the delta-update emits a minimal, value-free entity delta carrying only `is_contract=true` for the created address — value, tx-count and balance accounting stay on the call traces, unchanged. This brings the incremental Python delta-updater in line with the Spark transform (`computeContracts`), which already unioned trace- and tx-based creation detection. Takes effect for newly (re)processed blocks; to repair already-ingested keyspaces in place without a full re-transform, use the new `scripts/backfill_trx_is_contract.py` PySpark job (recomputes the contract-address set from the raw keyspace and flips only the `is_contract` column on the transformed `address` table via a partial CQL upsert; idempotent, dry-run by default with an explicit `--write` to apply, and reads connection/Spark details from `graphsense.yaml` like the `scripts/pubkey/*` jobs).

### Web API + Python client (webapi-2.13.5)

No changes.

## [2.14.6] - 2026-06-24

### Library

#### Added
- **`recover_base58_case` recovers the original letter-casing of a base58check string whose case was lost** (e.g. an address that was upper- or lower-cased). base58check carries a 4-byte SHA256d checksum and SHA256 is not invertible, so there is no algebraic shortcut: `graphsenselib.utils.address.recover_base58_case` enumerates the valid case variants of each letter and returns the first one whose checksum is intact. The search is pruned to genuinely case-ambiguous characters (digits and the asymmetric `i`/`o`/`l` are fixed) and stops at the first match (the 32-bit checksum makes a hit effectively unique), so a typical 34-char address (~2^24..2^28 variants) resolves in seconds to minutes. Companion helpers `iter_base58_case_recovery` (lazy generator of all matches) and `count_base58_case_candidates` (search-space size) are also exported; a `max_candidates` guard prevents accidental runaway searches. Applies to base58check only — bech32 (`bc1…`, `ltc1…`) is spec-defined as single-case so there is nothing to recover.

#### Fixed
- **Cross-chain pubkey lookups no longer surface Bitcoin Cash addresses in `bitcoincash:` CashAddr form.** BCH addresses were materialized into the `pubkey_by_address` dataset in the modern CashAddr encoding (e.g. `bitcoincash:qqtmf0v6...`), while the rest of the system keys BCH addresses by their legacy base58 form (`13AM4VW2...`, ingest normalizes CashAddr → legacy). The two encodings of the same address showed up as duplicate graph nodes in `GET /{network}/addresses/{address}/related?address_relation_type=pubkey`. The reader (`get_cross_chain_pubkey_related_addresses`) now normalizes every derived address to its canonical user form (CashAddr → legacy base58 for BCH; a no-op for already-canonical networks) and dedupes on `(currency, address)`, so the encodings collapse to a single node. No dataset rebuild is required — the fix is applied per request on read.
- **`GET /{network}/addresses/{address}/neighbors?only_ids=...` returned 500 on an invalid `only_ids` value for trx.** A garbage TRON id (e.g. `only_ids=dummy`) canonicalized via `validate=False` to empty bytes `b""`, which `bytes_to_hex` maps to `None`, crashing `scrub_prefix` with `AttributeError: 'NoneType' object has no attribute 'startswith'`. An empty/unknown id is now treated as a not-found neighbor and silently dropped (matching the existing behavior for valid-but-unknown ids and for utxo networks), so `get_address_id` short-circuits to `None` before issuing an empty-partition-key query to Cassandra.

### Web API + Python client (webapi-2.13.5)

#### Added
- **API docs now carry an MCP authentication note.** The "AI assistant access (MCP)" section of the OpenAPI description ends with an authentication note telling users the OAuth client ID (`iknaio-mcp`) to use when adding the MCP connector. The text is the new `docs_mcp_auth_note` config field (env `GSREST_DOCS_MCP_AUTH_NOTE`); override it for a different client ID or set it to an empty string to omit the note (e.g. deployments without OAuth).

## [2.14.5] - 2026-06-22

### Library

#### Fixed
- **Weekend FX-rate gaps spuriously aborted exchange-rate (and block) ingestion, stalling updates for fiat-converted currencies.** Two compounding bugs: (1) the per-day FX forward-fill only ran *within* the import window, so a Monday update whose window is just Sat+Sun (the ECB publishes no weekend rates) had no in-window anchor to fill from and aborted with "found missing values ... Probably a weekend". The FX rate is now forward-filled over a gap-free daily index from the full ECB history (`forward_filled_fx_rate`), so weekend / not-yet-published days correctly inherit the most recent known rate even when the anchor lies before the window. Fixed across all three providers (`coingecko`, `coinmarketcap`, `cryptocompare`). (2) The pre-ingest exchange-rates step (folded into `ingest from-node`) used to run as a separate command whose recoverable-gap exit (`SystemExit(15)`) was tolerated, so block ingestion still ran; after fusing it in, that exit aborted the entire command. The wrapper now treats the recoverable gap (exit 15) as a warning and continues with block ingestion, while still propagating critical multi-day gaps (exit 2) and any other error.

#### Added
- **MCP server advertises a connector icon and website URL in the `initialize` handshake.** New `GSMCPConfig` fields populate the MCP `serverInfo` `icons`/`websiteUrl` fields for spec-compliant hosts. `website_url` defaults to `https://www.iknaio.com/`; the icon defaults to the bundled favicon the REST app already serves at `/docs_assets/favicon.png` (same asset as the API docs) — a root-relative URL the host resolves against the server origin. Override the icon with `icon_url` (+ `icon_mime_type`, `icon_sizes`) — set an absolute, unauthenticated URL (not a data URI) for hosts that don't resolve relative refs — or suppress either by setting it to an empty string. Note that some hosts (e.g. Mistral) ignore `icons` entirely and derive the connector logo from the origin favicon instead.

### Web API + Python client (webapi-2.13.5)

No changes.


## [2.14.4] - 2026-06-16

### Library

#### Fixed
- **WAL serialization failed for the account model (TRX), aborting delta updates.** The account-model delta updater reads current DB values through the process-parallel reader, which hands UDT values back as `PlainRow` (an attribute-access view that rebinds to UDT columns on write). Those `PlainRow` objects enter `DbChange.data`, but `wal.py` had no msgpack ext handler for the type, so staging the redo log raised `TypeError: Cannot serialize ...PlainRow into the WAL payload`. The UTXO path was unaffected because its values are `DeltaValue` dataclasses. Added a generic `PlainRow` ext handler (encodes its dict recursively, decodes back to `PlainRow`) so the value tree rebinds byte-identically on replay regardless of UDT shape.
- **WAL serialization failed for arbitrary-precision integers (account model).** Cassandra `varint` columns hold values of unbounded magnitude — e.g. high-decimal token balances — and these exceed msgpack's native signed/unsigned 64-bit range. msgpack routes such ints to the `default` callback, where they hit the unknown-type guard and raised `TypeError: Cannot serialize ...int into the WAL payload`. Added a big-int ext handler that encodes the value as a decimal string (and decodes it back), preserving it exactly so it rebinds to `varint` identically on replay; recursion covers big ints nested inside `DeltaValue`/`PlainRow`.

### Web API + Python client (webapi-2.13.5)

No changes.

## [2.14.3] - 2026-06-16

### Library

#### Added
- **Crash-safe delta updates via an opt-in redo write-ahead log (WAL).** Delta updates compute aggregate columns (balances, tx/relation counts, summary statistics) read-modify-write and store the *absolute* result, so a crash partway through a batch's writes left the keyspace partially mutated and the next run — resuming from the last checkpoint — *recomputed* the block against that polluted state, double-counting every aggregate. With the WAL enabled, each batch's fully-resolved change set is staged durably **before** any of it is applied and cleared only **after** the whole set (data + bookkeeping) is acknowledged; on the next run the updater first **replays** any pending record verbatim (idempotent, since the stored writes are absolute values) and **never recomputes** a partially-applied batch. The WAL lives in a `delta_updater_wal` table in the transformed keyspace (chunked msgpack payload + a header row written last as the commit point), so the log is shared across independent container runs rather than lost with a container-local file; a `code_version` stamp refuses to replay records written by an incompatible binary. **On by default** as a mandatory safety net — opt out by setting `delta_updater_wal_enabled: false` in the config or passing `delta-update update --no-enable-wal` (flag overrides config); when disabled the WAL is fully inert (nothing staged, no recovery, table not created). (Single-writer mutual exclusion still relies on the existing `create_lock`; consolidating that onto a Cassandra LWT lock is a separate, deferred change.) The `code_version` fence can be overridden for a single run with `--force-wal-replay`: on a version-mismatched record it prints a loud warning and asks for an interactive yes/no confirmation before replaying the staged absolute values verbatim (the chunk-count and checksum verification still run, so torn/corrupt payloads are always rejected). Declining, or a non-interactive session, re-raises the mismatch and aborts.
- **`deltaupdate --parallel-workers` — process-parallel batch reads/writes for the account model.** The Cassandra delta-update batch reads and persists are client-CPU-bound on the driver, so they can be spread across worker processes. The new `--parallel-workers N` flag (account model only; default `1` keeps the existing single-process behavior) runs them on a `ParallelDbPool` of `N` processes, each building its own Cassandra `Cluster` after spawn (the driver is not fork-safe). Rows are flattened to a picklable form before crossing the process boundary and rebound byte-identically through the driver UDT serializer on the other side. The parent owns graceful shutdown: workers ignore `SIGINT`/`SIGTERM` so a Ctrl-C or group `SIGTERM` cannot tear a worker down mid-chunk — the parent finishes the in-flight batch, breaks at the next batch boundary, and drains the pool via its sentinel queue; each worker closes its session on exit via `atexit`.

#### Fixed
- **UTXO ingest now derives P2PK output/prevout addresses when the node omits them, network-aware (enables Zebra as a ZEC node).** P2PK locking scripts (`<pubkey> OP_CHECKSIG`) have no canonical address, so nodes report `type: "pubkey"` but may omit `addresses`/`reqSigs` — zcashd populates them, Zebra (and modern Bitcoin Core) do not. The parser previously reclassified those as `nonstandard` with a synthetic `sha256(script)` id that can never be mapped back to the real address, so a Zebra-ingested ZEC keyspace would silently diverge from a zcashd one. `_parse_output`/`_parse_input` now derive the address from the script hex using the chain's address version (BTC `0x00`, LTC `0x30`, ZEC t1 `0x1cb8`, BCH legacy `0x00`), keep the node's `pubkey` type, and fill `required_signatures=1`, so a P2PK output parses byte-identically whether or not the node returned the address. `parse_script` (the `enrich_txs` resolution path for genuinely non-standard scripts) is now network-aware too — it encodes p2pkh/p2sh/p2pk/multisig with the chain's version bytes and treats segwit-shaped scripts as non-standard on chains without segwit (e.g. ZEC); the btcpy shadow-validation runs only for BTC, the one encoding btcpy supports. The exporter and `enrich_txs` take a `network` argument (threaded from `SourceUTXO`, the transformer, the `ingest` entrypoint, and the watch flow provider); the address-present and BTC paths are byte-for-byte unchanged. Also blacklists Zebra's per-tx `blockhash`/`blocktime`/`confirmations`/`height`/`in_active_chain`/`time` fields.
- **`ingest from-node` graceful stop now breaks at the next file-chunk boundary instead of the next partition (TRX).** The shutdown flag was only polled per partition, so a stop request could wait up to a whole partition (≈100k blocks on TRX) before taking effect. Each file-chunk is already a durable append commit, so breaking mid-partition is safe and resumes from the last ingested block; the flag is now polled per file-chunk, cutting graceful-stop latency from up to 100k blocks to ≤1k.
- **Post-ingest staleness check was skipped when no new blocks were available.** In append mode without an explicit `--start-block`, `ingest from-node` exits with code 12 when the raw keyspace is already at the node's highest block — and that early exit also skipped the built-in staleness check, which is exactly the scenario it must alert on (a node that stopped syncing produces "no new blocks" forever while the raw keyspace goes stale). The no-new-blocks path now raises `NothingToIngestError` (a `SystemExit` subclass with code 12, so `ingest dump-rawdata` and wrapper scripts keying on exit 12 are unaffected) and `ingest from-node` runs the staleness check before exiting with the unchanged exit code 12.

### Web API + Python client (webapi-2.13.5)

No changes.

## [2.14.2] - 2026-06-11

### Library

#### Fixed
- **`exchange-rates cryptocompare` (fetch/ingest/dump) crashed with `KeyError: 'Data'`.** CoinDesk switched off anonymous access to `min-api.cryptocompare.com` around 2026-06-08 (archived responses still carry data on June 5, first `401 "API key required"` on June 9); the API now returns `{"Data": {}, "Err": ...}` and the unguarded `["Data"]["Data"]` access turned that into a bare KeyError. Affects every graphsense-lib version, independent of any code change. The provider now sends an API key from the new `cryptocompare_api_key` config field (`Authorization: Apikey` header; key is required, like the other providers) — this also covers the pre-ingest exchange-rates step of `ingest from-node` — and API error responses abort with the API's error message plus a hint where to get a key, instead of a KeyError. The regression ingest runners pick the key up from the `CRYPTOCOMPARE_API_KEY` environment variable.
- **`.gs` files could carry the same EVM address twice with different casing.** A pathfinder spec listing an ETH address both EIP-55-checksummed and lowercase produced two nodes for the same address (and a single checksummed entry collided with the canonical lowercase node in the pathfinder UI). `GsBuilder` now canonicalizes EVM-style hex identifiers — addresses (`0x` + 40 hex) and tx hashes (64 hex, optional `0x`, account-model `_T<n>`/`_I<n>` sub-payment suffixes preserved) — to lowercase, and merges duplicate addresses, txs, and agg edges on their normalized `(network, id)` (labels/colors are folded into the first occurrence, `starting_point` is OR-ed, edge `tx_ids` are unioned). The hierarchical layout and both pathfinder verifiers compare ids in the same canonical form; `verify_structural` additionally warns when a spec lists the same address or tx more than once, so agents building files via `build_pathfinder_file` get told to fix their spec.

### Web API + Python client (webapi-2.13.5)

No changes.

## [2.14.1] - 2026-06-10

### Library

#### Fixed
- **`parse_script` shadow-validation divergence on nonstandard scripts (LTC prod ingest crash).** The btcpy-free native output-script parser classified any script ending in `<pubkey-push> OP_CHECKSIG` as p2pk, silently skipping prefix opcodes (e.g. `OP_5 OP_5 OP_ADD OP_DROP <pk> OP_CHECKSIG` seen on LTC), while btcpy correctly treats such scripts as nonstandard — tripping the deliberately-loud shadow `AssertionError` and aborting `ingest from-node`. The native parser is now a faithful reimplementation of btcpy's token-based template matching, byte-identical on every probed and fuzzed input (0 divergences over 200k adversarial scripts plus exhaustive single-byte probes): exact single-push p2pk; token-template p2pkh/p2sh/segwit-v0 (non-minimal pushes accepted, as btcpy does); nulldata's `<1-83>` payload constraint incl. small-int payloads; hybrid (0x06/0x07) uncompressed pubkeys; and btcpy's full multisig quirks (numeric N from any push, unvalidated M slot, invalid-format keys dropped rather than rejected, truncated-trailing-push tolerance, and `IndexError` parity on empty-payload N/M pushes).

#### Added
- **Built-in pre-ingest exchange-rates update for `ingest from-node`.** When `ingest_config.exchange_rates_provider` is set in `graphsense.yaml` (`coingecko`, `coinmarketcap`, or `cryptocompare` — the first two need their API key in the config, cryptocompare needs none), the command ingests the latest exchange rates from that provider into the raw keyspace before ingesting blocks — equivalent to running `exchange-rates <provider> ingest --abort-on-gaps` first, which it replaces. Unfillable rate gaps abort the run with the same exit codes as the standalone command, so blocks are never ingested ahead of their rates. `--exchange-rates-provider` overrides the configured provider per run; `--no-exchange-rates` skips the step. Requires the cassandra sink (skipped with a warning otherwise); with no provider configured the behaviour is unchanged.
- **Built-in post-ingest staleness check for `ingest from-node`.** After a successful ingest run the command now performs the same check as `monitoring monitor-raw-ingest`: if the timestamp of the highest ingested raw block is older than a configured tolerance, a warning is logged and sent to a notification topic. This replaces the separate `monitoring monitor-raw-ingest` invocation after every ingest run. The tolerance is configured per network in `graphsense.yaml` (`ingest_config.raw_ingest_staleness_threshold`, in hours, e.g. `btc: 10`, `trx: 72`) and can be overridden per run with `--staleness-threshold`; `--no-staleness-check` skips the check, `--staleness-topic` selects the notification topic (default: `exceptions`). With no tolerance configured the behaviour is unchanged. The check requires the cassandra sink (it reads the raw keyspace) and is skipped with a warning otherwise. `monitoring monitor-raw-ingest` still works for standalone/cron use.

### Web API + Python client (webapi-2.13.5)

No changes.

## [2.14.0] - 2026-06-09

### Library (v2.14.0)

#### Added
- **`transformation raw-to-transformed` — drive the external graphsense-spark Scala job from graphsenselib.** ⚠️ **ALPHA** — the command interface and behaviour may change, and invoking it prints an alpha warning. New CLI command that replaces the standalone bash `spark-submit` driver: it creates a fresh transformed keyspace, downloads the graphsense-spark release jar from a public GitHub Release asset (cached locally), and launches the job via `spark-submit`. Supports self-contained or slim jars, an optional Cassandra Sidecar bulk-write path, and a dry run that prints the resolved command without side effects. The command is backend-neutral, so a future native-PySpark backend can be selected without changing how it is invoked.
- **`transformation pubkey-update` / `pubkey-compact` / `pubkey-detect` / `pubkey-load` — cross-chain pubkey → address lookup.** ⚠️ **ALPHA** — the command interface and behaviour may change, and invoking these prints an alpha warning. `pubkey-update` reads new transactions from a currency's source Delta Lake, extracts signing pubkeys into a shared cross-chain Delta store, and writes derived addresses for any pubkey newly observed on 2+ chains to either Cassandra or a Delta table. `pubkey-compact` deduplicates that store between runs; for multi-chain backfills `pubkey-detect` runs the cross-chain detection once over the fully-appended store; and `pubkey-load` loads a delta-only run's result into Cassandra, so the heavy extraction can run without production-Cassandra stress and be reviewed before the throttled write. Extraction covers the common UTXO input/output script types and the ETH/TRX account side (recovering the signing key, with a from-address self-check on ETH), and address derivation adds Bitcoin Cash CashAddr. BCH defaults its start block to the fork height so shared pre-fork BTC history isn't re-extracted into trivial cross-chain collisions. secp256k1 public-key validation now uses libsecp256k1 by default, far faster than the pure-Python path. The cross-chain materialisation persists only pubkeys it successfully derived at least one address for, so off-curve / special keys are retried on the next pass rather than silently consumed.
- **Environment-variable substitution in config files.** String values in config files may now reference environment variables (with optional defaults and an escape for literals), resolved at load time across all config-loading paths (CLI, REST, and the typed loader). Useful for keeping secrets like DB URLs and credentials out of committed config files. Backwards compatible: configs without placeholders are unchanged.
- **Overridable Spark Maven packages.** The Spark transformation session's Maven packages (previously hardcoded) can now be overridden per-package via config, so only the ones you want to change need to be specified while the defaults stand. The S3 connector is still added only when S3 credentials are present, and the existing full-replace escape hatch still wins.
- **Baseline inheritance for `s3_configs` and `spark_config`.** Both config sections can now define a shared `baseline` entry that every other named entry inherits from (its own keys win), removing per-entry duplication — e.g. a common S3 endpoint/region with only the credentials differing, or a shared Spark baseline with named profiles overriding it. `spark_config` still accepts its legacy flat form unchanged. Fully backwards compatible with configs that omit `baseline`. Every `transformation` command that starts a Spark session also takes a `--spark-profile` flag to select a profile per run (defaulting to the baseline).
- **Reader can merge cross-chain pubkey mappings from multiple keyspaces.** The REST reader's cross-chain pubkey keyspace setting now accepts a list as well as a single keyspace. When several are configured it looks the queried address up in each, derives addresses from every key found, and merges the results — so a validated new keyspace can be served alongside the legacy one, which still holds keys the new pipeline cannot reproduce exactly (e.g. doge-sourced cross-chain keys). Keyspaces lacking the lookup table are skipped, and the feature enables when at least one has it. On startup the service logs the resolved set once (`cross-chain pubkey lookup active on keyspaces: [...]`, or a `disabled` line), so it is visible on a running instance which keyspaces are actually used. Fully backwards compatible: a single keyspace behaves exactly as before.
- **Trivial cross-chain address detection for BTC↔BCH and TRX↔ETH, independent of the pubkey table.** Cross-chain address lookups now also surface the script-equivalent address on the paired chain even when the pubkey table has no entry for the queried address: BTC↔BCH via legacy/cashaddr normalisation (segwit addresses are correctly excluded, as they are not script-equivalent across the fork), and TRX↔ETH via address-format conversion in both directions. Results are deduped against pubkey-backed entries, and the API wire format is unchanged.

#### Changed
- **`transformation` commands renamed for clarity.** The two job commands now name their source → destination directly: `run` → **`delta-to-raw`** (loads the Cassandra raw keyspace from Delta Lake) and `run-full-transform` → **`raw-to-transformed`** (raw → transformed via the graphsense-spark job). Both are renamed outright with no aliases — update any scripts that invoked `transformation run`.
- **Docker image: multi-stage build, runtime shrunk from ~2.3 GB → ~1.7 GB.** The `Dockerfile` was split into a `builder` stage and a fresh `python:3.13-slim-bookworm` runtime stage. All build-time tooling — `gcc`/`g++`/`make`/`cmake`, the Rust toolchain, `curl`, and `libpq-dev` headers — now lives only in the builder and is verified absent from the shipped image; the runtime stage `COPY`s just the two pre-built wheels (`graphsense_lib` + `graphsense_clustering`) out of the builder. This replaces the previous single-stage build that installed the toolchain and then tried to `apt-get purge` / `rustup self uninstall` it back out within one layer. Runtime OS deps are now scoped to exactly what runs in-container: `openjdk-17-jre-headless` (PySpark — Java 17, since Java 21 dropped the `DirectByteBuffer(long,int)` Arrow 12 needs), `libpq5` (psycopg runtime lib, not the `-dev` headers), and `git` / `git-lfs` / `openssh-client` (GitPython + tagpack repo operations). `numpy`'s bundled OpenBLAS `.so` files are deliberately left un-`strip`ped (stripping corrupts their page-aligned LOAD segments and breaks `import numpy`); `__pycache__` dirs are dropped instead. Verified in the built image: `git` 2.39.5, `git-lfs` 3.3.0 (`git lfs install` applied), OpenJDK 17, `gs_clustering` imports, duckdb `httpfs` pre-installed, and a real local `SparkSession` (PySpark 3.5.8) starts and runs.

#### Fixed
- **Date scalars inside a tagpack/actorpack `context` block no longer break parsing.** When a `context` field is written as a YAML mapping, a bare date value (e.g. `valid_from: 2022-09-23`) is parsed by PyYAML into a `datetime.date`. `Tag`/`Actor` construction serialized that mapping to a JSON string via `json.dumps` without a `default` handler, raising `Object of type date is not JSON serializable`. Both call sites (`tagpack.py`, `actorpack.py`) now pass `default=str`, matching the existing `to_json()` helpers, so dates become ISO strings.
- **Taproot / P2TR (`bc1p…`) addresses no longer flagged "possible invalid" in tagpack validation.** The native segwit validator (`utils/address.py:bech32_validate`, used by BTC/LTC) only accepted the bech32 checksum constant (witness v0), so witness v1+ addresses — which BIP-350 encodes with bech32m (constant `0x2BC830A3`) — failed the checksum and were reported as possible invalid by `TagPack.verify_addresses`. Validation is now BIP-173/BIP-350-aware: it decodes the witness version, requires bech32 for v0 (with a 20- or 32-byte program) and bech32m for v1–v16, and validates the witness-program length via a `bech32_convertbits` helper. Mixed-case and over-length (`>90`) inputs are also rejected per spec. Verified against the BIP-350 test vectors (Taproot and v16 now pass; v0-with-bech32m and corrupted checksums correctly fail).
- **py4j DEBUG chatter silenced in verbose mode.** `configure_logging` now pins the `py4j` logger to `INFO`, so running a PySpark job at `-vvv` (DEBUG) no longer floods output with a pair of `Answer received: …` / `Command to send: …` lines for every JVM↔Python call, while graphsenselib's own DEBUG logs are kept. py4j warnings/errors still surface.
- **`get_spent_in_txs` / `get_spending_txs` now raise `BadUserInputException` instead of an unhandled `ValueError` for non-hex transaction hashes.** When passed a non-hex string (e.g. a BTC address like `bc1q…` or `35usp…`) as the transaction hash, both methods in `db/asynchronous/cassandra.py` called `bytearray.fromhex(tx_hash)` directly, raising an uncaught `ValueError` (which surfaced as an HTTP 500 in the REST layer). They now convert the hash once inside a guard that raises `BadUserInputException` (`"<hash> does not look like a valid transaction hash."`), matching the existing behaviour of `get_tx_by_hash`.

### Web API + Python client (webapi-2.13.5)

No changes.

## [2.13.5] - 2026-05-27

### Library (v2.13.5)

#### Added
- **Async Cassandra reader can downgrade LOCAL_QUORUM to LOCAL_ONE on transient unavailability.** New opt-in `CassandraConfig.consistency_level_fallback` (default `false`). When set, `GraphsenseFallbackToLocalOneRetryPolicy` downgrades a LOCAL_QUORUM read to LOCAL_ONE on the FIRST `Unavailable` / `ReadTimeout` if at least one replica is alive — letting the web tier survive a rolling restart on RF=2. Strictly scoped: one downgrade per query, only LOCAL_QUORUM is touched (QUORUM/ALL/EACH_QUORUM left alone), writes never downgraded. Trade-off: read-after-write consistency is dropped for those reads.
- **Optional Redis-backed file store and `/download/{token}` route.** New `web/file_store.py` (`RedisFileStore`, reusable `FileStore` protocol) holds files as TTL'd Redis hashes keyed by a 256-bit CSPRNG token; the route is a plain Starlette route, excluded from OpenAPI. New `FileStoreConfig` on `GSRestConfig`: `enabled` (default `false`), `redis_url`, `download_path` (`/download`), `ttl_s` (1800), `max_file_bytes` (5 MiB), `base_url`, `key_prefix`, `embed_resource`. URLs derive from `X-Forwarded-*`/`Host` with `base_url` override. Multi-worker safe; disabled by default.
- **Pathfinder verifier package (`graphsenselib.pathfinder`) and `verify=true` default on `build_pathfinder_file`.** A new top-level package — independent of MCP, importable by CLI / scripts / the python client — exposes two verifiers callers can mix and match:
    - `verify_structural(spec)` — sync, pure stdlib, no backend. Catches in-spec inconsistencies that produce visually broken `.gs` files: agg_edges without txs / tx_ids, references to ids not in their list, orphan txs, and **stray addresses** (listed but not used as any edge endpoint and not declared `starting_point=true`).
    - `verify_against_backend(spec, *, default_network, backend)` — async, takes a minimal `GraphsenseBackend` Protocol (`address_exists`, `tx_addresses`). Cross-checks against on-chain reality: address exists, tx exists, AND both endpoints of every edge are in the tx's participant set (the original `9e44196...` mediation failure mode). Concurrency-capped via semaphore (default 8) to avoid the 2026-05-04 pool-exhaustion shape.

  The shipped `RestBackend` adapter wires it to any graphsense REST deployment via httpx (in-process or out-of-process); python-client callers can write a ~15-line `asyncio.to_thread` adapter against the same Protocol. `build_pathfinder_file` runs both verifiers by default, merges findings into `summary.warnings` AND the TextContent block, and downgrades backend transport errors to a single "verifier unavailable" warning so a flaky backend never sinks a structurally valid build. Pass `verify=false` to skip backend checks while drafting (structural checks still run).

#### Changed
- **`build_pathfinder_file`: tidy-tree hierarchical layout, UI-matched spacing, and a `download_url` channel.** The hierarchical layout (used whenever a node is flagged `starting_point=true`) now builds a BFS spanning tree and walks it post-order, so every descendant stays on the same side as its ancestor and branches no longer cross; txs snap to the mean y of their endpoints, then a per-column single-linkage clustering pass (threshold `y_step / 2`) spreads multi-tx piles along y so an N-tx edge no longer collapses onto a single visible point (observed: 73 of 83 txs invisible before the fix). Layout constants (`_HIER_Y_STEP`, `GsBuilder._ROW`) moved from `3.0` to `2.5` to match the Pathfinder UI's own defaults (`Config/Pathfinder.elm`). When a file store is configured the tool stashes the `.gs` file and returns an additive `download_url` in `structured_content` (the channel weak MCP hosts can still use); the embedded resource is still sent unless `file_store.embed_resource=false`. The tool is now async. Backwards compatible — without `file_store` config the only change is `download_url: null`.
- **`build_pathfinder_file` MCP-side input pattern accepts underscore.** `_ID_PATTERN` is now `^[a-zA-Z0-9_]{1,150}$` so the documented account-model sub-payment identifier form `<hash>_I<n>` (internal trace) / `<hash>_T<n>` (token transfer) passes validation. Affects `build_pathfinder_file`, `lookup_tx_details`, `list_txs_for`, `list_tx_flows` and the other consolidated MCP tools — previously they rejected `_`-bearing ids with `Invalid tx hash`. The rejection message is also reworded to make clear it's a format check at the input boundary, not a verify finding ("…has an invalid format … This is a format check at the input boundary, NOT a verify finding"), so agents stop reaching for `verify=false` to work around a format complaint.
- **`build_pathfinder_file` docstring guidance.** `_TxSpec.id` no longer nudges callers toward the suffixed identifier unconditionally on account-model chains: bare `tx_hash` is the natural choice for the native transfer; `<hash>_I<n>` / `<hash>_T<n>` is for pointing at a specific sub-payment. `label` field descriptions tell the model not to restate attribution tags or transaction date/value (the UI already shows them) and to reserve `label` for case context. Description-only encoding-unchanged.

#### Fixed
- **MCP tool selection for "what currencies are supported?".** Weaker hosts (observed on Mistral Le Chat "Work" mode) picked `list_supported_tokens` instead of `get_statistics` and per-network-iterated empty token lists into "supports an extremely wide range of currencies". `tools.yaml` and `instructions.md` now disambiguate explicitly — `get_statistics` is THE source for supported networks; `list_supported_tokens` carries a negative cross-reference and notes that an empty list does NOT mean the network is unsupported. Description-only change.
- **Tagpack / actorpack `!include` now resolves against the repo root when called without an explicit `header_dir`.** Previously `gs tagpack validate <file>` only worked when run from the tagpack repo root. A new helper `find_pack_root` walks up ≤ 3 ancestors looking for `packs/` or a `.git` directory; the first match becomes the include `base_dir`. Explicit `header_dir` still wins; with no root found, the loader falls back to pyyaml-include's CWD-relative default.
- **Slack exception notifications now cover the MCP path.** Three gaps closed: (1) the Slack handler is now attached to the `graphsenselib.mcp` logger tree (siblings of `graphsenselib.web.app`, so handler propagation wasn't reaching them); (2) a new `ErrorLoggingMiddleware` (`mcp/error_logging.py`) registered on the FastMCP instance calls `logger.exception(...)` on any unhandled tool/resource/prompt exception before re-raising; (3) backend HTTP 5xx responses (previously wrapped as `ToolError` by `_get_json` / `_get_json_optional` and silenced) now log at `ERROR` via a new `_raise_backend_http_error` helper before raising. Caller-side errors stay silent: `ToolError`, `ResourceError`, `PromptError`, `fastmcp.exceptions.ValidationError` and `pydantic.ValidationError` are all in `_EXPECTED_MCP_ERRORS` — they're contract, not incidents, and 4xx upstream responses are model-fixable inputs (`tx not found`, `bad address`), not ops issues.
- **`build_pathfinder_file` robustness on weak MCP hosts (Mistral Le Chat).** Four behaviours that collectively kept Mistral-style hosts from rendering the tool's result correctly:
    - The docstring no longer claims the model receives the `.gs` bytes (the bytes travel in a resource channel the model cannot read; the old wording made hosts that drop the embedded resource fabricate base64 / `data:` URLs).
    - `PathfinderSpec` declares an optional `layout` field so the common LLM mistake of nesting `layout` inside `spec` no longer fails with a Pydantic `extra_forbidden` validation error — when the top-level argument is the default (`"auto"`) and `spec.layout` is set, the latter wins; explicit top-level `layout` still takes precedence.
    - The tool always appends a `TextContent` block carrying the download URL (or an "embedded in this response" line) so hosts that only render `content` and ignore `structured_content` still surface a usable response.
    - Verifier findings are folded into that same `TextContent` block under a `Warnings — fix the spec and rebuild:` section, so the original 9e44196... failure mode (warning fired in `structured_content`, host dropped it, agent shipped a broken `.gs`) cannot recur on content-only renderers.
- **Verifier `RestBackend.tx_addresses` requests `include_io=true` & `include_nonstandard_io=true`.** UTXO `inputs` / `outputs` are declared `Optional[...] = None` on the REST response model and excluded from the body when not requested (`response_model_exclude_none=True`). Without the params, the adapter returned an empty address set for every UTXO tx and the mediation check fired the misleading warning `tx involves {}` on every tx-mediated edge — even for byte-identical files that had previously passed verification. Account-model bodies carry `from_address` / `to_address` unconditionally, so the flags are no-ops there. Locked in by both a wire-level regression test on the query string and an end-to-end test that drives the adapter through a real FastAPI route with the same `response_model_exclude_none` contract.

### Web API + Python client (webapi-2.13.5)

#### Changed
- **`graphsenselib.convert.gs_files` hierarchical layout output has changed shape.** Downstream consumers that call `apply_hierarchical_layout` directly or build `.gs` files via `GsBuilder` and depend on exact coordinates / byte-identical output will see drift: the layout now uses a BFS-tidy-tree (subtree-clustered), row pitch matches the Pathfinder UI defaults (`_HIER_Y_STEP` 3.0 → 2.5, `GsBuilder._ROW` 3.0 → 2.5), and multi-tx edges are de-overlapped along y instead of stacking on a single point. The decoded structure (addresses, txs, edges, ids, labels) is unchanged — only the `(x, y)` coordinates differ. See the Library section for design details.

## [2.13.4] - 2026-05-20

### Library (v2.13.4)

#### Added
- **New MCP tool `build_pathfinder_file` produces a `.gs` save file from an investigation agent's findings.** The agent passes addresses, transactions, and aggregated edges accumulated via `lookup_address` / `list_neighbors` / `list_txs_for`; the tool returns the `.gs` bytes as an MCP embedded resource (`BlobResourceContents`, MIME `application/octet-stream`, URI `file:///<filename>.gs`) so clients can hand it to the user as a downloadable attachment without feeding the blob through the model — `structured_content` carries only `{filename, summary}` (layout, counts, warnings). Layout is automatic: a new BFS-hierarchical layout (`apply_hierarchical_layout` in `src/graphsenselib/convert/gs_files/encoder.py`) runs whenever at least one node is flagged `starting_point=true` — anchors at column 0, every other node placed by hop distance with txs as stepping stones, within-level order following the spec (so writing the most relevant nodes first puts them near the top of their column); otherwise the columnar `GsBuilder` defaults apply. The docstring spells out the join semantics (a tx renders only when listed in `txs` AND referenced from `agg_edges.tx_ids`) with a worked example, and `summary.warnings` (`_collect_warnings`) flags four common authoring mistakes that render an empty/abstract graph: edges with no `txs`, edges missing `tx_ids`, `tx_ids` referencing hashes not in `txs`, and `a`/`b` endpoints not in `addresses` (advisory only, unknown-id lists truncated to ten). Input boundary uses the same conservative currency/id guards as elsewhere in `src/graphsenselib/mcp/tools/consolidated.py`. Registered in `src/graphsenselib/mcp/curation/tools.yaml` under `consolidated_tools` with `replaces: []` — net new surface, no existing tool or endpoint changed.

#### Changed
- **MCP hides the deprecated `entity` and `status` fields on every address, cluster, and raw-tag response.** Both fields are already flagged `deprecated: true` in the OpenAPI schema (`Address.entity`, `Address.status`, `Entity.entity`, `NeighborEntity.entity`, `AddressTag.entity`) and the REST surface dual-emits `entity` alongside the preferred `cluster` alias for backwards compatibility; surfacing both via MCP made the LLM double-read or pick the wrong one. `entity` and `status` are now added to `_LEGACY_ADDRESS_FIELDS`, `entity` to `_LEGACY_CLUSTER_FIELDS`, and a new `_LEGACY_TAG_FIELDS = {"entity"}` is applied to each row in `list_tags_by_address` (`src/graphsenselib/mcp/tools/consolidated.py`). REST endpoints and OpenAPI schemas are unchanged — the deprecation markers continue to advertise the field as legacy for non-MCP consumers.

#### Fixed
- **Type-checker warnings: `namedtuple("Row", ...)` calls in `tests/schema/test_apply_migrations.py` now use names that match their variables (`TransformedRow`, `RawRow`).** Pure rename — no behavioural change; clears the two `mismatched-type-name` diagnostics from `uv run ty check`.

### Web API + Python client (webapi-2.13.2)

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
