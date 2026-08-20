# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [v26.08.0] 2026-08-20
### Fixed
- Retrying a TRX transform no longer skips a table that was only written
  half way. The "should I write this" guards asked whether the target table
  was empty, which cannot tell a finished write from one whose run died mid
  write — the retry then left the keyspace silently incomplete, and the
  operator had to remember to truncate partial tables by hand. Each table
  now gets a completion marker in the gs-cache directory
  (`<gs-cache-dir>/<keyspace>/_written/<table>`), written only after the
  write returned, and the guards consult those markers. Tables that already
  held data before markers existed are adopted as complete once, so existing
  keyspaces keep working; deleting the cache directory resets the state.
- A completed transform no longer reports failure because of a Spark shutdown
  race. `SparkContext.stop()` tears down the driver's RPC connections while
  in-flight callbacks may still be pending, which surfaces as
  `TransportResponseHandler: Still have N requests outstanding` plus a
  `RejectedExecutionException` from the already-shutting-down dispatcher pool
  (seen on Spark 3.5 at the end of a 17h TRX run, after every table was
  written). Shutdown is now best-effort and the job exits explicitly with 0
  once the transform is done, so the exit code reports the transform's
  outcome rather than teardown noise.

## [v26.07.5] 2026-08-20
### Fixed
- The TRX transform now pins its processed block range (`min/maxBlockToProcess`)
  to the gs-cache (`blockRangeToProcess` dataset). The cutoff is derived from
  the exchange rates, which advance with every daily rate ingest, so a retry
  that reused cached datasets (`computeCached`) silently recomputed everything
  live — filtered counts, max block timestamp, `summary_statistics` — against
  a NEWER horizon than the cached table content. Observed on the 2026-07/08
  TRX full transform: after eleven runs spanning a month, `summary_statistics`
  claimed the transform reached 2026-08-18 while the cached content ended at
  the first run's horizon (~2026-07-19) — a delta updater onboarded from that
  row would have silently skipped the difference. The first run resolves and
  stores the range; every rerun loads it (a diverging `--min-block`/
  `--max-block` is reported); deleting the cache directory re-resolves it.

## [v26.07.4] 2026-08-18
### Fixed
- Sidecar bulk writes now detect Cassandra partitions too large for the bulk
  writer and route those rows through the plain Spark Cassandra connector
  (CQL write path) instead, keeping the bulk path for everything else.
  cassandra-analytics 0.3.0's sorted-mode `CQLSSTableWriter` accumulates each
  whole Cassandra partition as an in-heap `PartitionUpdate` BTree before
  appending it to the sstable, and a partition key hashes to a single token,
  so no split count can divide it: a partition beyond a few hundred million
  rows OOMs its executor no matter how the write is tuned. Observed on the
  2026-07 TRX full transform, where two burst-shaped ~455M-row
  `address_transactions` partitions (one address-id group hammered inside one
  block bucket) killed 160g executors across three runs, surviving both a 9x
  smaller `--block-bucket-size-address-txs` and 4x more upload splits. The
  row-count threshold is `spark.graphsense.sidecar.maxRowsPerPartition`
  (default 50000000; <= 0 disables the check); detection costs one extra
  aggregation job over only the partition-key columns per table write.

### Changed
- The eth job persists `addressRelations` before writing it. It was
  recomputed from its persisted inputs three times — once for the row count
  and once for each of the two sorted relation writes (the new
  oversized-partition detection would have added a pass per write on top).

## [v26.07.3] 2026-07-28
### Changed
- Sidecar bulk writes now size their upload-task count per table via
  `spark.graphsense.sidecar.splits.<table>` (splits per ring token range,
  fallback `spark.graphsense.sidecar.splits.default`, default 1) instead of
  the hard-coded `number_splits=-1`, which derived one global split count
  from `spark.default.parallelism` for every table. Large tables need many
  splits so the biggest (unequal) token ranges still fit in executor memory,
  but cassandra-analytics 0.3.0 builds one never-closed sidecar client per
  upload task on the executors, so a global split count high enough for
  `address_transactions` made small-table writes exhaust the executors' file
  descriptors (`EPoll.create: Too many open files`, 2026-07-28 TRX run).

## [v26.07.2] 2026-07-23
### Fixed
- Null-safe the pegged-token fiat conversion (`toFiatCurrency` in the account
  transformation). `computeExchangeRates` zero-fills dates that are missing
  from the raw `exchange_rates` table, so the USD/EUR-peg cross rate divides
  `0/0`, which is NULL in Spark. The null survives per-transfer into the
  per-(address, token) aggregates: an address whose transfers of a token all
  fall on such a day gets a `fiat_values` list with a null element inside the
  `total_tokens_received/spent` map, and the Cassandra (bulk) writer rejects
  it with `NullPointerException: Collection elements cannot be null` — this
  killed the 2026-07-22 TRX full transform at the `address` write (one missing
  rate day, 2022-05-24, was enough). The cross rate is now coalesced to 0: on
  a day with unknown base rates a pegged transfer keeps its exact peg-currency
  value and reports 0 for the derived fiat currency, matching the existing
  "zero fiat values when no rate is known" behavior of unpegged tokens.

## [v26.07.1] 2026-07-21
### Fixed
- Removed the hard-coded `broadcast(exchangeRates)` join hint from the TRX
  encoded-transactions step. `exchange_rates` has one row per block, so the
  broadcast table grows with chain length; TRX (~75M blocks at 3s block time)
  now builds an 8.6 GiB broadcast relation, exceeding Spark's
  non-configurable 8 GiB broadcast limit and killing the transform 4+ hours
  in with `Cannot broadcast the table that is larger than 8.0 GiB`. No
  configuration can prevent it: the explicit hint bypasses
  `spark.sql.autoBroadcastJoinThreshold`, and `spark.sql.optimizer.disableHints`
  only strips unresolved `.hint()`-style hints — `functions.broadcast()`
  injects an already-resolved hint node that survives it (verified on Spark
  3.5.8). Without the hint the planner picks a
  sort-merge join, and AQE (enabled in the production properties) still
  converts back to a broadcast join at runtime whenever the actual rate set
  is small enough. The identical hint in the ETH transformation is kept:
  ETH's ~12s block time puts it around ~23M blocks / ~2.5 GiB, decades from
  the cap. Output is unchanged — this only affects the join strategy.

## [v26.07.0] 2026-07-07
### Added
- **Unpegged token support for the account model (ETH, TRX), mirroring
  graphsense-lib's extended token support.** `token_configuration` entries may
  now have no `peg_currency` (`pegCurrency = None` in the `TokenSet`). Such
  tokens are priced from per-token exchange rates instead of the native-coin
  rate: the transform reads the raw `token_exchange_rates(asset, date)` table
  (populated by the graphsense-lib rates ingest), maps the daily rates to
  per-block rows for every (asset, block) pair that saw a transfer of an
  unpegged token, writes them to the transformed
  `token_exchange_rates(asset, block_id)` table, and uses them to compute the
  fiat values of encoded token transfers (and therefore of all downstream
  aggregates: address/relation token values). Unpegged transfers with no
  fetched rate get zero fiat values, matching the graphsense-lib delta-updater.
  Keyspaces without the new tables (schema migration not applied yet) keep
  working: the read falls back to an empty rate set and the write is skipped,
  each with a warning.

## [v26.06.0] 2026-05-29
### Added
- Optional Cassandra Sidecar bulk-write path, selected with the `--writer`
  argument (`cassandra`, default, or `sidecar`). With `--writer sidecar`,
  transformed tables are written by generating SSTables on the Spark executors
  and streaming them into Cassandra through the Cassandra Sidecar (via the
  cassandra-analytics data source), bypassing the CQL coordinator/commitlog
  write path. The default keeps the Spark Cassandra connector write path
  unchanged.
- Release workflow now builds and attaches both the slim (`sbt package`) and
  fat (`sbt assembly`) jars as GitHub Release assets, so consumers can download
  a versioned jar from a public, token-free URL instead of GitHub Packages.
### Changed
- `build.sbt`: application runtime dependencies (scallop, spark-cassandra-connector,
  joda-time, web3j, graphframes) moved from `Provided` to compile scope so the
  assembly jar bundles them (graphframes in particular is not on Maven Central).
  Spark (`spark-sql`, `spark-graphx`) and the optional Cassandra Sidecar
  `cassandra-analytics-core` remain `Provided`. `sbt package` / `sbt publish`
  output and the existing spark-submit flow are unchanged.
### Fixed
- `docker/submit.sh` pinned the Spark Cassandra connector (`3.4.1`) and
  graphframes (`spark3.4`) packages to Spark 3.4 artifacts while the build
  targets Spark 3.5.

## [25.07.2] 2026-05-12
### Fixed
- Docker build failed on current `eclipse-temurin:11-jdk` (Ubuntu 25.10) because `apt-key` has been removed. Switched the SBT repository key handling to `gpg --dearmor` + `signed-by=` in `/etc/apt/keyrings/sbt.gpg`.
- `make build-docker` failed under Podman, which does not accept `--provenance=false`. The flag is now only passed when the local `docker` binary advertises support for it.
### Changed
- Bump Spark 3.5.3 → 3.5.8 (latest patch in the 3.5.x line) and pull the tarball from `dlcdn.apache.org` instead of `archive.apache.org` to avoid the rate-limited archive host.
- Bump Hadoop 2.7.7 → 2.10.2 (last patch in the 2.x line, wire/API-compatible) and switch its download from `archive.apache.org` to `dlcdn.apache.org` for the same reason.

## [24.07.0] 2025-06-26
### added
- new supported tokens for eth

## [24.11.1] 2024-12-06
### Fixed
- block difficulty and total difficulty can now be null

## [24.11.0] 2024-11-14
### Changed
- Upgrade to Spark 3.5.3
- Upgrade DataStax Spark Cassandra connector to 3.5.1
### Added
- max-block cli parameter for utxo currencies and eth to test with smaller datasets.

## [24.02.0] 2024-03-04
### Fixed
- excessive logging in container
- tron.trace callvalue overflow int -> bigint
### Changed
- Upgrade to Spark 3.4.2
- Upgrade DataStax Spark Cassandra connector to 3.4.1
- Simplified (record local) calculation of secondary ids
- Simplified (record local) calculation of tx ids (account model currencies)

## [24.01.0] 2024-01-08
### Added
- implemented transform for tron currency
- checkpoints and loading on HDFS
### Changed
- Upgrade to Spark 3.2.4
- Change package name graphsense-ethereum-transformation -> graphsense-spark
- integrated UTXO (BTC, ZEC, LTC, BCH transform)
- revised namespace structure (BREAKING: call is different path, new --network parameter needed!)

## [23.09/1.5.1] 2023-10-25
### Fixed
- duplicated txs ids in `block_transactions`

## [23.06/1.5.0] 2023-06-10
### Changed
- Include Ethereum internal transactions in `address`, `address_relations` tables. [#8](https://github.com/graphsense/graphsense-ethereum-transformation/issues/8)

## [23.01/1.4.0] 2023-03-29
### Changed
- Upgrade to Spark 3.2.3
- Changed handling of missing exchange rates values; don't fill with zeros,
  remove blocks, txs etc instead.

## [23.01/1.3.0] 2023-01-30
### Added
- Token Support for Ethereum stable coin tokens (WETH, USDT, USDC)
- Added Parsing of ETH-logs to support tokens
- Compute contracts from traces table
- Changed schema of `address_transactions`, `address`, `address_relations` to support tokens and their aggregated values.
- Balance table now contains on balance per currency (ETH and tokens)
- New table token configurations containing the supported tokens and their details
- Added scalafmt and scalastyle sbt plugins

## [22.11] 2022-11-24
### Added
- Added columns to `summary_statistics` table

## [22.10] 2022-10-10
### Changed
- Upgraded to Spark 3.2.1
- Updated Spark Cassandra connector to version 3.2.0

## [1.0.0] 2022-07-11
### Changed
- Updated raw Cassandra schema

## [0.5.2] 2022-03-10
### Changed
- Improved balance calculation
### Removed
- Removed tag handling (see graphsense/graphsense-tagpack-tool)

## [0.5.1] 2021-11-29
### Changed
- Upgrade to Spark 3
- Improved Cassandra schema
- Changed package name

### Added
- Added command-line arguments to spark submit script
- Added `trace` table
- Added balance calculation

## [0.5.0] 2021-05-31
### Changed
- Initial release
