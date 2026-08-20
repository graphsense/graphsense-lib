# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

The GraphSense Spark Transformation Pipeline: a Scala / Apache Spark batch job that
reads raw blockchain block & transaction data from Apache Cassandra (ingested
separately by `graphsense-lib`) and computes a de-normalized "transformed" keyspace
(address graph, address/cluster relations, balances, statistics) written back into
Cassandra. The transformed keyspace is then served by `graphsense-rest`.

## Build & test commands

Build tooling is SBT (Scala 2.12.17). `make` targets wrap SBT:

- `make test` — run all tests (`sbt test`)
- `make test-account` / `make test-utxo` / `make test-common` — run a model's test subtree
- Single test class: `sbt "testOnly org.graphsense.account.eth.TransformationTest"`
- `make format` — `sbt scalafmt`
- `make lint` — `sbt compile && sbt scalafix`
- `make build` — `sbt package` (thin jar in `target/scala-2.12/`)
- `make build-fat` — `sbt assembly` (fat jar)
- `make build-docker` — build the `graphsense-spark` Docker image
- `make run-docker-{eth,trx,btc,ltc,zec,bch}-transform-local` — build image and run a
  full transform against a Cassandra on the host network

scalafmt and scalafix run automatically on every compile (`scalafmtOnCompile`,
`scalafixOnCompile` in `build.sbt`), so a plain `sbt compile` reformats and lints.
Run `make format && make test` before committing.

Java 11 is the primary/CI target. Java 17 works; the required `--add-opens` /
`--add-exports` JVM flags are already set in `build.sbt`.

The release version is the `RELEASE` line in the `Makefile` — `build.sbt` parses it
from there for local builds (CI derives the version from the git tag instead).

## Spark dependency model

Spark, the spark-cassandra-connector, web3j, scallop and graphframes are declared
`Provided` in `build.sbt` — they are NOT in the package jar. At runtime they are
supplied via `spark-submit --packages` (see `docker/submit.sh`). When bumping any of
these, update both `build.sbt` and the `SPARK_PACKAGES` default in `docker/submit.sh`.

## Architecture

### Entry point and network dispatch

`org.graphsense.TransformationJob` is the top-level `main`. It reads the `--network`
arg and dispatches to one of two model families:

- `eth`, `trx` → `org.graphsense.account.TransformationJob` (account model)
- `btc`, `zec`, `bch`, `ltc` → `org.graphsense.utxo.TransformationJob` (UTXO model)

The two families have fundamentally different graph models (account balances vs.
unspent-output clustering) and do not share transformation code, only the helpers in
the top-level `org.graphsense` package.

### Account model (`org.graphsense.account.*`) — ETH, TRX

Cleanly layered into four roles, instantiated in `account/TransformationJob.scala`:

- **Job** (`eth/Job.scala` `EthereumJob`, `trx/Job.scala` `TronJob`; implement the
  `Job` trait) — orchestrate the ordered pipeline steps.
- **Source** (`CassandraEthSource`, `CassandraTrxSource`, extending
  `CassandraAccountSource`) — typed reads of raw Cassandra tables.
- **Sink** (`CassandraAccountSink` implementing the `AccountSink` trait) — typed
  writes of every transformed table.
- **Transformation** (`eth/Transformation.scala`, `trx/Transformation.scala`) — the
  actual Spark DataFrame/Dataset computations.

Token support: ERC-20-style token transfers are decoded from raw ETH `log` rows by
`contract/TokenTransferHelper.scala`; the supported token set per network lives in
`eth/Tokens.scala` / `trx/Tokens.scala`.

### UTXO model (`org.graphsense.utxo.*`) — BTC, LTC, ZEC, BCH

`utxo/TransformationJob.scala` `main` orchestrates the whole pipeline directly — no
Job/Source/Sink abstraction. It uses `CassandraStorage` directly for I/O and calls
`Transformation` / `Transformator` for computations. Key extra step vs. the account
model: address clustering via the multi-input heuristic, with optional CoinJoin
filtering (`--coinjoin-filtering`). `Fields.scala` holds column-name constants.

### Storage

`storage/CassandraStorage.scala` is the only Cassandra-facing code: generic
`load[T]` / `store[T]` over the spark-cassandra-connector, plus `isTableEmpty`. The
account model wraps it behind Source/Sink traits; the UTXO model calls it directly.

### Shared code (top-level `org.graphsense` package)

- `TransformHelpers.scala` — id-group bucketing, hash-prefix columns, secondary
  partition-id lookups (these implement the Cassandra partitioning scheme).
- `Util.scala` — timing wrappers, monotonic tx-id packing (`block << 32 | pos`),
  base58, sha256.
- `Traits.scala` — the `Job` trait (`run(from, to)`).
- `Model.scala` — shared exchange-rate models.

### CLI configuration

`scallop` parses args. `account/Config.scala` (`AccountConfig`) and `utxo/Config.scala`
(`UtxoConf`) define the option sets — bucket sizes, prefix lengths, min/max block,
cache dir, etc. Bucket sizes and prefix lengths control Cassandra partitioning and
must match the schema created by `graphsense-lib`.

## Tests

ScalaTest + `spark-fast-tests`. `src/test/scala/org/graphsense/Helpers.scala` defines
`TestBase` (a local `SparkSession` plus `assertDataFrameEquality`). Tests read fixture
inputs from `src/test/resources/<model>/<network>/...` (CSV/JSON) and compare computed
DataFrames against golden `reference/*.json` files in the same tree. `readTestData`
handles the CSV-can't-hold-binary problem by reading hex/base64 strings and casting to
`BinaryType`. The gitignored `test_ref/` directory holds test output dumps.
