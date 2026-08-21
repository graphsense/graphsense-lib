# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working inside `spark/`. This tree is
part of the **graphsense-lib monorepo** — it was the standalone `graphsense-spark`
repository until 2026-08, which is now archived. See the repository-root `CLAUDE.md`
first; the rule that matters most lives there.

## Read this before changing anything here

**The Spark pipeline and the Python delta updater (`src/graphsenselib/deltaupdate/`)
must agree column-for-column.** Both produce the same derived tables — Spark from
scratch, the delta updater incrementally — so a change on one side that the other
cannot reproduce is a divergence bug that only surfaces at the next full re-run.
That is why the two now live in one repo. Any change to schema, transforms,
aggregation, or address/cluster/token accounting here needs the corresponding
delta-updater change, ideally in the same commit. CI enforces the reverse direction
too: `.github/workflows/spark_tests.yml` runs this suite on changes to
`deltaupdate/**` and the transformed schema, not just to `spark/**`.

## What this is

The GraphSense Spark Transformation Pipeline: a Scala / Apache Spark batch job that
reads raw blockchain block & transaction data from Apache Cassandra (ingested by the
Python side of this repo) and computes a de-normalized "transformed" keyspace
(address graph, address/cluster relations, balances, statistics) written back into
Cassandra. The transformed keyspace is then served by `graphsense-rest`.

## Build & test commands

Build tooling is SBT (Scala 2.12.17), separate from the repo's Python tooling — the
Python formatting hooks deliberately skip this tree. The targets below are
`spark/Makefile`'s, so run them from **this** directory; from the repository root the
same work is `make test-spark` / `format-spark` / `lint-spark` / `build-spark-jar`
(plain `make test` at the root runs the Python suite).

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

The release version is the `RELEASE` line in this directory's `Makefile` —
`build.sbt` parses it from there for local builds (CI derives the version from the
git tag instead). It must stay in step with `SPARKSEM` in the repository-root
`Makefile`; `make tag-spark-version` refuses to tag if the two disagree.

Jars ship on their own release track, `spark-vYY.MM.P` (see `VERSIONING.md`), so a
Python-only release does not force a jar rebuild and an operator can pin a jar
independently of the library version. `build.sbt` derives the version from the tag's
FIRST character, so the release workflow strips the `spark-` prefix before invoking
sbt. Jars for `v26.08.0` and earlier live on releases of the archived standalone
repo, which is kept precisely so those pinned assets keep resolving.

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
