# GraphSense Library

[![Test and Build Status](https://github.com/graphsense/graphsense-lib/actions/workflows/run_tests.yaml/badge.svg)](https://github.com/graphsense/graphsense-lib/actions) [![PyPI version](https://badge.fury.io/py/graphsense-lib.svg)](https://badge.fury.io/py/graphsense-lib) [![Python](https://img.shields.io/pypi/pyversions/graphsense-lib)](https://pypi.org/project/graphsense-lib/) [![Downloads](https://static.pepy.tech/badge/graphsense-lib)](https://pepy.tech/project/graphsense-lib)

A comprehensive Python library for the GraphSense crypto-analytics platform. It provides database access, data ingestion, maintenance tools, and analysis capabilities for cryptocurrency transactions and networks.

> **Note:** This library uses optional dependencies. Use `graphsense-lib[all]` to install all features.

## Quick Start

### Installation

```bash
# Install with all features
uv add graphsense-lib[all]

# Install from source
git clone https://github.com/graphsense/graphsense-lib.git
cd graphsense-lib
make install
```

### Serving the REST API locally

The web API requires two backend connections: a **Cassandra** cluster (blockchain data) and a **TagStore** (PostgreSQL). You can configure them via environment variables or a YAML config file.

#### Option A: Environment variables only

```bash
GS_CASSANDRA_ASYNC_NODES='["<cassandra-host>"]' \
GRAPHSENSE_TAGSTORE_READ_URL='postgresql+asyncpg://<user>:<password>@<host>:<port>/tagstore' \
GS_CASSANDRA_ASYNC_CURRENCIES='{"btc":{"raw": "btc_raw", "transformed": "btc_transformed"},"eth":{}}' \
uv run --extra web uvicorn graphsenselib.web.app:create_app --factory --host localhost --port 9000 --reload
```

#### Option B: YAML config file

Point `CONFIG_FILE` to a REST-specific config (see `instance/config.yaml` for a full example):

```bash
CONFIG_FILE=./instance/config.yaml make serve-web
```

Or without Make:

```bash
CONFIG_FILE=./instance/config.yaml \
uv run --extra web uvicorn graphsenselib.web.app:create_app --factory --host localhost --port 9000 --reload
```

#### Option C: `.graphsense.yaml` with a `web` key

If you already have a `.graphsense.yaml` (or `~/.graphsense.yaml`) for the CLI, you can add a `web` key containing the REST config. The app will pick it up automatically without setting `CONFIG_FILE`:

```yaml
# .graphsense.yaml
environments:
  # ... your existing CLI config ...

web:
  database:
    nodes: ["<cassandra-host>"]
    currencies:
      btc:
      eth:
  gs-tagstore:
    url: "postgresql+asyncpg://<user>:<password>@<host>:<port>/tagstore"
```

```bash
make serve-web
```

**Config resolution order:** explicit `config_file` param > `CONFIG_FILE` env var > `./instance/config.yaml` > `.graphsense.yaml` `web` key > env vars only.

#### Environment variable substitution in YAML

Any string value in a config file may reference environment variables with `${VAR}`. This works in both the CLI config (`.graphsense.yaml`) and the REST config, and applies to nested values (lists, maps) too. Useful for keeping secrets out of the file:

```yaml
gs-tagstore:
  url: ${TAGSTORE_URL}
database:
  nodes:
    - ${CASSANDRA_HOST}
```

- **Defaults:** `${VAR:-fallback}` uses `fallback` when `VAR` is unset or empty. Use `${VAR:-}` to allow an empty value.
- **Errors:** a `${VAR}` referencing an unset variable with no default fails loudly at load time.
- **Escaping:** write `$${VAR}` to emit a literal `${VAR}` without substitution. A bare `$VAR` (no braces) is never substituted.

#### Optional REST settings (env vars)

| Variable | Default | Description |
|---|---|---|
| `GSREST_DISABLE_AUTH` | `false` | Disable API key authentication |
| `GSREST_ENSURE_TAGSTORE_SCHEMA_ON_STARTUP` | `false` | Auto-initialize TagStore tables/views at startup when missing |
| `GRAPHSENSE_TAGSTORE_READ_URL` | — | TagStore database URL (e.g., `postgresql://user:password@host:5432/tagstore`) |
| `GSREST_ALLOWED_ORIGINS` | `*` | CORS allowed origins |
| `GSREST_LOGGING_LEVEL` | — | Logging level (DEBUG, INFO, …) |
| `GS_CASSANDRA_ASYNC_PORT` | `9042` | Cassandra port |
| `GS_CASSANDRA_ASYNC_USERNAME` | — | Cassandra username |
| `GS_CASSANDRA_ASYNC_PASSWORD` | — | Cassandra password |

> **Precedence: the YAML config file wins over these environment variables.**
> This is the reverse of the more common convention, so it is easy to get caught
> out by. The variables above are a *fallback* for keys the config file does not
> set — they do not override it.
>
> Precedence is decided **per key**, not per block. Given
> `database: {nodes: [...], username: gs_user}`, a `GS_CASSANDRA_ASYNC_USERNAME`
> is ignored (the file sets it), while `GS_CASSANDRA_ASYNC_PASSWORD` still
> applies (the file does not). The same rule covers the `GSREST_*` variables:
> `GSREST_DISABLE_AUTH=true` has no effect if the file already sets
> `disable_auth: false`.
>
> To make an environment variable authoritative for a key the config file sets —
> the usual case for rotating a Cassandra password without editing the file —
> reference it from the file instead of shadowing it:
>
> ```yaml
> database:
>   username: gs_user
>   password: ${CASSANDRA_PASSWORD}
> ```
>
> See [Environment variable substitution in YAML](#environment-variable-substitution-in-yaml)
> above. Note that `${VAR}` and `GS_CASSANDRA_ASYNC_PASSWORD` are independent
> mechanisms: the placeholder is resolved when the file is read, so the variable
> name it references is yours to choose and need not match any name in this table.
>
> This ordering applies to the **REST config only**. The CLI config
> (`.graphsense.yaml`, `GRAPHSENSE_*`) resolves the two the other way round —
> there a `GRAPHSENSE_`-prefixed variable *does* override the file. `${VAR}`
> substitution works the same in both, which is why it is the portable way to
> keep a secret out of either config file.

When enabling `GSREST_ENSURE_TAGSTORE_SCHEMA_ON_STARTUP=true`, keep in mind:

- The DB user must have DDL privileges (create tables/views/indexes/extensions/procedures).
- Startup may be slower because schema checks and potential initialization run before the app serves traffic.
- In multi-replica deployments, initialize schema once (migration/init job) to avoid startup races.

If TagStore is not configured (`gs-tagstore` missing) or the TagStore URL is unreachable, the REST app now falls back to a mock TagStore so endpoints still work. In this mode, tag-specific responses (labels, actors, taxonomies, tag counts) are empty.

### MCP (Model Context Protocol) interface

The web stack also serves a curated MCP endpoint at `/mcp` (path configurable via `GS_MCP_PATH`) so LLM clients — Claude Code, Claude Desktop, Cursor, custom agents — can query GraphSense directly without writing glue code. It is **mounted inside the main FastAPI app**: one `uvicorn` process serves both REST and MCP.

```bash
# Install with the mcp extra (pulls in fastmcp)
uv add 'graphsense-lib[mcp]'

# Start the stack — MCP comes up automatically at /mcp
uv run uvicorn graphsenselib.web.app:create_app --factory --port 9000

# Drift gate (suitable for CI; no DB needed):
uv run graphsense-cli mcp validate-curation
```

The tool surface is strictly curated — **not** every REST endpoint becomes a tool. Consolidations merge endpoints that LLMs always chain (e.g. one `lookup_address` replaces four address-level calls). See [`src/graphsenselib/mcp/README.md`](src/graphsenselib/mcp/README.md) for the design principles, the full tool catalog, how to add or modify tools, deployment details (transport, lifespan composition), and the proprietary `search_neighbors` forward.

#### Using the GraphSense skills in Claude

The MCP ships agent skills that teach Claude how to run an investigation with
the GraphSense tools: three investigators, which differ only in how far they
go beyond the question, and three shared skills they load.

| Skill | What it does |
| --- | --- |
| `investigate-strict` | The default. Answers exactly what was asked. No suggestions, no extra leads, no unrequested exports. |
| `investigate-advisor` | Answers, then suggests one next step or a Pathfinder export. |
| `investigate-autonomous` | Follows the obvious leads itself, one or two hops beyond the finding, and ends with findings instead of suggestions. |
| `identifier-integrity` | Shared: never write an address, hash or link from memory or complete a truncated one. |
| `trace-funds` | Shared: which tools to use and how to follow funds hop by hop without closing a trace on a guess. |
| `investigation-reporting` | Shared: provenance per fact, High/Medium/Low confidence, no legal judgment, AI-generated disclaimer. |

**Claude Code.** Install the `graphsense` plugin from this repository. It
brings the skills and registers the GraphSense MCP, so no separate
`claude mcp add` is needed:

```bash
claude plugin marketplace add graphsense/graphsense-lib
claude plugin install graphsense@graphsense
```

On install, Claude Code asks for the MCP URL (default
`https://api.iknaio.com/mcp/`, the hosted Iknaio endpoint) and your API key,
which is sent as the `Authorization` header like for the REST API. Then either
call an investigator directly:

```text
/graphsense:investigate-advisor bc1q... where did the funds go after March 2024?
/graphsense:investigate-strict 0x... is this address tagged?
```

or just ask ("trace the funds from bc1q..."): Claude picks the investigator
from the skill descriptions, `investigate-strict` unless you ask for advice on
next steps or for Claude to keep going on its own. Pull updates with
`claude plugin marketplace update graphsense`.

**Claude app (claude.ai, Desktop).** Add GraphSense as a connector, then
upload the skills: zip each skill folder and upload the zips under
*Settings → Capabilities → Skills* (organization admins can provision them for
everyone). Upload all six; the investigators load the shared ones by name.

```bash
cd src/graphsenselib/mcp/skills
mkdir -p /tmp/gs-skills
for d in */; do zip -qr "/tmp/gs-skills/${d%/}.zip" "$d"; done
```

**Other MCP clients.** The skills are also MCP resources
(`skill://<name>/SKILL.md`), so any client can read them, or sync them to a
skills directory with FastMCP:

```python
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.utilities.skills import sync_skills

transport = StreamableHttpTransport(
    "https://api.iknaio.com/mcp/",
    headers={"Authorization": API_KEY},  # the bare key, no "Bearer"
)
async with Client(transport) as client:
    await sync_skills(client, "~/.claude/skills")
```

See [`src/graphsenselib/mcp/README.md`](src/graphsenselib/mcp/README.md#skills)
for how the skills are built and tested.

### REST API evolution and deprecation policy

The REST API follows semantic versioning via `info.version` in the OpenAPI spec
and the `__api_version__` field in the library:

- **Patch** (`2.10.x`): bug fixes, no schema changes.
- **Minor** (`2.y.0`): additive changes — new endpoints, new fields, new
  optional parameters. Deprecations may be introduced here but deprecated
  surfaces continue to work.
- **Major** (`x.0.0`): removal of deprecated surfaces and other breaking
  changes. Major bumps are rare and announced in advance.

Deprecated endpoints and fields remain fully functional for **at least two
minor releases or six months**, whichever is longer. Deprecations are announced
through three mechanisms, listed from most to least machine-readable:

#### OpenAPI schema (`deprecated: true`)

Deprecated paths and response fields carry `deprecated: true` in
`/openapi.json`, which renders as a strikethrough in Swagger UI (`/docs`) and
is propagated to the generated Python client's docstrings. Check the spec at
build time to fail CI if you depend on a deprecated surface.

#### HTTP response headers (RFC 9745 / RFC 8594)

Responses from deprecated routes carry:

- **`Deprecation: true`** — [RFC 9745](https://www.rfc-editor.org/rfc/rfc9745).
  Signals that this specific endpoint is deprecated. Currently emitted as the
  literal string `true`; future releases may upgrade it to an `@<epoch>`
  timestamp indicating when deprecation took effect.
- **`Link: </docs#section/Deprecation-policy>; rel="deprecation"; type="text/html"`** —
  points clients at the authoritative policy page.
- **`Sunset: <HTTP-date>`** — [RFC 8594](https://www.rfc-editor.org/rfc/rfc8594).
  Announces the committed removal date for the endpoint. After that date
  the deprecated endpoint may be removed without further notice. The
  `/entities/...` endpoints (superseded by `/clusters/...`) were removed
  this way after their sunset on 31 Oct 2026.

To detect deprecation in your own client code, inspect the `Deprecation` and
`Sunset` response headers and log a warning (or fail CI) when you hit a
deprecated surface. Example with the generated Python client:

```python
from graphsense import ApiClient, Configuration
from graphsense.api import ClustersApi

cfg = Configuration(host="https://api.iknaio.com", api_key={"api_key": "..."})
with ApiClient(cfg) as api_client:
    clusters = ClustersApi(api_client)
    response = clusters.get_cluster_with_http_info(currency="btc", cluster=264711)
    headers = response.headers
    if headers.get("Deprecation"):
        sunset = headers.get("Sunset", "no sunset date set")
        print(f"WARNING: endpoint deprecated (sunset: {sunset})")
```

#### CHANGELOG

Every deprecation is recorded in [`CHANGELOG.md`](CHANGELOG.md) under the
release that introduced it, and every removal is recorded in the major release
that applies it. Use the changelog as the audit trail when planning client
upgrades.

### Basic Usage

#### Database Access with Configuration File

```python
from graphsenselib.db import DbFactory

# Using GraphSense config file (default: ~/.graphsense.yaml)
with DbFactory().from_config("development", "btc") as db:
    highest_block = db.transformed.get_highest_block()
    print(f"Highest BTC block: {highest_block}")

    # Get block details
    block = db.transformed.get_block(100000)
    print(f"Block 100000: {block.block_hash}")
```

#### Direct Database Connection

```python
from graphsenselib.db import DbFactory

# Direct connection without config file
with DbFactory().from_name(
    raw_keyspace_name="eth_raw",
    transformed_keyspace_name="eth_transformed",
    schema_type="account",
    cassandra_nodes=["localhost"],
    currency="eth"
) as db:
    print(f"Highest block: {db.transformed.get_highest_block()}")
```

#### Async Database Services

The async services are used internally by the REST API and can also be used standalone. `AddressesService` depends on several other services:

```python
from graphsenselib.db.asynchronous.services import (
    BlocksService, AddressesService, TagsService,
    EntitiesService, RatesService,
)

# Services are initialized with their dependencies
blocks_service = BlocksService(db, rates_service, config, logger)
addresses_service = AddressesService(
    db, tags_service, entities_service, blocks_service, rates_service, logger
)

address_info = await addresses_service.get_address("btc", "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
txs = await addresses_service.list_address_txs("btc", "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
```

## Command Line Interface

GraphSense-lib exposes a comprehensive CLI tool: `graphsense-cli`

### Basic Commands

```bash
# Show help and available commands
graphsense-cli --help

# Check version
graphsense-cli version

# Show current configuration
graphsense-cli config show

# Generate config template
graphsense-cli config template > ~/.graphsense.yaml

# Show config file path
graphsense-cli config path
```

## Modules

### Database Management

Query and manage the GraphSense database state.

```bash
# Show database management options
graphsense-cli db --help

# Check database state/summary
graphsense-cli db state -e development

# Get block information
graphsense-cli db block info -e development -c btc --height 100000

# Query logs (for Ethereum-based chains)
graphsense-cli db logs -e development -c eth --from-block 1000000 --to-block 1000100
```

### Schema Operations

Create and validate database schemas.

```bash
# Show schema options
graphsense-cli schema --help

# Create database schema for a currency
graphsense-cli schema create -e dev -c btc

# Validate existing schema
graphsense-cli schema validate -e dev -c btc

# Show expected schema for currency
graphsense-cli schema show-by-currency btc

# Show schema by type (utxo/account)
graphsense-cli schema show-by-schema-type utxo
```

### Data Ingestion

Ingest raw cryptocurrency data from nodes.

```bash
# Show ingestion options
graphsense-cli ingest --help

# Ingest blocks from cryptocurrency node
graphsense-cli ingest from-node \
    -e dev \
    -c btc \
    --start-block 0 \
    --end-block 1000 \
    --create-schema

# Ingest with custom batch size
graphsense-cli ingest from-node \
    -e dev \
    -c eth \
    --start-block 1000000 \
    --end-block 1001000 \
    --batch-size 100
```

### Full Transform (raw → transformed)

Run the raw → transformed "full transform" by driving the Scala Spark
transformation job via `spark-submit`. That job lives in this repository under
[`spark/`](spark/README.md) (formerly the standalone `graphsense-spark` repo) and
is released on its own `spark-vX.Y.Z` tag track — see [VERSIONING.md](VERSIONING.md).
The release jar is downloaded from a public GitHub Release asset (token-free) and
cached under `<cache_directory>/spark-jars`. The command creates a fresh dated
transformed keyspace, resolves Spark properties from a `spark_config` profile, and
launches the job.

```bash
# Show options
graphsense-cli transformation raw-to-transformed --help

# Run against a pinned release, creating a fresh dated transformed keyspace
graphsense-cli transformation raw-to-transformed -e prod -c btc --version spark-v26.09.1

# Use the latest stable release (also the default when no version is configured)
graphsense-cli transformation raw-to-transformed -e prod -c btc --version latest

# Preview only: print the resolved Spark config + spark-submit command, no side effects
graphsense-cli transformation raw-to-transformed -e prod -c btc --dry-run

# Run Spark locally for testing
graphsense-cli transformation raw-to-transformed -e dev -c btc --local

# Write into an existing transformed keyspace instead of a fresh dated one
graphsense-cli transformation raw-to-transformed -e prod -c btc \
    --target-keyspace btc_transformed

# Pass extra args through to the Spark transformation job after `--`
graphsense-cli transformation raw-to-transformed -e prod -c btc -- --debug 1
```

Configured via a `full_transform_args` section in the config file:

```yaml
full_transform_args:
  # Repository publishing the jar releases, and the tag prefix of its Spark
  # release track. These are the defaults: jars built from `spark/` in this
  # monorepo, whose releases span several tracks, so the prefix is what makes
  # "latest" resolve to a Spark release rather than a Python-only one that
  # carries no jar. To pin a jar from before the move (`v26.08.0` and earlier),
  # point `repo` at `graphsense/graphsense-spark` and set the prefix to `""` —
  # that repo is archived rather than deleted so its assets keep resolving.
  repo: graphsense/graphsense-lib
  release_tag_prefix: spark-
  # Release tag to run. Omit or set to "latest" to resolve the newest stable
  # (non-prerelease) release of that track from the GitHub API at run time.
  version: spark-v26.09.1
  version_overrides: # optional, per-currency
    eth: spark-v26.09.1
  artifact: fat # "fat" (self-contained assembly, default) or "slim" (+ Maven packages)
  spark_profile: # selects a spark_config profile per currency
    btc: utxo
  jar_args: # optional, per-currency extra job args (override data_configuration)
    btc: ["--checkpoint-dir", "file:///data/spark-checkpoint"]
  # Optional Cassandra Sidecar bulk-write path
  sidecar:
    enabled: false
    contact_points: ["cassandra-1:9043", "cassandra-2:9043"]
    local_dc: dc1
    consistency_level: LOCAL_QUORUM
```

Spark properties (including `spark.master`) come from the selected `spark_config`
profile; Cassandra coordinates (host/port/credentials) are taken from the
environment config. The command is backend-neutral (`backend: scala` today,
`pyspark` reserved) so a future native implementation can be selected without
changing how it is invoked.

The layout of the transformed keyspace (`bucket_size`, `address_prefix_length`,
`bech_32_prefix` and `coinjoin_filtering` for UTXO chains; `tx_prefix_length`,
`block_bucket_size_address_txs` and `addressrelations_ids_nbuckets` for account
chains) comes from `keyspace_setup_config.transformed.data_configuration` and is
passed to the job as `--bucket-size`, `--bech32-prefix`, etc. Keys left out are
filled from graphsense-lib's per-currency defaults, which are the production
layout. Passing them explicitly keeps older job releases, whose built-in
defaults differ, on the same layout. An option set in `jar_args` or after `--`
takes precedence.

Earlier versions ignored `data_configuration`. So that a stale or mistaken
value there cannot change the layout unnoticed, the command fails before
creating the keyspace if a value differs from the default, and lists the
values that do. Pass `--override-defaults` to use them anyway.

#### Java runtime when submitting from the Docker image

The Docker image defaults to Java 17 but also ships a secondary Temurin 11 JRE
at `/opt/java11`. If you submit against a standalone cluster whose executors run
Java 11 (the prod cluster — its hosts are shared with Cassandra 4.x, which caps
them there) and the profile uses `spark.serializer=KryoSerializer` (the full
transform profiles do), the driver must run Java 11 too: Kryo serializes
JDK-internal field layouts that differ between major Java versions, so a
Java-17 driver fails to deserialize task results from Java-11 executors
(`java.io.EOFException` in `TaskResultGetter`). Opt in per run by setting
`JAVA_HOME` — `spark-submit` picks it up, nothing else changes:

```bash
# From the host
docker run -e JAVA_HOME=/opt/java11 ... graphsense-cli transformation raw-to-transformed -e prod -c trx ...

# Or inside the container
JAVA_HOME=/opt/java11 graphsense-cli transformation raw-to-transformed -e prod -c trx ...
```

Drop the override once the cluster JVM moves to 17+ (needs Cassandra 5.x on the
shared hosts first).

### Delta Updates

Update transformed keyspace from raw keyspace.

```bash
# Show delta update options
graphsense-cli delta-update --help

# Check update status
graphsense-cli delta-update status -e dev -c btc

# Perform delta update
graphsense-cli delta-update update -e dev -c btc

# Validate delta update consistency
graphsense-cli delta-update validate -e dev -c btc

# Patch exchange rates for specific blocks
graphsense-cli delta-update patch-exchange-rates \
    -e dev \
    -c btc \
    --start-block 100000 \
    --end-block 200000
```

### Continuous Sync

`sync run` replaces the per-network cron jobs (`ingest from-node`, then
`delta-update update`). It is one long-running daemon that polls each node
and runs ingest and the delta update as soon as there is something new, as
child processes of the same graphsense-lib version. Claims, pauses and status
live in Redis when `use_redis_locks`/`redis_url` are set, otherwise in files
under `/tmp/graphsense-sync/<env>`. Without Redis, daemons in separate
containers need that directory, and `/tmp` for the jobs' file locks, on a
shared volume.

#### Configuration

How a network is ingested and updated belongs in its keyspace config, so a
manual run, a cron job and the daemon all do the same. The flags of the same
name still override it for one run. When moving a network from cron, every
flag of its cron command goes here (`--write-mode append` and
`--updater-version 2` are the defaults and can be dropped):

| cron flag | keyspace config |
| --- | --- |
| `--exchange-rates-provider coingecko` | `ingest_config.exchange_rates_provider` |
| `--sinks cassandra --sinks delta` | `ingest_config.sinks` |
| `--auto-compact "1w;sunday"`, `--auto-compact-last-n 16` | `ingest_config.auto_compact`, `auto_compact_last_n` |
| `--write-batch-size`, `--parallel-workers`, `--forward-fill-rates` | `delta_update_config.*` |

A missing `sinks` makes the daemon ingest into Cassandra only, and the Delta
Lake falls behind silently; a missing `auto_compact` means no compaction.

```yaml
environments:
  prod:
    keyspaces:
      eth:
        ingest_config:
          exchange_rates_provider: coingecko
          sinks: [cassandra, delta]       # default --sinks
          auto_compact: "1w;sunday"       # compact on Sundays, at most weekly
          auto_compact_last_n: 16
        delta_update_config:
          forward_fill_rates: true
          write_batch_size: 60
          parallel_workers: 8
      zec:
        ingest_config:
          exchange_rates_provider: coingecko
          sinks: [cassandra, delta]
          auto_compact: "1w;sat"
          # reorg_backoff_blocks: 24      # override the built-in backoff (zec: 100)
        delta_update_config:
          forward_fill_rates: true
          write_batch_size: 40
          parallel_workers: 4
    sync:
      rates_every_minutes: 30             # exchange rates with every Nth ingest only
      stop_grace_seconds: 480             # see "Stopping and switching images"
      networks:
        eth:
          poll_seconds: 600
          lag_alert_minutes: 60
        zec:
          poll_seconds: 600
          lag_alert_minutes: 300
```

A network is only run by the daemon if it has an entry under
`sync.networks`: without one, `sync run -c <net>` warns once and runs nothing
until the entry appears (the config is re-read on change); with `--once` it
exits 2. Entries without a daemon are harmless; `sync status` lists them as
"no daemon", which makes it safe to prepare the config for every network
while the cron jobs still run (older versions only warn about the keys they
do not know).

- **`poll_seconds`** (default 30): how often the node is checked. Every check
  that finds new blocks starts an ingest and a delta update, each a new
  process with a few seconds of startup, so for fast chains (eth, trx, zec)
  a few minutes collect several blocks per job instead of one.
- **`lag_alert_minutes`**: alert when the newest raw block is older than
  this. Raw always trails the node by the reorg backoff (built in: btc 3,
  ltc 12, bch 10, zec 100, eth 70, trx 20 blocks; `reorg_backoff_blocks`
  overrides it), so the threshold must exceed backoff × block time, plus the
  poll interval and some block-time jitter. It replaces the per-run
  `raw_ingest_staleness_threshold`, which the daemon turns off.
- **`rates_every_minutes`**: ingest jobs skip the exchange-rates pre-step
  except once per interval, and always in the first cycle after a start.

The daemon passes `-e`, `-c`, `--end-block`, `--no-exchange-rates`,
`--no-staleness-check` and `--no-auto-compact` itself. It compacts in a step
of its own (`ingest delta-lake auto-compact`, checked every
`compaction_check_every_minutes`, timeout `compaction_timeout_minutes`), so a
long compaction does not hit the ingest timeout. `ingest_args` /
`delta_update_args` take extra flags for the rare case the config has no
field for. The config file is re-read when it changes, so none of these
settings needs a restart.

#### Running with docker compose

Run one daemon per network, each in its own container (`-c`). A daemon runs
its network's jobs one after another, so the container's memory limit works
like `docker run -m` per job of a cron job. Its version is the image tag, so
a canary is one service on a newer tag. The shared settings go into one
block, so a network is four lines:

```yaml
# compose.yaml
x-sync: &sync
  image: ghcr.io/graphsense/graphsense-lib:v2.19.0
  restart: unless-stopped
  init: true                      # reaps the jobs' child processes
  stop_grace_period: 10m          # > sync.stop_grace_seconds (8 min)
  mem_limit: 45g                  # per running job, like docker run -m
  environment:
    AWS_EC2_METADATA_DISABLED: "true"
  volumes:
    - ~/.graphsense.yaml:/home/graphsense/.graphsense.yaml:ro
    - /srv/graphsense-sync:/tmp/graphsense-sync   # only without Redis
  logging:
    driver: json-file
    options: {max-size: 100m, max-file: "5"}

services:
  sync-btc:
    <<: *sync
    container_name: sync-btc
    command: graphsense-cli -v sync run -e prod -c btc
  sync-zec:
    <<: *sync
    container_name: sync-zec
    command: graphsense-cli -v sync run -e prod -c zec
    image: ghcr.io/graphsense/graphsense-lib:v2.20.0-rc.1   # canary
```

Give the containers the same network setup the cron containers had (e.g.
`extra_hosts` for node hostnames). A service's own `environment` or `volumes`
replaces the shared list instead of extending it, so repeat the shared
entries there. `.graphsense.yaml` is mounted as a single file: an editor
that saves by writing a new file and renaming it is not seen inside the
container until it restarts.

The `max_parallel_*` limits count within one daemon, so with one network per
daemon they limit nothing, and delta updates of different networks can
overlap, as cron jobs that start at the same minute do. Several networks can
also share a daemon: give them a `channel` and run `sync run --channel
<name>`. Its jobs then share one memory limit, so size the container for
what can run at once. A channel only groups networks onto a daemon; the
release track is the image tag.

#### Moving a network from cron to the daemon

1. Put its cron flags into the keyspace config and add it to
   `sync.networks` (see above). `sync status` then lists it as "no daemon".
2. Comment out its cron line and wait until no job of it is running.
3. Check that `sync status` shows its WAL as `empty`. A record written by
   another version blocks the first run (see below).
4. Run one cycle by hand and watch it:
   `docker compose run --rm sync-<net> graphsense-cli -v sync run -e prod --once -c <net>`.
   Expect the reorg check to pass silently, an ingest into both sinks, a
   delta update, and `finished: ok` for each.
5. Add its service to the compose file and `docker compose up -d sync-<net>`.

#### Scheduled tasks

Jobs that belong to no single network, such as the cross-chain pubkey
update, go under `sync.tasks`. Each runs at a time of day, optionally on
one weekday only:

```yaml
    sync:
      tasks:
        pubkey:
          args: [transformation, pubkey-update-all, --spark-profile, pubkey,
                 --sink-type, delta, --s3-config, spark-ingest]
          weekday: saturday               # unset: every day
          at: "19:00"                     # UTC
          channel: tasks
          timeout_minutes: 720
          retry_minutes: 60               # after a failure, until it succeeds
          success_topic: pubkey           # optional; failures go to alert_topic
```

The daemon appends `-e <env>` to `args`. A task runs on a daemon of its
`channel` started without `-c`, and on one of them at a time. A run counts
for the most recent scheduled time, and the last run is kept with the rest
of the sync state, so a restart neither repeats the run nor drops it. A time
missed while no daemon ran is caught up when one starts. A failure alerts
and is retried every `retry_minutes`. A run stopped with the daemon starts
again on the next daemon. `sync status` lists the tasks, and
`sync pause/resume --task <name>` work like they do for a network.

Give a Spark task a channel and a container of its own. The Spark driver
must be reachable from the executors (`network_mode: host`, as for the
cron container), and the job needs a larger memory limit:

```yaml
  sync-tasks:
    <<: *sync
    container_name: sync-tasks
    network_mode: host
    command: graphsense-cli -v sync run -e prod --channel tasks
```

Spark jobs may read a network's Delta Lake while its compaction runs:
vacuum keeps replaced files for 48 hours, so a running reader's snapshot
stays readable.

#### Stopping and switching images

SIGTERM (`docker stop`, `docker compose up -d` with a new tag) stops the
daemon from starting jobs; a running job finishes its current chunk or batch
and exits, within `sync.stop_grace_seconds` (default 480) before it is
killed. A second signal lists what is still running, a third kills the jobs.
Killing is safe for the data (ingest writes `block` last, the delta update
replays its WAL), but a killed job's Redis lock, which has no TTL, stays
behind until `redis-cli DEL`.

So switching a daemon to a new image, or back, is:

```bash
# change the tag in compose.yaml, then
docker compose up -d
```

as long as the container's `stop_grace_period` is longer than
`stop_grace_seconds`. Compose pulls the new image before it stops the old
container. Two things to know:

- A Delta Lake compaction cannot stop midway. If one is running (on the day
  `auto_compact` names), it is killed after the grace period and its lock
  stays behind; the data is safe.
- The delta update refuses to replay a WAL record written by another
  version (`--force-wal-replay` overrides it). A clean stop never leaves
  one, but a killed one does. When unsure, stop first and check:

  ```bash
  graphsense-cli sync pause -e prod -c zec --reason "image switch" --for 30m --wait
  # shows the network once no cycle runs; WAL "empty": change the tag, then
  docker compose up -d sync-zec
  graphsense-cli sync resume -e prod -c zec
  ```

  This is also how to switch away from a daemon older than the version that
  introduced `stop_grace_seconds` (see CHANGELOG), which stopped jobs after
  2 minutes and could leave a lock behind.

Go back to an older version only if the newer one's schema migrations are
backward compatible.

#### Status and day-to-day commands

```bash
graphsense-cli sync status -e prod
graphsense-cli sync pause -e prod -c zec --reason "node resync" --for 6h
graphsense-cli sync resume -e prod -c zec
graphsense-cli sync run -e prod --once -c zec   # one cycle, then exit
```

```
sync prod · 1 daemon
  zec · daemon f4f1ddc88fb7 on 3a9c2e1d0b7f
    dev build 817aec8 (2.19.0.dev55), up 25m

zec · idle · next cycle in 6m
  node         3,526,805  (target 3,526,705, backoff 100)
  raw          3,526,705  0 behind target
  transformed  3,526,705
  WAL          empty
  last         ok, 15:12 UTC (10m ago)
```

`sync status` reads the shared state, so it works from any container and
any version, including a one-off `docker compose run --rm sync-<net> …`
next to a running daemon. Its WAL line is `empty`, or the pending blocks and
the version that wrote them, or `?` when the check failed.

Before every ingest, the newest raw block hashes are compared with the
node's. A mismatch means a reorg went deeper than the reorg backoff, and the
network is paused until someone has repaired raw and runs `sync resume`.

### Exchange Rates

Fetch and ingest exchange rates from various sources.

```bash
# Show exchange rate options
graphsense-cli exchange-rates --help

# Fetch from CoinMarketCap (requires API key in config)
graphsense-cli exchange-rates coinmarketcap -e dev -c btc

# Ingest from CryptoCompare (requires cryptocompare_api_key in config,
# see https://developers.coindesk.com/)
graphsense-cli exchange-rates cryptocompare ingest -e dev -c btc --abort-on-gaps
```

### Monitoring

Monitor GraphSense infrastructure health and state.

```bash
# Show monitoring options
graphsense-cli monitoring --help

# Get database summary
graphsense-cli monitoring get-summary -e dev

# Get summary for specific currency
graphsense-cli monitoring get-summary -e dev -c btc

# Send notifications to configured handlers
graphsense-cli monitoring notify \
    --topic "database-update" \
    --message "BTC ingestion completed"
```

### Event Watching (Deprecated)

> **Deprecated:** `graphsense-cli watch` will be removed in a release after
> 2027-04-09.

Watch for cryptocurrency events and generate notifications.

```bash
# Show watch options
graphsense-cli watch --help

# Watch for money flows on the addresses listed in a watchpoints file
graphsense-cli watch money-flows \
    -e dev \
    -c btc \
    --watchpoints-file watchpoints.json \
    --state-file watcher-state.json
```

### File Conversion Tools

Convert between different file formats.

```bash
# Show conversion options
graphsense-cli convert --help
```

## Configuration

GraphSense-lib uses a YAML configuration file that defines database connections and environment settings. Default locations: `./.graphsense.yaml`, `~/.graphsense.yaml`.

### Generate Configuration Template

```bash
graphsense-cli config template > ~/.graphsense.yaml
```

### Example Configuration Structure

```yaml
# Optional: default environment to use
default_environment: dev

environments:
  dev:
    # Cassandra cluster configuration
    # Default port is 9042. To use a non-default port, embed it in the host
    # string ("host:port"); all nodes should use the same port (a warning is
    # logged and the first port wins if they differ).
    cassandra_nodes: ["localhost"]
    # Optional authentication
    # username: "cassandra"
    # password: "cassandra"

    # Consistency levels for the synchronous (ingest/write) connection.
    # consistency_level applies to the whole ExecutionProfile (reads + writes);
    # serial_consistency_level only affects lightweight transactions
    # (IF NOT EXISTS upserts). Defaults shown below.
    # consistency_level: "LOCAL_QUORUM"
    # serial_consistency_level: "LOCAL_SERIAL"

    # Currency/keyspace configurations
    keyspaces:
      btc:
        raw_keyspace_name: "btc_raw"
        transformed_keyspace_name: "btc_transformed"
        schema_type: "utxo"

        # Node connection for ingestion
        ingest_config:
          node_reference: "http://localhost:8332"
          # Optional authentication for node
          # username: "rpcuser"
          # password: "rpcpassword"
          # Optional: staleness tolerance in hours. After every
          # `ingest from-node` run the timestamp of the highest ingested raw
          # block is checked against this threshold and a notification is sent
          # if the data is older (same check as `monitoring monitor-raw-ingest`).
          # raw_ingest_staleness_threshold: 10
          # Optional: ingest the latest exchange rates from this provider
          # before every `ingest from-node` run (same as `exchange-rates
          # <provider> ingest --abort-on-gaps`). Supported: coingecko,
          # coinmarketcap, cryptocompare (each needs its API key below).
          # exchange_rates_provider: "coingecko"

        # Keyspace setup for schema creation
        keyspace_setup_config:
          raw:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"
          transformed:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"

      eth:
        raw_keyspace_name: "eth_raw"
        transformed_keyspace_name: "eth_transformed"
        schema_type: "account"

        ingest_config:
          node_reference: "http://localhost:8545"

        keyspace_setup_config:
          raw:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"
          transformed:
            replication_config: "{'class': 'SimpleStrategy', 'replication_factor': 1}"

  prod:
    cassandra_nodes: ["cassandra1.prod", "cassandra2.prod", "cassandra3.prod"]
    username: "gs_user"
    password: "secure_password"

    keyspaces:
      btc:
        raw_keyspace_name: "btc_raw"
        transformed_keyspace_name: "btc_transformed"
        schema_type: "utxo"

        ingest_config:
          node_reference: "http://bitcoin-node.internal:8332"

        keyspace_setup_config:
          raw:
            replication_config: "{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}"
          transformed:
            replication_config: "{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}"

# Optional: Slack notification configuration
slack_topics:
  database-update:
    hooks: ["https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"]

  payment_flow_notifications:
    hooks: ["https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"]

# Optional: API keys for external services
coingecko_api_key: ""
coinmarketcap_api_key: "YOUR_CMC_API_KEY"
# Required for the cryptocompare exchange-rates provider since June 2026
# (https://developers.coindesk.com/)
cryptocompare_api_key: ""

# Optional: cache directory for temporary files
cache_directory: "~/.graphsense/cache"
```

## Advanced Features

### Tagpack Management

GraphSense-lib includes comprehensive tagpack management tools (formerly standalone tagpack-tool). For detailed documentation, see [Tagpack README](tagpack/docs/README.md).

```bash
# Validate tagpacks
graphsense-cli tagpack-tool tagpack validate /path/to/tagpack

# Insert tagpack into tagstore
graphsense-cli tagpack-tool insert \
    --url "postgresql://user:pass@localhost/tagstore" \
    /path/to/tagpack

# Show quality measures
graphsense-cli tagpack-tool quality show-measures \
    --url "postgresql://user:pass@localhost/tagstore"
```

### Tagstore Operations

```bash
# Initialize tagstore database
graphsense-cli tagstore init

# Initialize with custom database URL
graphsense-cli tagstore init --db-url "postgresql://user:pass@localhost/tagstore"

# Get DDL SQL for manual setup
graphsense-cli tagstore get-create-sql
```

### Cross-chain Analysis

```python
# Using an initialized AddressesService (see above for setup)
related = await addresses_service.get_cross_chain_pubkey_related_addresses(
    "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
)

for addr in related:
    print(f"Network: {addr.network}, Address: {addr.address}")
```

### Function Call Parsing

```python
from graphsenselib.utils.function_call_parser import parse_function_call

# Parse Ethereum function calls
function_signatures = {
    "0xa9059cbb": [{
        "name": "transfer",
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"}
        ]
    }]
}

parsed = parse_function_call(tx_input_bytes, function_signatures)
if parsed:
    print(f"Function: {parsed['name']}")
    print(f"Parameters: {parsed['parameters']}")
```

## Development

**Important:** Requires Python >=3.11, <3.14 and [uv](https://docs.astral.sh/uv/) >=0.13
(`required-version` in `pyproject.toml`; run `uv self update` if uv refuses to start).

### Setup Development Environment

```bash
# Initialize development environment (installs deps + pre-commit hooks)
make dev

# Or install dev dependencies only
make install-dev
```

### Code Quality and Testing

```bash
# Format code
make format

# Lint code
make lint

# The gate the pre-commit hook runs (~33s)
make test-fast

# Run every hook over the whole tree
make pre-commit
```

**`make test-fast` is a subset, not the full suite.** It runs 1307 of 2327
tests: it skips the `slow` marks and the four directories whose tests need a
Cassandra/Postgres/Redis testcontainer (`tests/web`, `tests/db`,
`tests/tagstore`, `tests/integration` — ~80s of a ~95s suite, and the fixtures
are session-scoped so the cost is all-or-nothing). That keeps the commit loop
at ~33s; CI runs everything on every push and PR.

Before opening a PR, or whenever you touch the web/REST, DB or tagstore layers,
run the full suite yourself:

```bash
# The whole Python suite, slow tests included (~105s) -- what CI runs
make test-ci

# Rust clustering extension (also gated on rust/** in pre-commit)
make test-rust

# Scala pipeline. No hook runs this; CI does, on every push and PR.
make test-spark
```

| command | scope | time |
| --- | --- | --- |
| `make test-fast` | Python minus slow + container tests (1307 tests) | ~33s |
| `make test-ci` | whole Python suite, slow included (2327 tests) | ~105s |
| `make test` | as `test-ci`, plus coverage and a forced dependency sync | ~145s |
| `make test-rust` | `cargo test` | 3.3s cold, 0.06s warm |
| `make test-spark` | `sbt test` | minutes |

#### Without a Docker daemon

The suite collects and runs without one. Tests that need a Cassandra, Postgres
or Redis container (`tests/web`, `tests/db`, `tests/tagstore`,
`tests/integration`) are skipped with a single reason and a banner in the
summary. On CI (`CI` set) an unreachable daemon is a hard error instead, so a
broken runner cannot pass while covering nothing.

#### Podman Notes

If you run the test suite with Podman, make sure your shell points at the Podman socket:

```bash
export DOCKER_HOST="unix://${XDG_RUNTIME_DIR}/podman/podman.sock"
```

The test fixtures automatically disable Ryuk when `DOCKER_HOST` contains `podman.sock` and rely on explicit fixture cleanup instead.

### Release Process

This repository uses two source-of-truth versions in the root `Makefile`:

- **Library version**: `RELEASESEM` (released with `vX.Y.Z`, `vX.Y.Z-rc.N`, or `vX.Y.Z-dev.N` tags)
- **OpenAPI/API version**: `WEBAPISEM` (written to `src/graphsenselib/web/version.py`)

The Python client package version is derived from the API version and should match it.

Library package versioning is dynamic via `setuptools_scm` (`pyproject.toml`):

- Git tag `v2.9.8` -> package version `2.9.8`
- Git tag `v2.9.8-rc.1` -> package version `2.9.8rc1`
- Git tag `v2.9.8-dev.1` -> package version `2.9.8.dev1`
- Commits after a tag append local metadata, for example `2.9.8.dev1+g<sha>.d<date>`

Use the root Makefile helpers:

```bash
# Show all current versions
make show-versions

# Update and validate OpenAPI contract version
make update-api-version WEBAPISEM=v2.10.0
make check-api-version WEBAPISEM=v2.10.0

# Sync client version from API version and validate
make sync-client-version WEBAPISEM=v2.10.0
make check-client-version WEBAPISEM=v2.10.0

# Generate Python client (package version = OpenAPI info.version)
make generate-python-client

# Create both release tags from Makefile versions
make tag-version
```

Tagging behavior:

- Library release tag: `vX.Y.Z`, `vX.Y.Z-rc.N`, or `vX.Y.Z-dev.N` (from `RELEASESEM`)
- Client release tag: `webapi-vA.B.C` (from `WEBAPISEM`)

Recommended library versioning routine:

1. For development prereleases, set `RELEASESEM` to `vX.Y.Z-dev.N` (for example `v2.10.0-dev.1`)
2. For release candidates, set `RELEASESEM` to `vX.Y.Z-rc.N`
3. For stable releases, set `RELEASESEM` to `vX.Y.Z`
4. Create tags with `make tag-version`
5. Push tags with `git push origin --tags`

CI trigger background:

- Stable library tags (`vX.Y.Z`) trigger:
  - GitHub Release creation
  - Python library package build/publish (`graphsense-lib`)
  - Docker image build/publish
- Client tags (`webapi-vA.B.C`) trigger Python client package build/publish (`clients/python`)
- Other library tags (`vX.Y.Z-rc.N`, `vX.Y.Z-dev.N`) do not trigger GitHub Release or Python package publish; they only trigger Docker image build/publish

1. Update CHANGELOG.md with new features and fixes
2. Update relevant versions (library/API/client) based on what changed
3. Sync API/client versions if needed (`make update-api-version` + `make sync-client-version`)
4. Create and push tags:

```bash
make tag-version
git push origin --tags
```

## Troubleshooting

### OpenSSL Errors

Some components use OpenSSL hash functions that aren't available by default in OpenSSL 3.0+ (e.g., ripemd160). This can cause test suite failures. To fix this, enable legacy providers in your OpenSSL configuration. See the "fix openssl legacy mode" step in `.github/workflows/run_tests.yaml` for an example.

### Common Issues

1. **Connection Refused**: Verify Cassandra is running and accessible
2. **Schema Validation Errors**: Ensure database schema matches expected version
3. **Import Errors**: Install with `[all]` option for complete feature set
4. **Python Version**: Requires Python >=3.11, <3.14

### Getting Help

- Check [GitHub Issues](https://github.com/graphsense/graphsense-lib/issues)
- Review [GraphSense Documentation](https://graphsense.github.io/)
- Use `--help` with any CLI command for detailed usage information
- For tagpack-specific issues, see [Tagpack Documentation](tagpack/docs/README.md)

## License

See LICENSE file for licensing details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make pre-commit` for the fast gate, then `make test-ci` for the full
   suite (the hook deliberately skips the slow and container-backed tests)
5. Submit a pull request

---

**GraphSense** - Open Source Crypto Analytics Platform
Website: https://graphsense.github.io/
