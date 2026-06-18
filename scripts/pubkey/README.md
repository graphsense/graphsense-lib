# Cross-chain pubkey → address lookup

> ⚠️ **ALPHA.** The `pubkey-*` commands and these scripts are alpha and may
> change. Run them against **isolated keyspaces / paths**, never on top of a
> production dataset.

This feature finds public keys that were used on **two or more chains** and
materialises the addresses they map to on every supported chain, so the REST
API can answer "what other addresses across chains belong to the same key?".

It works in two layers:

1. **Per chain** — read transactions from the chain's source Delta Lake, extract
   the signing public keys (UTXO input/output scripts and the ETH/TRX account
   side), normalise them, and append them to a shared, append-only `observed`
   store.
2. **Cross-chain** — find keys seen on ≥2 chains that haven't been materialised
   yet, derive their addresses on all supported chains, and write the
   `pubkey → address` lookup to the configured backend.

The result is written either to **Cassandra** (served by the REST API) or to a
**Delta** table (cheaper to produce and review; load into Cassandra afterwards
with `pubkey-load`).

## Commands

All commands live under `graphsense-cli transformation` and read their settings
from your `graphsense.yaml`. The shared store location can be set once via
`environments.<env>.pubkey.sink_path` so you don't have to pass `--sink-path`
every time.

| Command | What it does |
|---|---|
| `pubkey-update -c <chain>` | Extract one chain's pubkeys and (by default) run cross-chain detection. **Resumable**: with no `--start-block` it continues from the last processed block stored in the Delta `state` table. |
| `pubkey-detect` | Run the cross-chain detection/materialisation step once over the fully-appended store (currency-agnostic). |
| `pubkey-compact` | Deduplicate/compact the append-only `observed` store between runs. Detection-neutral. |
| `pubkey-load` | Load a `sink_type=delta` run's `pubkey_by_address` table into Cassandra. |

Useful options (see `--help` for the full list):

- `--sink-type {cassandra,delta}` — where results go. `delta` writes no Cassandra
  rows, so it's safe for dry/staging runs.
- `--sink-path <path>` — base path of the shared store (or set it in the config).
- `--pubkey-keyspace <name>` — Cassandra write keyspace (defaults to a fresh
  `pubkey_v2`, isolated from any legacy `pubkey` keyspace).
- `--s3-config <name>` — the `s3_configs` entry holding S3/MinIO credentials.
- `--start-block` / `--end-block` — bound the range (rehearsals / chunked runs).
- `--skip-detect` — append only, defer detection to a later `pubkey-detect` pass.
- `--create-schema` — create the Cassandra keyspace/table if missing (idempotent).

### Why defer detection?

For a multi-chain run it is cheaper to append **all** chains first
(`pubkey-update --skip-detect` per chain) and then run detection **once**
(`pubkey-detect`), instead of re-running the full-table detection after every
chain. `backfill.sh` does this by default.

## `backfill.sh` — run all chains end to end

`backfill.sh` drives the commands above inside the published Docker image, so a
host needs only Docker + a `graphsense.yaml`. It appends every chain, runs one
detection pass, then compacts.

It is configured entirely through environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `ENV` | _(required)_ | Environment name in your `graphsense.yaml`. |
| `S3_CONFIG` | _(required)_ | Name of the `s3_configs` entry with credentials. |
| `GRAPHSENSE_CONFIG` | _(required)_ | Absolute path to `graphsense.yaml` on the host. |
| `SINK_PATH` | from config | Base path of the shared store (e.g. `s3://<bucket>/<pubkey-sink>`). |
| `SINK_TYPE` | `cassandra` | `delta` to write no Cassandra rows. |
| `CHAINS` | `eth trx ltc zec bch btc` | Chains to process, in order (account → UTXO → BTC last, as BTC is heaviest). |
| `PUBKEY_KEYSPACE` | `pubkey_v2` | Cassandra write keyspace. |
| `TAG` | `dev` | Image tag. Pin a release/short-sha for reproducible runs. |
| `ENV_FILE` | _(none)_ | File with `${VAR}` secrets your config references. |
| `END_BLOCK` | _(none)_ | Bound every chain to this block (smoke tests). |
| `SKIP_DETECT` | `1` | Defer detection to a single final pass (set `0` for inline). |
| `DRY_RUN` | `0` | `1` prints every command without executing (no pull, no compute). |

The host must be reachable by the Spark cluster's executors (the container is the
Spark driver in client mode, run with `--network host`), and the executors'
Python must be able to import `graphsenselib` (the image ships a relocatable env
the config can reference). See the comments at the top of `backfill.sh` for the
`spark_config` keys this needs.

### Examples

Dry run — validate wiring, run nothing:

```bash
DRY_RUN=1 ENV=<env> S3_CONFIG=<s3cfg> \
  GRAPHSENSE_CONFIG=/path/to/graphsense.yaml \
  ./scripts/pubkey/backfill.sh
```

Delta-only run over a subset of chains (use a **fresh** `SINK_PATH` if you want
results confined to exactly these chains):

```bash
CHAINS="ltc zec bch" SINK_TYPE=delta ENV=<env> S3_CONFIG=<s3cfg> \
  SINK_PATH=s3://<bucket>/<pubkey-sink> \
  GRAPHSENSE_CONFIG=/path/to/graphsense.yaml \
  ./scripts/pubkey/backfill.sh
```

### Recurring / incremental runs (no script needed)

Because every `pubkey-update` resumes from the Delta `state` table, re-running
the commands on a schedule **is** the incremental-update path — each run only
processes the blocks added since the last run, then re-detects and compacts. It
is safe to re-run after a failure: completed chains no-op and the failed chain
picks up mid-way.

For production you don't need `backfill.sh` — the same end-to-end run is a single
`docker run` with an inline loop, so nothing has to be deployed to the host
besides the config:

```bash
docker run --rm --network host \
  -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \
  -v /path/to/graphsense.yaml:/graphsense.yaml:ro \
  --entrypoint bash ghcr.io/graphsense/graphsense-lib:<tag> -c '
    set -e
    for C in <chains>; do
      graphsense-cli transformation pubkey-update -e <env> -c "$C" \
        --sink-type delta --s3-config <s3cfg> --sink-path s3://<bucket>/<pubkey-sink> \
        --pubkey-keyspace pubkey_v2 --skip-detect
    done
    graphsense-cli transformation pubkey-detect -e <env> \
      --sink-type delta --s3-config <s3cfg> --sink-path s3://<bucket>/<pubkey-sink> \
      --pubkey-keyspace pubkey_v2
    graphsense-cli transformation pubkey-compact -e <env> \
      --s3-config <s3cfg> --sink-path s3://<bucket>/<pubkey-sink>
  '
```

This needs the same `graphsense.yaml` (Cassandra nodes, per-currency source
sinks, an `s3_configs` entry, and a `spark_config` whose `spark.master` points at
the cluster and that references the executor env the image ships) as a full
backfill — see the comments at the top of `backfill.sh`.

For an unattended schedule (e.g. weekly cron): pin `<tag>` to a release for
reproducibility, wrap the call in `flock` so a long run never overlaps the next
tick, and redirect output to a dated log file.

If you run `--sink-type delta` (cheaper, writes no Cassandra rows), follow up with
`pubkey-load` to publish the result into Cassandra for the REST API — or use
`--sink-type cassandra` throughout to write directly.

## Serving the result via the REST API

The reader chooses its source keyspace independently of the writer, via the
`cassandra.cross_chain_pubkey_mapping_keyspace` setting. It accepts either a
single keyspace or a **list**:

```yaml
cassandra:
  # serve the new keyspace, and keep the legacy one for keys the new pipeline
  # can't reproduce — looked up in each and merged:
  cross_chain_pubkey_mapping_keyspace: [pubkey_v2, pubkey]
```

The feature auto-enables when at least one configured keyspace actually has the
`pubkey_by_address` table; keyspaces missing it are skipped. Set it to a single
string (the default `pubkey`) for the original single-keyspace behaviour, or to
`null` to disable the lookup.
