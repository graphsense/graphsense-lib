#!/usr/bin/env bash
#
# Full-history pubkey_v2 backfill across all chains (near-production test).
#
# ALPHA: the pubkey-update / pubkey-compact commands this drives are alpha and
# not yet production-validated. Run against isolated keyspaces/paths only.
#
# Writes the new cross-chain pubkey -> address lookup into a FRESH Cassandra
# keyspace (default: pubkey_v2) on the production cluster, isolated from the
# legacy "pubkey" table an older script owns. The REST reader keeps serving the
# legacy keyspace until you flip cross_chain_pubkey_mapping_keyspace.
#
# RESUMABLE: each invocation resumes from the per-network last_processed_block
# stored in the shared Delta `state` table. Re-running after a failure CONTINUES
# from where it stopped, it does not restart. So this whole script is safe to
# re-run; completed chains no-op and the failed chain picks up mid-way.
#
# Runs graphsense-cli inside the deployed Docker image (the same way we deploy),
# so the host needs only Docker + this script — no local graphsenselib install.
#
# Prereqs:
#   - Docker, able to pull ghcr.io/graphsense/graphsense-lib.
#   - A graphsense.yaml on the host (path in GRAPHSENSE_CONFIG); the env below
#     must define cassandra_nodes, the per-currency delta source sinks, an
#     s3_configs entry, and a spark_config with spark.master pointing at the
#     standalone cluster (NOT local — this drives the real cluster). Recommended:
#     add an environments.<ENV>.pubkey section once so you don't pass --sink-path:
#
#       environments:
#         <ENV>:
#           pubkey:
#             sink_path: s3://<staging-bucket>/pubkey-xchain   # FRESH, not a prod path
#             sink_type: cassandra
#             keyspace:  pubkey_v2
#
#   - Python UDFs run on the EXECUTORS, so the Spark workers need graphsenselib
#     + its native deps (coincurve, ...) in their Python. Otherwise the run dies
#     with `ModuleNotFoundError: No module named 'graphsenselib'` from the worker.
#     This image BAKES a minimal site-packages archive at
#     /opt/graphsense/spark-env.tar.gz — reference it in spark_config. The file://
#     scheme makes the driver's HTTP file server distribute it (NO S3/HDFS), and
#     PYTHONPATH makes the executors' OWN python import it. Do NOT override
#     spark.pyspark.python — keep the cluster's executor python:
#
#       spark_config:
#         spark.archives: "file:///opt/graphsense/spark-env.tar.gz#environment"
#         spark.executorEnv.PYTHONPATH: "./environment"
#
#     REQUIREMENT: the archive carries NO interpreter, only packages with native
#     wheels built for CPython 3.13 — the executors must run Python 3.13.x (you
#     confirmed 3.13.12). A different minor would fail to load the native .so;
#     then rebuild the archive for that version.
#
#   - If the config uses ${VAR} placeholders for secrets (s3 keys, cassandra
#     password), pass them to the container via ENV_FILE=/path/to/.env.
#   - Run on a host the Spark workers can route back to: the container is the
#     Spark driver (client mode), so we use --network host. If a firewall sits
#     between driver and workers, also pin spark.driver.port /
#     spark.blockManager.port in spark_config and open them.
#
# Image tag: defaults to the rolling `dev` tag (latest develop build). Pin a
# release candidate (TAG=v2.14.0-rc.1) or an immutable short-sha for repro.
#
# Usage:
#   ENV=prod S3_CONFIG=minio GRAPHSENSE_CONFIG=/etc/graphsense/graphsense.yaml \
#     ./scripts/pubkey/backfill.sh
#   # optional overrides:
#   ENV=prod S3_CONFIG=minio GRAPHSENSE_CONFIG=/etc/graphsense/graphsense.yaml \
#     TAG=dev ENV_FILE=/etc/graphsense/secrets.env \
#     PUBKEY_KEYSPACE=pubkey_v2 SINK_PATH=s3://staging/pubkey-xchain \
#     CHAINS="eth trx ltc zec bch btc" \
#     ./scripts/pubkey/backfill.sh
#
# Rehearsals (there is no native --dry-run):
#   DRY_RUN=1   echo every docker/graphsense-cli command without executing it
#               (validates tag/config/env/keyspace/arg wiring; no compute, no
#               writes, no docker pull). Combine with the env above.
#   END_BLOCK=N bound EVERY chain to block N for a small real run. Pair with
#               SINK_TYPE=delta to write nothing to Cassandra:
#                 DRY_RUN=1 ENV=prod S3_CONFIG=minio \
#                   GRAPHSENSE_CONFIG=/etc/graphsense/graphsense.yaml \
#                   ./scripts/pubkey/backfill.sh
#                 END_BLOCK=200000 SINK_TYPE=delta ENV=prod S3_CONFIG=minio \
#                   SINK_PATH=s3://staging/pubkey-rehearsal \
#                   GRAPHSENSE_CONFIG=/etc/graphsense/graphsense.yaml \
#                   ./scripts/pubkey/backfill.sh
#
# Limited, delta-only run over a subset of chains (e.g. bch/ltc/zec only):
#   set CHAINS to the subset and SINK_TYPE=delta. The single deferred
#   pubkey-detect pass then only finds pubkeys reused across THOSE chains (plus
#   whatever already sits in `observed` at SINK_PATH) — use a FRESH SINK_PATH if
#   you want results confined to exactly these three. bch defaults to starting
#   at its fork block; ltc/zec run full history unless you bound them:
#     CHAINS="ltc zec bch" SINK_TYPE=delta ENV=prod S3_CONFIG=minio \
#       SINK_PATH=s3://staging/pubkey-bch-ltc-zec \
#       GRAPHSENSE_CONFIG=/etc/graphsense/graphsense.yaml \
#       ./scripts/pubkey/backfill.sh
#   Add END_BLOCK=N to also cap the block range for a quick smoke test.
#
set -euo pipefail

DRY_RUN="${DRY_RUN:-0}"          # 1 => echo commands, do not execute
END_BLOCK="${END_BLOCK:-}"       # bound every chain to this block (rehearsals)
SINK_TYPE="${SINK_TYPE:-cassandra}"  # 'delta' writes no Cassandra rows
# 1 (default) => pass --skip-detect to EVERY pubkey-update so each chain only
# appends to 'observed', then run cross-chain detection ONCE via a final
# 'pubkey-detect' pass. Avoids re-running the full-table detection groupBy once
# per chain. A standalone final pass (not detection on the last chain) is used
# deliberately: a resumed run whose last chain is already up to date returns
# before its detection step, which would silently skip detection entirely.
# Set SKIP_DETECT=0 to detect inline after every chain (old behaviour).
SKIP_DETECT="${SKIP_DETECT:-1}"
# graphsense-cli global verbosity (counted: -v=warning, -vv=info, -vvv=debug).
# Default -vv so the resolved run parameters / startup banner are logged. The
# flag is global, so it goes BEFORE the subcommand. Set VERBOSITY= to silence.
VERBOSITY="${VERBOSITY:--vv}"

# --- Docker / image ---
IMAGE="${IMAGE:-ghcr.io/graphsense/graphsense-lib}"
TAG="${TAG:-dev}"  # rolling latest-develop build; pin -rc.N or <short-sha> for repro
GRAPHSENSE_CONFIG="${GRAPHSENSE_CONFIG:?set GRAPHSENSE_CONFIG=/abs/path/to/graphsense.yaml on the host}"
ENV_FILE="${ENV_FILE:-}"  # optional file with ${VAR} secrets the config references

# --- Job ---
ENV="${ENV:?set ENV (e.g. prod)}"
S3_CONFIG="${S3_CONFIG:?set S3_CONFIG (name of the s3_configs entry)}"
KS="${PUBKEY_KEYSPACE:-pubkey_v2}"

# Account chains first (cheap extract), UTXO next, BTC LAST (heaviest: wide
# transaction-input arrays). Override via CHAINS="...".
read -r -a CHAINS <<< "${CHAINS:-eth trx ltc zec bch btc}"

# --sink-path is optional if environments.<ENV>.pubkey.sink_path is configured.
SINK_ARG=()
[[ -n "${SINK_PATH:-}" ]] && SINK_ARG=(--sink-path "$SINK_PATH")

# Run graphsense-cli inside the container. --network host so the in-container
# Spark driver (client mode) is reachable by the standalone-cluster executors,
# and so cassandra / s3 / hdfs endpoints resolve as on the host. The config is
# mounted read-only and located via GRAPHSENSE_CONFIG_YAML.
ENVFILE_ARG=()
[[ -n "$ENV_FILE" ]] && ENVFILE_ARG=(--env-file "$ENV_FILE")
gs_cli() {
  # Global verbosity flag goes BEFORE the subcommand (it's on the root group).
  local verbosity_arg=()
  [[ -n "$VERBOSITY" ]] && verbosity_arg=("$VERBOSITY")
  local cmd=(docker run --rm
    --network host
    -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml
    "${ENVFILE_ARG[@]}"
    -v "$GRAPHSENSE_CONFIG:/graphsense.yaml:ro"
    "$IMAGE:$TAG"
    graphsense-cli "${verbosity_arg[@]}" "$@")
  if [[ "$DRY_RUN" != "0" ]]; then
    printf '[dry-run] '
    printf '%q ' "${cmd[@]}"
    printf '\n'
    return 0
  fi
  "${cmd[@]}"
}

echo "image=$IMAGE:$TAG  ENV=$ENV  keyspace=$KS  sink=$SINK_TYPE  chains=${CHAINS[*]}"
[[ "$SKIP_DETECT" == "1" ]] \
  && echo "Detection DEFERRED: appending all chains, then one pubkey-detect pass." \
  || echo "Detection INLINE: cross-chain detection runs after every chain."
echo "Reader keyspace is unchanged (legacy 'pubkey'); production is untouched."
[[ "$DRY_RUN" != "0" ]] && echo "DRY RUN: commands are printed, not executed."
[[ -n "$END_BLOCK" ]] && echo "REHEARSAL: every chain bounded to end-block=$END_BLOCK."
echo

# Refresh the rolling tag so we run the latest develop build.
if [[ "$DRY_RUN" == "0" ]]; then
  docker pull "$IMAGE:$TAG"
else
  echo "[dry-run] docker pull $IMAGE:$TAG"
fi
echo

# Optional per-chain block bound (rehearsals).
END_ARG=()
[[ -n "$END_BLOCK" ]] && END_ARG=(--end-block "$END_BLOCK")

# Defer cross-chain detection to a single final pass (see SKIP_DETECT above).
SKIP_ARG=()
[[ "$SKIP_DETECT" == "1" ]] && SKIP_ARG=(--skip-detect)

first=1
for C in "${CHAINS[@]}"; do
  CREATE=()
  # Create the keyspace/table once, on the first chain. IF NOT EXISTS, so it is
  # idempotent and harmless to leave on if you re-run. Skipped for delta sink.
  if [[ $first -eq 1 && "$SINK_TYPE" == "cassandra" ]]; then
    CREATE=(--create-schema)
  fi
  first=0

  echo ">>> [$C] pubkey-update  (sink=$SINK_TYPE${END_BLOCK:+, end-block=$END_BLOCK}${SKIP_DETECT:+, skip-detect=$SKIP_DETECT}, resume from state)"
  gs_cli transformation pubkey-update \
    -e "$ENV" -c "$C" \
    --sink-type "$SINK_TYPE" --pubkey-keyspace "$KS" \
    --s3-config "$S3_CONFIG" \
    "${SINK_ARG[@]}" "${CREATE[@]}" "${END_ARG[@]}" "${SKIP_ARG[@]}"
  echo ">>> [$C] done"
  echo

  # If BTC shows memory pressure as a single shot, stop and re-run BTC in
  # bounded chunks instead (state makes this safe), e.g.:
  #   for EB in 200000 400000 600000 ... <top>; do
  #     gs_cli transformation pubkey-update -e "$ENV" -c btc \
  #       --sink-type cassandra --pubkey-keyspace "$KS" --s3-config "$S3_CONFIG" \
  #       "${SINK_ARG[@]}" --end-block "$EB"
  #   done
done

# Detection was deferred per chain (--skip-detect): run it ONCE now over the
# fully-appended `observed`. Idempotent (anti-joins the materialised set), so a
# re-run after a failure here re-derives only what is still missing.
if [[ "$SKIP_DETECT" == "1" ]]; then
  echo ">>> cross-chain detection (single deferred pass)"
  gs_cli transformation pubkey-detect \
    -e "$ENV" --sink-type "$SINK_TYPE" --pubkey-keyspace "$KS" \
    --s3-config "$S3_CONFIG" "${SINK_ARG[@]}"
  echo
fi

# Deduplicate / bin-pack the shared `observed` table after the bulk append.
# Detection-neutral: it must not change which pubkeys are cross-chain.
echo ">>> compacting observed"
gs_cli transformation pubkey-compact \
  -e "$ENV" --s3-config "$S3_CONFIG" "${SINK_ARG[@]}"

echo
echo "Backfill complete. Validate (diff.py needs Spark+Cassandra; run it in the"
echo "same image, mounting the script and config):"
echo "  docker run --rm --network host \\"
echo "    -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \\"
echo "    -v $GRAPHSENSE_CONFIG:/graphsense.yaml:ro \\"
echo "    -v \$PWD/scripts/pubkey/diff.py:/diff.py:ro \\"
echo "    $IMAGE:$TAG \\"
echo "    python /diff.py --env $ENV --old-keyspace pubkey --new-keyspace $KS"
