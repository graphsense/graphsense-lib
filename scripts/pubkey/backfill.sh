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
set -euo pipefail

DRY_RUN="${DRY_RUN:-0}"          # 1 => echo commands, do not execute
END_BLOCK="${END_BLOCK:-}"       # bound every chain to this block (rehearsals)
SINK_TYPE="${SINK_TYPE:-cassandra}"  # 'delta' writes no Cassandra rows
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

first=1
for C in "${CHAINS[@]}"; do
  CREATE=()
  # Create the keyspace/table once, on the first chain. IF NOT EXISTS, so it is
  # idempotent and harmless to leave on if you re-run. Skipped for delta sink.
  if [[ $first -eq 1 && "$SINK_TYPE" == "cassandra" ]]; then
    CREATE=(--create-schema)
  fi
  first=0

  echo ">>> [$C] pubkey-update  (sink=$SINK_TYPE${END_BLOCK:+, end-block=$END_BLOCK}, resume from state)"
  gs_cli transformation pubkey-update \
    -e "$ENV" -c "$C" \
    --sink-type "$SINK_TYPE" --pubkey-keyspace "$KS" \
    --s3-config "$S3_CONFIG" \
    "${SINK_ARG[@]}" "${CREATE[@]}" "${END_ARG[@]}"
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
