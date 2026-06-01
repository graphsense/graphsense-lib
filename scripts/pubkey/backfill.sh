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
# Prereqs:
#   - GRAPHSENSE config (graphsense.yaml) reachable; the env below must define
#     cassandra_nodes, the per-currency delta source sinks, and an s3_configs
#     entry. Recommended: add an environments.<ENV>.pubkey section once so you
#     don't pass --sink-path every run:
#
#       environments:
#         <ENV>:
#           pubkey:
#             sink_path: s3://<staging-bucket>/pubkey-xchain   # FRESH, not a prod path
#             sink_type: cassandra
#             keyspace:  pubkey_v2
#
#   - Run on the Spark driver host (spark.master comes from the spark_config
#     profile in graphsense.yaml). Do NOT pass --local for a prod-scale run.
#
# Usage:
#   ENV=prod S3_CONFIG=minio ./scripts/pubkey/backfill.sh
#   # optional overrides:
#   ENV=prod S3_CONFIG=minio PUBKEY_KEYSPACE=pubkey_v2 \
#     SINK_PATH=s3://staging/pubkey-xchain CHAINS="eth trx ltc zec bch btc" \
#     ./scripts/pubkey/backfill.sh
#
set -euo pipefail

ENV="${ENV:?set ENV (e.g. prod)}"
S3_CONFIG="${S3_CONFIG:?set S3_CONFIG (name of the s3_configs entry)}"
KS="${PUBKEY_KEYSPACE:-pubkey_v2}"

# Account chains first (cheap extract), UTXO next, BTC LAST (heaviest: wide
# transaction-input arrays). Override via CHAINS="...".
read -r -a CHAINS <<< "${CHAINS:-eth trx ltc zec bch btc}"

# --sink-path is optional if environments.<ENV>.pubkey.sink_path is configured.
SINK_ARG=()
[[ -n "${SINK_PATH:-}" ]] && SINK_ARG=(--sink-path "$SINK_PATH")

echo "ENV=$ENV  keyspace=$KS  chains=${CHAINS[*]}"
echo "Reader keyspace is unchanged (legacy 'pubkey'); production is untouched."
echo

first=1
for C in "${CHAINS[@]}"; do
  CREATE=()
  # Create the keyspace/table once, on the first chain. IF NOT EXISTS, so it is
  # idempotent and harmless to leave on if you re-run.
  if [[ $first -eq 1 ]]; then CREATE=(--create-schema); first=0; fi

  echo ">>> [$C] pubkey-update -> ${KS}.pubkey_by_address  (resume from state)"
  graphsense-cli transformation pubkey-update \
    -e "$ENV" -c "$C" \
    --sink-type cassandra --pubkey-keyspace "$KS" \
    --s3-config "$S3_CONFIG" \
    "${SINK_ARG[@]}" "${CREATE[@]}"
  echo ">>> [$C] done"
  echo

  # If BTC shows memory pressure as a single shot, stop and re-run BTC in
  # bounded chunks instead (state makes this safe), e.g.:
  #   for EB in 200000 400000 600000 ... <top>; do
  #     graphsense-cli transformation pubkey-update -e "$ENV" -c btc \
  #       --sink-type cassandra --pubkey-keyspace "$KS" --s3-config "$S3_CONFIG" \
  #       "${SINK_ARG[@]}" --end-block "$EB"
  #   done
done

# Deduplicate / bin-pack the shared `observed` table after the bulk append.
# Detection-neutral: it must not change which pubkeys are cross-chain.
echo ">>> compacting observed"
graphsense-cli transformation pubkey-compact \
  -e "$ENV" --s3-config "$S3_CONFIG" "${SINK_ARG[@]}"

echo
echo "Backfill complete. Validate with:"
echo "  uv run python scripts/pubkey/diff.py --env $ENV --old-keyspace pubkey --new-keyspace $KS"
