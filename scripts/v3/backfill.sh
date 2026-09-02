#!/usr/bin/env bash
#
# v3 backfill / benchmark run, driven from the deployed Docker image.
#
# Writes ONLY to <network>_<kind>_v3[_<label>] keyspaces. That is enforced in
# code, not by this script: graphsense_v3.spark.writer refuses any keyspace name
# that does not match the v3 pattern, and no v2 name does. Production keyspaces
# are read from in exactly one place -- exchange_rates -- and never written.
#
# PREREQUISITES, in the order they bite:
#
#   1. AN IMAGE BUILT FROM THIS BRANCH. The published tag has no graphsense_v3.
#      Build and push, or build on the server:
#        docker build -t "$IMAGE:$TAG" .
#
#   2. THE EXECUTORS MUST BE ABLE TO IMPORT graphsense_v3. The pandas UDFs
#      (address encode/decode, varint) run ON THE EXECUTORS. The image bakes
#      /opt/graphsense/spark-env.tar.gz from the same wheel, so v3 is in it --
#      but the spark_config has to reference it:
#        spark.archives: "file:///opt/graphsense/spark-env.tar.gz#environment"
#        spark.executorEnv.PYTHONPATH: "./environment"
#      Do NOT override spark.pyspark.python; the archive carries no interpreter.
#      `verify` below checks this before anything is submitted.
#
#   3. pandas AND pyarrow ON THE EXECUTOR PYTHON. This job uses pandas_udf,
#      which the pubkey job did not. The archive does not carry them, so the
#      cluster's executor python must. Check on a worker host:
#        /home/spark/.venv/bin/python -c "import pandas, pyarrow; print('ok')"
#
# Usage:
#   ./scripts/v3/backfill.sh verify                 # image + archive checks only
#   ./scripts/v3/backfill.sh plan                   # what a run would touch
#   ./scripts/v3/backfill.sh create                 # create the keyspaces (RF 1)
#   ./scripts/v3/backfill.sh schema > v3.cql        # ... or the CQL, for cqlsh
#   ./scripts/v3/backfill.sh dry-run                # build + conform, no writes
#   ./scripts/v3/backfill.sh run                    # the real thing
#
# Env vars:
#   IMAGE, TAG          default graphsense-lib / v3
#   ENV                 config environment; default prod
#   NETWORK             default btc; both families transform
#   LABEL               keyspace suffix; default bench1
#   PROFILE             spark_config profile; default v3-utxo
#   WRITER              connector | sidecar; default sidecar
#   RF                  replication factor; default 1 (benchmark keyspace: half
#                       the disk, half the write cost, and nothing depends on
#                       it. NEVER 1 for anything that matters.)
#   START_BLOCK/END_BLOCK   bound the run. STRONGLY RECOMMENDED -- see below.
#   GRAPHSENSE_CONFIG   path to graphsense.yaml; default ./graphsense.yaml
#   ENV_FILE            optional docker --env-file for ${VAR} secrets
#
# WRITER: `sidecar` bulk-writes SSTables through the Cassandra Sidecar, the same
# path the TRON transform uses; `connector` goes through the CQL write path at
# throughputMBPerSec, which is what that transform moved off. Sidecar is the
# default here, but it has NOT yet been exercised from PySpark against a real
# cluster -- prove it on a small block range before a long run.
#
# WHY BOUND THE RUN ANYWAY: a full-history BTC backfill is hours of writing
# whichever path you pick, and a bounded slice answers the benchmark question.
set -euo pipefail

IMAGE="${IMAGE:-graphsense-lib}"
TAG="${TAG:-v3}"
ENV="${ENV:-prod}"
NETWORK="${NETWORK:-btc}"
LABEL="${LABEL:-bench1}"
PROFILE="${PROFILE:-v3-utxo}"
WRITER="${WRITER:-sidecar}"
RF="${RF:-1}"
GRAPHSENSE_CONFIG="${GRAPHSENSE_CONFIG:-$PWD/graphsense.yaml}"
START_BLOCK="${START_BLOCK:-}"
END_BLOCK="${END_BLOCK:-}"
ENV_FILE="${ENV_FILE:-}"

ENVFILE_ARG=()
[[ -n "$ENV_FILE" ]] && ENVFILE_ARG=(--env-file "$ENV_FILE")

# RF 1 is a deliberate choice for a benchmark keyspace and has to be said out
# loud: the code refuses it otherwise, because a production keyspace once sat at
# RF 1 unnoticed and a single node loss would have lost data outright.
REPLICATION=(--replication-factor "$RF")
[[ "$RF" == "1" ]] && REPLICATION+=(--allow-single-replica)

BOUNDS=()
[[ -n "$START_BLOCK" ]] && BOUNDS+=(--start-block "$START_BLOCK")
[[ -n "$END_BLOCK" ]] && BOUNDS+=(--end-block "$END_BLOCK")

# --network host: the container IS the Spark driver (client mode), so the
# workers must be able to route back to it.
v3() {
  docker run --rm --network host \
    -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \
    "${ENVFILE_ARG[@]}" \
    -v "$GRAPHSENSE_CONFIG:/graphsense.yaml:ro" \
    "$IMAGE:$TAG" graphsense-v3 "$@"
}

case "${1:-}" in
  verify)
    echo ">>> graphsense_v3 in the driver image"
    docker run --rm "$IMAGE:$TAG" python3 -c \
      "import graphsense_v3, graphsense_v3.spark.udf; print('driver ok')"
    echo ">>> graphsense_v3 in the baked executor archive"
    docker run --rm "$IMAGE:$TAG" sh -c \
      'mkdir -p /tmp/e && tar xzf /opt/graphsense/spark-env.tar.gz -C /tmp/e \
       && PYTHONPATH=/tmp/e python3 -c "import graphsense_v3.codec, graphsense_v3.spark.udf; print(\"executor archive ok\")"'
    echo
    echo "STILL TO CHECK BY HAND, on a worker host -- this job uses pandas_udf:"
    echo "  /home/spark/.venv/bin/python -c \"import pandas, pyarrow; print('ok')\""
    ;;
  plan)
    v3 plan -e "$ENV" -n "$NETWORK" --label "$LABEL" --spark-profile "$PROFILE"
    ;;
  create)
    v3 create -e "$ENV" -n "$NETWORK" --label "$LABEL" "${REPLICATION[@]}"
    ;;
  schema)
    v3 schema -n "$NETWORK" --kind raw --label "$LABEL" "${REPLICATION[@]}"
    v3 schema -n "$NETWORK" --kind derived --label "$LABEL" "${REPLICATION[@]}"
    ;;
  dry-run)
    v3 -v run -e "$ENV" -n "$NETWORK" --label "$LABEL" \
      --spark-profile "$PROFILE" --writer "$WRITER" "${BOUNDS[@]}" --dry-run
    ;;
  run)
    [[ -z "$END_BLOCK" ]] && {
      echo "refusing an unbounded run: set END_BLOCK (see the header)." >&2
      exit 2
    }
    v3 -v run -e "$ENV" -n "$NETWORK" --label "$LABEL" \
      --spark-profile "$PROFILE" --writer "$WRITER" "${BOUNDS[@]}" --yes
    ;;
  *)
    sed -n '2,50p' "$0"
    exit 1
    ;;
esac
