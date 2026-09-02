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
#   1. AN IMAGE BUILT FROM THIS BRANCH. CI publishes one on every feature/**
#      push (github-packages-publish.yaml), tagged both by branch and by short
#      SHA. `verify` pulls it. A release tag will NOT do: graphsense_v3 only
#      exists on this branch.
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
#   3. pandas AND pyarrow. This job uses pandas_udf, which the pubkey job did
#      not. They are BAKED INTO THE ARCHIVE at the same versions the driver
#      runs, so the executors get a matching pair rather than whatever the
#      cluster venv happens to hold -- `verify` checks it.
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
#   PULL                1 (default) pulls before every command; 0 skips
#   IMAGE, TAG          default ghcr.io/graphsense/graphsense-lib /
#                       feature-backend-v3. PIN THE SHORT SHA for a run you
#                       want to reproduce -- the branch tag moves on the
#                       next push, and a benchmark whose image changed
#                       underneath is worse than no benchmark. TAG may be a
#                       sha256:... digest, which is addressed with @ not :.
#   ENV                 config environment; default prod
#   NETWORK             default btc; both families transform
#   LABEL               keyspace suffix; default bench1
#   PROFILE             spark_config profile; default v3-utxo
#   WRITER              connector | sidecar; default connector
#   FULL                set to 1 for a full-history run (no END_BLOCK)
#   RF                  replication factor; default 1 (benchmark keyspace: half
#                       the disk, half the write cost, and nothing depends on
#                       it. NEVER 1 for anything that matters.)
#   START_BLOCK/END_BLOCK   bound the run. STRONGLY RECOMMENDED -- see below.
#   GRAPHSENSE_CONFIG   path to graphsense.yaml; default ./graphsense.yaml
#   ENV_FILE            optional docker --env-file for ${VAR} secrets
#
# WRITER: `sidecar` bulk-writes SSTables through the Cassandra Sidecar, the same
# path the TRON transform uses; `connector` goes through the CQL write path at
# throughputMBPerSec, which is what that transform moved off. Sidecar is much
# faster for volume but has NOT been exercised from PySpark against a real
# cluster, so `connector` is the default -- prove the pipeline on the path that
# has run, then switch.
#
# BOUND THE FIRST RUN. `run` refuses an unbounded one unless FULL=1, because a
# bounded slice exercises every seam in minutes and a full history does not.
set -euo pipefail

IMAGE="${IMAGE:-ghcr.io/graphsense/graphsense-lib}"
TAG="${TAG:-feature-backend-v3}"
ENV="${ENV:-prod}"
NETWORK="${NETWORK:-btc}"
LABEL="${LABEL:-bench1}"
PROFILE="${PROFILE:-v3-utxo}"
WRITER="${WRITER:-connector}"
RF="${RF:-1}"
GRAPHSENSE_CONFIG="${GRAPHSENSE_CONFIG:-$PWD/graphsense.yaml}"
START_BLOCK="${START_BLOCK:-}"
END_BLOCK="${END_BLOCK:-}"
ENV_FILE="${ENV_FILE:-}"

# A digest pins the image; a tag does not. `TAG=sha256:...` addresses it by
# digest, which is what a run you intend to reproduce should use.
if [[ "$TAG" == sha256:* ]]; then
  IMAGE_REF="$IMAGE@$TAG"
else
  IMAGE_REF="$IMAGE:$TAG"
fi

# Pull for EVERY command, not just verify: the branch tag moves on each push, so
# a `run` a day after a `verify` would otherwise write with yesterday's image
# while `plan` reported today's schema. A digest ref is immutable, and a tag
# already current costs one manifest check. PULL=0 skips it.
if [[ "${PULL:-1}" == "1" && -n "${1:-}" && "${1:-}" != "-h" ]]; then
  docker pull "$IMAGE_REF" >/dev/null || {
    echo "could not pull $IMAGE_REF" >&2
    exit 2
  }
  echo "image  $(docker image inspect --format '{{index .RepoDigests 0}}' \
    "$IMAGE_REF" 2>/dev/null || echo "$IMAGE_REF (local)")"
fi

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
    "$IMAGE_REF" graphsense-v3 "$@"
}

case "${1:-}" in
  verify)
    echo ">>> graphsense_v3 in the driver image"
    docker run --rm "$IMAGE_REF" python3 -c \
      "import graphsense_v3, graphsense_v3.spark.udf; print('driver ok')"
    echo ">>> graphsense_v3 in the baked executor archive"
    docker run --rm "$IMAGE_REF" sh -c \
      'mkdir -p /tmp/e && tar xzf /opt/graphsense/spark-env.tar.gz -C /tmp/e \
       && PYTHONPATH=/tmp/e python3 -c "import graphsense_v3.codec, graphsense_v3.spark.udf; print(\"executor archive ok\")"'
    echo ">>> pandas/pyarrow in the archive, and matching the driver"
    docker run --rm "$IMAGE_REF" sh -c \
      'mkdir -p /tmp/e && tar xzf /opt/graphsense/spark-env.tar.gz -C /tmp/e \
       && PYTHONPATH=/tmp/e python3 -c "
import pandas, pyarrow, pyspark
print(\"archive pandas\", pandas.__version__, \"pyarrow\", pyarrow.__version__)
print(\"driver pyspark\", pyspark.__version__)
assert pandas.__file__.startswith(\"/tmp/e\"), pandas.__file__
assert pyarrow.__file__.startswith(\"/tmp/e\"), pyarrow.__file__
"'
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
    if [[ -z "$END_BLOCK" && "${FULL:-0}" != "1" ]]; then
      echo "refusing an unbounded run: set END_BLOCK, or FULL=1 to mean it." >&2
      echo "A bounded slice exercises every seam in minutes; do that first." >&2
      exit 2
    fi
    v3 -v run -e "$ENV" -n "$NETWORK" --label "$LABEL" \
      --spark-profile "$PROFILE" --writer "$WRITER" "${BOUNDS[@]}" --yes
    ;;
  *)
    sed -n '2,50p' "$0"
    exit 1
    ;;
esac
