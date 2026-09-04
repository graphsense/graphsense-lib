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
#   ./scripts/v3/backfill.sh probe                  # read-only: every DAL query
#   ./scripts/v3/backfill.sh run                    # the real thing
#
# Env vars:
#   SPARK_LOCAL_DIR     the real nvme, passed through at the same path on both
#                       sides; must match the profile's spark.local.dir.
#                       Default /var/data/nvme4/spark/local_storage
#   DRIVER_SCRATCH      small host dir mounted at its PARENT, so the driver has
#                       a writable /var/data/nvme4/spark inside the container.
#                       Default /home/iknaio/gs-docker-cache/driver-scratch
#   CACHE_DIR           holds the Ivy cache; default /home/iknaio/gs-docker-cache
#   JAVA11_HOME/CACERTS host JDK 11 and its truststore, mounted for the driver
#   BE0_IP              IP for ikn-vie02-client01[-be0]; default 172.22.240.71
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
#   WRITER              connector | sidecar; default sidecar
#   ACCEPT_PREFLIGHT    set to 1 to proceed past preflight problems. Only for
#                       findings that are properties of the CHAIN rather than
#                       of the run -- BCH block 556045 really does hold 166,882
#                       transactions, and no configuration makes that untrue.
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
# throughputMBPerSec, which is what that transform moved off.
#
# Sidecar is the default since 2026-09-03, when both paths wrote LTC 0-100000
# and `summary_statistics` matched exactly (297506 transactions, 311608
# addresses, 7004294 relations): 4m28s against 88m, and `transaction_io` alone
# went from 4933s to under 85s. Set WRITER=connector to compare.
#
# It is NOT uniformly faster. The bulk path pays fixed per-write overhead
# (SSTable generation, then streaming), so on the small derived tables it loses
# -- address_tx_pages 1.0s -> 4.1s, address_by_prefix 3.0s -> 7.6s, and the
# derived stage as a whole 113s -> 128s. It wins where there is volume, which
# is the whole of the raw stage and all of a full-history run.
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
WRITER="${WRITER:-sidecar}"
ACCEPT_PREFLIGHT="${ACCEPT_PREFLIGHT:-}"
PREFLIGHT_ARG=()
[[ "$ACCEPT_PREFLIGHT" == "1" ]] && PREFLIGHT_ARG=(--accept-preflight)
RF="${RF:-1}"
DATACENTER="${DATACENTER:-DC1}"
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

# --- container plumbing, from the working TRON full-transform run ------------
# That run drives the Scala job, but almost all of this is cluster-shaped
# rather than Scala-shaped. What is NOT carried over is the sidecar's Vert.x
# cache dir, since this defaults to the connector write path.

# JAVA 11 FOR THE DRIVER, and this is not optional. The image ships Java 17;
# the cluster's executors run Java 11. A JDK-17 driver deserialising Kryo task
# results from JDK-11 executors fails with java.io.EOFException in
# TaskResultGetter -- and the v3-utxo profile sets KryoSerializer, inherited
# from transform-utxo, so this job is exposed to it exactly as the Scala one
# was. The read-only host JDK has no usable cert symlinks inside the container,
# so the adoptium cacerts are mounted and set as the truststore; Ivy needs
# HTTPS to resolve the connector packages.
JAVA11_HOME="${JAVA11_HOME:-/usr/lib/jvm/temurin-11-jdk-amd64}"
CACERTS="${CACERTS:-/etc/ssl/certs/adoptium/cacerts}"

# TWO mounts, not one. spark.local.dir is the real nvme, passed through at the
# same path on both sides; the driver additionally needs a writable PARENT
# (/var/data/nvme4/spark) inside the container, which a small host scratch dir
# supplies. Container user is uid 1000.
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/var/data/nvme4/spark/local_storage}"
DRIVER_SCRATCH="${DRIVER_SCRATCH:-/home/iknaio/gs-docker-cache/driver-scratch}"
DRIVER_SCRATCH_MOUNT="${DRIVER_SCRATCH_MOUNT:-/var/data/nvme4/spark}"

# Persists the Ivy-resolved connector packages across runs: ~213MB otherwise
# re-fetched every attempt, and a network dependency on every one.
CACHE_DIR="${CACHE_DIR:-/home/iknaio/gs-docker-cache}"

# --network host does NOT inherit the host's /etc/hosts, so the names in
# spark.master and spark.eventLog.dir must be mapped explicitly.
# Verify with: getent hosts ikn-vie02-client01-be0
BE0_IP="${BE0_IP:-172.22.240.71}"

for path in "$JAVA11_HOME" "$CACERTS"; do
  [[ -e "$path" ]] || {
    echo "ERROR: $path not found (override JAVA11_HOME / CACERTS)" >&2
    exit 1
  }
done

# Must exist as a FILE before docker runs: a missing bind-mount source makes
# docker create it as an empty DIRECTORY on both sides, and the container then
# reports a confusing config error instead of a missing file.
if [[ ! -f "$GRAPHSENSE_CONFIG" ]]; then
  echo "ERROR: config file not found: $GRAPHSENSE_CONFIG" >&2
  [[ -d "$GRAPHSENSE_CONFIG" ]] && echo "NOTE: it is a DIRECTORY -- probably auto-created by an earlier docker run; rmdir it" >&2
  exit 1
fi

mkdir -p "$CACHE_DIR/graphsense" "$CACHE_DIR/ivy2" "$DRIVER_SCRATCH"

# The two argument arrays. They were used below before they were ever built,
# and bash expands an UNSET array to nothing under `set -u` rather than
# erroring -- so END_BLOCK was read here and silently dropped on the way to the
# CLI, and a "bounded" dry run scanned the whole chain.
BOUNDS=()
[[ -n "$START_BLOCK" ]] && BOUNDS+=(--start-block "$START_BLOCK")
[[ -n "$END_BLOCK" ]] && BOUNDS+=(--end-block "$END_BLOCK")

REPLICATION=(--replication-factor "$RF" --datacenter "$DATACENTER")
[[ "$RF" == "1" ]] && REPLICATION+=(--allow-single-replica)

# --network host: the container IS the Spark driver (client mode), so the
# workers must be able to route back to it.
v3() {
  docker run --rm --network host \
    --ulimit nofile=1048576:1048576 \
    --add-host "ikn-vie02-client01:$BE0_IP" \
    --add-host "ikn-vie02-client01-be0:$BE0_IP" \
    -e GRAPHSENSE_CONFIG_YAML=/graphsense.yaml \
    -e JAVA_HOME=/opt/java11 \
    -e JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStore=/opt/cacerts \
    "${ENVFILE_ARG[@]}" \
    -v "$JAVA11_HOME:/opt/java11:ro" \
    -v "$CACERTS:/opt/cacerts:ro" \
    -v "$GRAPHSENSE_CONFIG:/graphsense.yaml:ro" \
    -v "$CACHE_DIR/graphsense:/home/graphsense/.graphsense" \
    -v "$CACHE_DIR/ivy2:/home/graphsense/.ivy2" \
    -v "$DRIVER_SCRATCH:$DRIVER_SCRATCH_MOUNT" \
    -v "$SPARK_LOCAL_DIR:$SPARK_LOCAL_DIR" \
    "$IMAGE_REF" graphsense-v3 "$@"
}

case "${1:-}" in
  verify)
    # Cheapest failure first: Spark dies on this ~40s into startup, after Ivy
    # resolution and a JVM launch, with an AccessDeniedException that names a
    # blockmgr-* directory rather than the mount.
    echo ">>> driver scratch is writable from inside the container"
    docker run --rm \
      -v "$DRIVER_SCRATCH:$DRIVER_SCRATCH_MOUNT" \
      -v "$SPARK_LOCAL_DIR:$SPARK_LOCAL_DIR" "$IMAGE_REF" sh -c \
      "id && touch $SPARK_LOCAL_DIR/.probe $DRIVER_SCRATCH_MOUNT/.probe \
       && echo '  writable' && rm $SPARK_LOCAL_DIR/.probe $DRIVER_SCRATCH_MOUNT/.probe" \
      || {
        echo "  NOT writable by the container user (uid 1000). Fix with:" >&2
        echo "    sudo chmod a+rwxt $SPARK_LOCAL_DIR" >&2
        echo "    sudo chown -R 1000 $CACHE_DIR" >&2
        exit 2
      }
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
    # --writer too: without it `plan` prints the settings default and claims a
    # run would use the connector, while `run` below passes $WRITER (sidecar).
    # A plan that misreports the plan is worse than no plan.
    v3 plan -e "$ENV" -n "$NETWORK" --label "$LABEL" --spark-profile "$PROFILE" \
      --writer "$WRITER"
    ;;
  create)
    v3 create -e "$ENV" -n "$NETWORK" --label "$LABEL" "${REPLICATION[@]}"
    ;;
  schema)
    v3 schema -n "$NETWORK" --kind raw --label "$LABEL" "${REPLICATION[@]}"
    v3 schema -n "$NETWORK" --kind derived --label "$LABEL" "${REPLICATION[@]}"
    ;;
  probe)
    # Read-only; every statement is a SELECT. CASSANDRA_HOSTS bypasses
    # graphsense.yaml entirely, which is also how this runs off a laptop.
    HOSTS_ARG=()
    [[ -n "${CASSANDRA_HOSTS:-}" ]] && HOSTS_ARG=(--hosts "$CASSANDRA_HOSTS")
    v3 -v probe -e "$ENV" -n "$NETWORK" --label "$LABEL" "${HOSTS_ARG[@]}"
    ;;
  dry-run)
    if [[ -z "$END_BLOCK" && "${FULL:-0}" != "1" ]]; then
      echo "refusing an unbounded dry run: set END_BLOCK, or FULL=1 to mean it." >&2
      echo "A dry run samples every frame, and the window and group-by frames" >&2
      echo "cannot push the sample's LIMIT down -- so unbounded it computes the" >&2
      echo "entire transform to look at ten rows." >&2
      exit 2
    fi
    v3 -v run -e "$ENV" -n "$NETWORK" --label "$LABEL" \
      --spark-profile "$PROFILE" --writer "$WRITER" "${BOUNDS[@]}" \
      "${PREFLIGHT_ARG[@]}" --dry-run
    ;;
  run)
    if [[ -z "$END_BLOCK" && "${FULL:-0}" != "1" ]]; then
      echo "refusing an unbounded run: set END_BLOCK, or FULL=1 to mean it." >&2
      echo "A bounded slice exercises every seam in minutes; do that first." >&2
      exit 2
    fi
    v3 -v run -e "$ENV" -n "$NETWORK" --label "$LABEL" \
      --spark-profile "$PROFILE" --writer "$WRITER" "${BOUNDS[@]}" \
      "${PREFLIGHT_ARG[@]}" --yes
    ;;
  *)
    sed -n '2,50p' "$0"
    exit 1
    ;;
esac
