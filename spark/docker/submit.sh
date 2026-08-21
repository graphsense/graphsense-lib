#!/bin/bash

FOO="${SPARK_MASTER:=local[*]}"
FOO="${SPARK_DRIVER_HOST:=localhost}"
FOO="${SPARK_DRIVER_PORT:=0}"
FOO="${SPARK_LOCAL_DIR:=./spark-data}"
FOO="${SPARK_UI_PORT:=4040}"
FOO="${SPARK_BLOCKMGR_PORT:=0}"
FOO="${SPARK_PARALLELISM:=16}"
FOO="${SPARK_EXECUTOR_MEMORY:=16g}"
FOO="${SPARK_DRIVER_MEMORY:=16g}"
FOO="${SPARK_CASSANDRA_OUTPUT_THROUGHPUT_MB_PER_SEC:=0.2}"
FOO="${SPARK_CASSANDRA_INPUT_THROUGHPUT_MB_PER_SEC:=0.2}"
FOO="${SPARK_CASSANDRA_CONNECTION_TIMEOUT_MS:=60000}"
FOO="${SPARK_CASSANDRA_READ_TIMEOUT_MS:=120000}"
FOO="${SPARK_CASSANDRA_QUERY_RETRY_COUNT:=10}"
FOO="${SPARK_CASSANDRA_RECONNECTION_DELAY_MAX_MS:=10000}"
FOO="${SPARK_CASSANDRA_OUTPUT_CONCURRENT_WRITES:=2}"

# FOO="${TRANSFORM_VERSION:=v1.5.1}"
FOO="${TRANSFORM_BUCKET_SIZE:=10000}"
FOO="${NETWORK:=ETH}"

FOO="${SPARK_PACKAGES:=com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.rogach:scallop_2.12:4.1.0,joda-time:joda-time:2.10.10,org.web3j:core:4.8.7,org.web3j:abi:4.8.7,graphframes:graphframes:0.8.3-spark3.5-s_2.12}"

FOO="${GS_SPARK_WRITER:=cassandra}"

# Write path for the transformed tables, forwarded to the job as CLI arguments.
# The cassandra-analytics bulk writer (GS_SPARK_WRITER=sidecar) also needs its
# Spark data source on the classpath; the default Cassandra connector does not.
# When GS_SPARK_WRITER=sidecar, GS_SPARK_SIDECAR_CONTACT_POINTS and
# GS_SPARK_SIDECAR_LOCAL_DC are required (GS_SPARK_SIDECAR_CONSISTENCY_LEVEL is
# optional, defaults to LOCAL_QUORUM).
GS_SPARK_ARGS="--writer $GS_SPARK_WRITER"
if [ "$GS_SPARK_WRITER" = "sidecar" ]; then
  SPARK_PACKAGES="$SPARK_PACKAGES,org.apache.cassandra:cassandra-analytics-core_spark3_2.12:0.3.0"
  GS_SPARK_ARGS="$GS_SPARK_ARGS --sidecar-contact-points $GS_SPARK_SIDECAR_CONTACT_POINTS"
  GS_SPARK_ARGS="$GS_SPARK_ARGS --sidecar-local-dc $GS_SPARK_SIDECAR_LOCAL_DC"
  GS_SPARK_ARGS="$GS_SPARK_ARGS --sidecar-consistency-level ${GS_SPARK_SIDECAR_CONSISTENCY_LEVEL:-LOCAL_QUORUM}"
fi

FOO="${CASSANDRA_HOST:=localhost}"

echo -en "Starting Spark job ...\n" \
         "Config:\n" \
         "- Spark master:        $SPARK_MASTER\n" \
         "- Spark driver:        $SPARK_DRIVER_HOST:$SPARK_DRIVER_PORT\n" \
         "- Spark local dir:     $SPARK_LOCAL_DIR\n" \
         "- Cassandra host:      $CASSANDRA_HOST\n" \
         "- Writer:              $GS_SPARK_WRITER\n" \
         "- Cassandra output MB/s: $SPARK_CASSANDRA_OUTPUT_THROUGHPUT_MB_PER_SEC (0 = no throttling)\n" \
         "- Cassandra input MB/s:  $SPARK_CASSANDRA_INPUT_THROUGHPUT_MB_PER_SEC (0 = no throttling)\n" \
         "- Cassandra conn timeout ms:  $SPARK_CASSANDRA_CONNECTION_TIMEOUT_MS\n" \
         "- Cassandra read timeout ms:  $SPARK_CASSANDRA_READ_TIMEOUT_MS\n" \
         "- Cassandra query retries:    $SPARK_CASSANDRA_QUERY_RETRY_COUNT\n" \
         "- Cassandra reconnect max ms: $SPARK_CASSANDRA_RECONNECTION_DELAY_MAX_MS\n" \
         "- Cassandra concurrent writes:$SPARK_CASSANDRA_OUTPUT_CONCURRENT_WRITES\n" \
         "- Executor memory:     $SPARK_EXECUTOR_MEMORY\n" \
         "- Spark parallelism:   $SPARK_PARALLELISM\n" \
         "- Transform Version:   $TRANSFORM_VERSION\n" \
         "Arguments:\n" \
         "- Raw keyspace:        $RAW_KEYSPACE\n" \
         "- Target keyspace:     $TGT_KEYSPACE\n" \
         "- Bucket Size:         $TRANSFORM_BUCKET_SIZE\n"

time "$SPARK_HOME"/bin/spark-submit \
  --class "org.graphsense.TransformationJob" \
  --master "$SPARK_MASTER" \
  --conf spark.driver.bindAddress="0.0.0.0" \
  --conf spark.driver.host="$SPARK_DRIVER_HOST" \
  --conf spark.driver.port="$SPARK_DRIVER_PORT" \
  --conf spark.ui.port="$SPARK_UI_PORT" \
  --conf spark.blockManager.port="$SPARK_BLOCKMGR_PORT" \
  --conf spark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.cassandra.output.throughputMBPerSec="$SPARK_CASSANDRA_OUTPUT_THROUGHPUT_MB_PER_SEC" \
  --conf spark.cassandra.input.throughputMBPerSec="$SPARK_CASSANDRA_INPUT_THROUGHPUT_MB_PER_SEC" \
  --conf spark.cassandra.connection.timeoutMS="$SPARK_CASSANDRA_CONNECTION_TIMEOUT_MS" \
  --conf spark.cassandra.read.timeoutMS="$SPARK_CASSANDRA_READ_TIMEOUT_MS" \
  --conf spark.cassandra.query.retry.count="$SPARK_CASSANDRA_QUERY_RETRY_COUNT" \
  --conf spark.cassandra.connection.reconnectionDelayMS.max="$SPARK_CASSANDRA_RECONNECTION_DELAY_MAX_MS" \
  --conf spark.cassandra.output.concurrent.writes="$SPARK_CASSANDRA_OUTPUT_CONCURRENT_WRITES" \
  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
  --conf spark.local.dir="$SPARK_LOCAL_DIR" \
  --conf spark.default.parallelism=$SPARK_PARALLELISM \
  --conf spark.driver.memory=$SPARK_DRIVER_MEMORY \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
  --conf spark.kryo.referenceTracking=false \
  --conf "spark.executor.extraJavaOptions=-XX:+UnlockExperimentalVMOptions -XX:hashCode=0" \
  --conf "spark.driver.extraJavaOptions=-XX:+UnlockExperimentalVMOptions -XX:hashCode=0" \
  --packages $SPARK_PACKAGES \
  graphsense-spark.jar \
  --network "$NETWORK" \
  --raw-keyspace "$RAW_KEYSPACE" \
  --target-keyspace "$TGT_KEYSPACE" \
  $GS_SPARK_ARGS \
  # --gs-cache-dir file:///tmp/spark/ \
  # --bucket-size $TRANSFORM_BUCKET_SIZE \

exit $?
