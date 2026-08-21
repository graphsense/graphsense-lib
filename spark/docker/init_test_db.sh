#!/bin/sh

echo "Creating Cassandra keyspace ${RAW_KEYSPACE} on ${CASSANDRA_HOST} "
python3 /opt/graphsense/scripts/create_keyspace.py \
    -d "${CASSANDRA_HOST}" \
    -k "${RAW_KEYSPACE}" \
    -s /opt/graphsense/scripts/schema_raw.cql

echo "Creating Cassandra keyspace ${TGT_KEYSPACE} on ${CASSANDRA_HOST} " && \
python3 /opt/graphsense/scripts/create_keyspace.py \
    -d "${CASSANDRA_HOST}" \
    -k "${TGT_KEYSPACE}" \
    -s /opt/graphsense/scripts/schema_transformed.cql



exec "$@"
