# Pre-baked Cassandra image with GraphSense test schemas
# Build: docker build -t graphsense/cassandra-test:4.1.4 tests/cassandra/
FROM cassandra:4.1.4

# Copy schema initialization script
COPY init-schemas.py /init-schemas.py

# Install Python dependencies for schema init
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    pip3 install cassandra-driver requests && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure for fast single-node startup BEFORE first start
# This is critical - num_tokens is locked after first start
RUN sed -i 's/num_tokens: 16/num_tokens: 1/' /etc/cassandra/cassandra.yaml

# Set JVM options for fast startup
ENV JVM_EXTRA_OPTS="-Dcassandra.ring_delay_ms=100 -Dcassandra.skip_wait_for_gossip_to_settle=0"

RUN set -e && \
    echo "Starting Cassandra for schema initialization..." && \
    cassandra -R && \
    echo "Waiting for Cassandra to be ready..." && \
    until cqlsh -e "SELECT now() FROM system.local" 2>/dev/null; do sleep 1; done && \
    echo "Cassandra is ready, creating schemas..." && \
    python3 /init-schemas.py && \
    echo "Schemas created, stopping Cassandra..." && \
    nodetool stopdaemon && \
    sleep 2 && \
    echo "Schema initialization complete."

# Clean up init script (no longer needed)
RUN rm /init-schemas.py
