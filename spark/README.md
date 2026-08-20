[![sbt test](https://github.com/graphsense/graphsense-spark/actions/workflows/sbt_test.yml/badge.svg)](https://github.com/graphsense/graphsense-spark/actions/workflows/sbt_test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# GraphSense Spark-Transformation Pipeline

The GraphSense Transformation Pipeline reads raw block and transaction data,
which is ingested into [Apache Cassandra][apache-cassandra]
by the [graphsense-lib][graphsense-lib] component.
The transformation pipeline computes an address graph and de-normalized views
using [Apache Spark][apache-spark], which are again stored in Cassandra to provide efficient queries.

The views computed by this component are subsequently served by the
[GraphSense REST][graphsense-rest] interface, which is used main data-source
[graphsense-dashboard][graphsense-dashboard], the main graphical user-interface of the GraphSense stack.

This component is implemented in [Scala][scala-lang] using
[Apache Spark][apache-spark].

## Local Development Environment Setup

### Prerequisites

Make sure [Java 11][java] and [sbt >= 1.0][scala-sbt] is installed:

    java -version
    sbt about

**Java 17 Support:** Java 17 is supported but not actively tested. When using Java 17, the following JVM flags must be set:

```
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-exports=java.base/sun.misc=ALL-UNNAMED
```

These flags are already configured in `build.sbt` for local development.

Download, install, and run [Apache Spark][apache-spark] (version >= 3.5.1)
in `$SPARK_HOME`:

    $SPARK_HOME/sbin/start-master.sh

Download, install, and run [Apache Cassandra][apache-cassandra]
(version >= 3.11) in `$CASSANDRA_HOME`

    $CASSANDRA_HOME/bin/cassandra -f

For testing purposes it is easiest to run a dockerized instance of cassandra

    docker run --rm --name cassandra_dev -p 9042:9042 -d cassandra:4.0

### Ingest Raw Block Data

Use graphsense-lib to ingest data into the raw keyspace (tables). Before you can run the the data import please create a valid ```.graphsense.yaml``` config file in your home directory. For more details see [graphsense-lib][graphsense-lib].

    graphsense-cli -v ingest from-node -e dev -c {NETWORK} --batch-size 10  --end-block 1000 --version 2 --create-schema
    graphsense-cli -v exchange-rates coinmarketcap ingest -e dev -c {NETWORK}

*Note:* replace {NETWORK} by the three letter code of the currency you want to import (e.g eth, btc, zec, etc.)

This should create a keyspace `{NETWORK}_raw` (tables `exchange_rates`,
`transaction`, `block`, etc.).
Check as follows

    cqlsh localhost
    cqlsh> USE eth_raw;
    cqlsh:btc_raw> DESCRIBE tables;

## Execute Transformation on localhost

Create the target keyspace for transformed data

    graphsense-cli -v schema create-new-transformed -e dev -c {NETWORK} --no-date --suffix dev

Compile, test the implementation

    make test && make build

Run the dockerized pipeline on localhost

   make build-docker && make run-docker-{NETWORK}-transform-local

Check the running job using the local Spark UI at http://localhost:4040/jobs. 

Detailed information about how to submit a job and how to install the necessary infrastructure we point you to the Dockerfile and the submit script in ```docker/submit.sh```.

## Writing transformed data to Cassandra

The transformation job can write its output tables to Cassandra in two ways,
selected with the `--writer` job argument:

- `cassandra` (default) — the [DataStax Spark Cassandra connector][spark-cassandra-connector].
  Tables are written through the regular CQL write path (coordinator, commitlog,
  memtables).
- `sidecar` — bulk loading via [cassandra-analytics][cassandra-analytics]. Each
  Spark task generates SSTables and streams them into Cassandra through the
  [Cassandra Sidecar][cassandra-sidecar], bypassing the CQL write path. This
  avoids the coordinator and commitlog and causes far less compaction and GC
  pressure, which makes it the better option for large loads.

The `sidecar` writer requires the Cassandra Sidecar to be running on the cluster
nodes and takes these additional arguments:

    --writer sidecar
    --sidecar-contact-points host1,host2,host3   # Sidecar hosts (seed list)
    --sidecar-local-dc <datacenter>
    --sidecar-consistency-level LOCAL_QUORUM     # optional, default LOCAL_QUORUM

When run through `docker/submit.sh`, these are taken from the `GS_SPARK_WRITER`,
`GS_SPARK_SIDECAR_CONTACT_POINTS`, `GS_SPARK_SIDECAR_LOCAL_DC` and
`GS_SPARK_SIDECAR_CONSISTENCY_LEVEL` environment variables (e.g. via
`docker run -e`) and forwarded as the arguments above; `submit.sh` also adds the
`cassandra-analytics` Spark package automatically. The default `cassandra` path
is unaffected.

## Contributions

Community contributions e.g. new features and bug fixes are very welcome. For both please create a pull request with the proposed changes. We will review as soon as possible. To avoid frustration and wasted work please contact us to discuss changes before you implement them. This is best done via an issue or our [discussion board](https://github.com/orgs/graphsense/discussions/).

Please make sure that the submitted code is always tested and properly formatted. 

Do not forget to format and test your code using 
```
make format && make test
```
before committing.

[graphsense-lib]: https://github.com/graphsense/graphsense-lib
[graphsense-dashboard]: https://github.com/graphsense/graphsense-dashboard
[graphsense-rest]: https://github.com/graphsense/graphsense-rest
[java]: https://adoptopenjdk.net
[scala-lang]: https://www.scala-lang.org
[scala-sbt]: http://www.scala-sbt.org
[dsbulk]: https://github.com/datastax/dsbulk
[apache-spark]: https://spark.apache.org/downloads.html
[apache-cassandra]: http://cassandra.apache.org
[spark-cassandra-connector]: https://github.com/datastax/spark-cassandra-connector
[cassandra-analytics]: https://github.com/apache/cassandra-analytics
[cassandra-sidecar]: https://github.com/apache/cassandra-sidecar


