package org.graphsense.config

import org.rogach.scallop.{ScallopConf, ScallopOption}

/** CLI options, shared by the account and UTXO transformation jobs, that select
  * and configure how transformed tables are written to Cassandra.
  */
trait WriterConfig { self: ScallopConf =>

  val writer: ScallopOption[String] = opt[String](
    "writer",
    required = false,
    default = Some("cassandra"),
    noshort = true,
    descr = "Write path for transformed tables: 'cassandra' (Spark Cassandra " +
      "connector, default) or 'sidecar' (cassandra-analytics SSTable bulk " +
      "loader via the Cassandra Sidecar)"
  )

  val sidecarContactPoints: ScallopOption[String] = opt[String](
    "sidecar-contact-points",
    required = false,
    default = None,
    noshort = true,
    descr =
      "Comma-separated Cassandra Sidecar hosts; required when --writer=sidecar"
  )

  val sidecarLocalDc: ScallopOption[String] = opt[String](
    "sidecar-local-dc",
    required = false,
    default = None,
    noshort = true,
    descr = "Cassandra local datacenter; required when --writer=sidecar"
  )

  val sidecarConsistencyLevel: ScallopOption[String] = opt[String](
    "sidecar-consistency-level",
    required = false,
    default = Some("LOCAL_QUORUM"),
    noshort = true,
    descr = "Consistency level for the sidecar bulk write"
  )
}
