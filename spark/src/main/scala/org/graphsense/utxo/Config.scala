package org.graphsense.utxo.config

import org.graphsense.config.WriterConfig
import org.rogach.scallop._

class UtxoConf(arguments: Seq[String])
    extends ScallopConf(arguments)
    with WriterConfig {
  val network: ScallopOption[String] = opt[String](
    required = true,
    descr = "Cryptocurrency (e.g. BTC, BCH, LTC, ZEC)"
  )
  val rawKeyspace: ScallopOption[String] =
    opt[String](
      "raw-keyspace",
      required = true,
      noshort = true,
      descr = "Raw keyspace"
    )
  val targetKeyspace: ScallopOption[String] = opt[String](
    "target-keyspace",
    required = true,
    noshort = true,
    descr = "Transformed keyspace"
  )
  val bucketSize: ScallopOption[Int] = opt[Int](
    "bucket-size",
    required = false,
    default = Some(25000),
    noshort = true,
    descr = "Bucket size for Cassandra partitions"
  )
  val addressPrefixLength: ScallopOption[Int] = opt[Int](
    "address-prefix-length",
    required = false,
    default = Some(4),
    noshort = true,
    descr = "Prefix length of address hashes for Cassandra partitioning keys"
  )
  val coinjoinFilter: ScallopOption[Boolean] = toggle(
    "coinjoin-filtering",
    default = Some(true),
    noshort = true,
    prefix = "no-",
    descrYes = "Exclude coinJoin transactions from clustering",
    descrNo = "Include coinJoin transactions in clustering"
  )
  val bech32Prefix: ScallopOption[String] =
    opt[String](
      "bech32-prefix",
      default = Some(""),
      noshort = true,
      descr =
        "Bech32 address prefix (e.g. 'bc1' for Bitcoin or 'ltc1' for Litecoin)"
    )
  val checkpointDir: ScallopOption[String] = opt[String](
    "checkpoint-dir",
    default = Some("file:///tmp/spark-checkpoint"),
    noshort = true,
    descr = "Spark checkpoint directory (HFDS in non-local mode)"
  )
  val maxBlock: ScallopOption[Int] = opt[Int](
    "max-block",
    default = None,
    noshort = true,
    descr = "Max block that is used for the transformation job"
  )
  verify()
}
