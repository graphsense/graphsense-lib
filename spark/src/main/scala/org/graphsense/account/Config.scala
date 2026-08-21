package org.graphsense.account.config

import org.graphsense.config.WriterConfig
import org.rogach.scallop._

class AccountConfig(arguments: Seq[String])
    extends ScallopConf(arguments)
    with WriterConfig {
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
    default = Some(5),
    noshort = true,
    descr = "Prefix length of address hashes for Cassandra partitioning keys"
  )
  val txPrefixLength: ScallopOption[Int] = opt[Int](
    "tx-prefix-length",
    required = false,
    default = Some(5),
    noshort = true,
    descr = "Prefix length for tx hashes Cassandra partitioning keys"
  )
  val network: ScallopOption[String] = opt[String](
    "network",
    required = true,
    noshort = true,
    descr =
      "Select which network we are processing (supported at the moment are, eth, trx)"
  )
  val minBlock: ScallopOption[Int] = opt[Int](
    "min-block",
    required = false,
    default = None,
    noshort = true,
    descr = "Earliest block to process."
  )
  val maxBlock: ScallopOption[Int] = opt[Int](
    "max-block",
    required = false,
    default = None,
    noshort = true,
    descr = "Limit the max block to process"
  )
  val cacheDirectory: ScallopOption[String] = opt[String](
    "gs-cache-dir",
    required = false,
    default = None,
    noshort = true,
    descr = "Directory to cache datasets"
  )
  val debug: ScallopOption[Int] = opt[Int](
    "debug",
    required = false,
    default = Some(0),
    noshort = true,
    descr = "Debug level 0, no debug level. 1 and above produce debug output."
  )
  val forceOverwrite: ScallopOption[Boolean] = opt[Boolean](
    "force-overwrite",
    required = false,
    default = Some(false),
    noshort = true,
    descr = "Ignore if table is not empty and overwrite the data."
  )
  val blockBucketSizeAddressTxs: ScallopOption[Int] = opt[Int](
    "block-bucket-size-address-txs",
    required = false,
    default = Some(150000),
    noshort = true,
    descr =
      "Bucket size for Cassandra partitions of address transactions base on blockid"
  )
  val addressrelationsIdsNbuckets: ScallopOption[Int] = opt[Int](
    "addressrelations-ids-nbuckets",
    required = false,
    default = Some(100),
    noshort = true,
    descr =
      "Number of buckets for Cassandra partitions of address relations based on address id"
  )
  verify()
}
