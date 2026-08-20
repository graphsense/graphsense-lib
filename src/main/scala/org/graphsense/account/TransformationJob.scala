package org.graphsense.account

import org.apache.spark.sql.SparkSession
import org.graphsense.Job
import org.graphsense.account.config.AccountConfig
import org.graphsense.account.eth.{CassandraEthSource, EthereumJob}
import org.graphsense.account.trx.{CassandraTrxSource, TronJob}
import org.graphsense.storage.{CassandraStorage, SidecarBulkWriter}

object TransformationJob {

  def main(args: Array[String]): Unit = {
    // import spark.implicits._

    val conf = new AccountConfig(args)

    val spark = SparkSession.builder
      .config(SidecarBulkWriter.sparkConf(conf.writer()))
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())
    println("Address prefix length:         " + conf.addressPrefixLength())
    println("Tx prefix length:              " + conf.txPrefixLength())
    println(
      "Min block:                     " + conf.minBlock.toOption.getOrElse(-1)
    )
    println(
      "Max block:                     " + conf.maxBlock.toOption.getOrElse(-1)
    )
    println(
      "Cache dataset dir:             " + conf.cacheDirectory.toOption
        .getOrElse("not set")
    )
    println(
      "Debug level:                   " + conf.debug.toOption
        .getOrElse(0)
    )

    println("Writer:                        " + conf.writer())

    val cassandra = new CassandraStorage(
      spark,
      SidecarBulkWriter.forWriter(
        conf.writer(),
        conf.sidecarContactPoints.toOption,
        conf.sidecarLocalDc.toOption,
        conf.sidecarConsistencyLevel()
      )
    )

    val transform: Job = conf.network() match {
      case "eth" =>
        new EthereumJob(
          spark,
          new CassandraEthSource(
            cassandra,
            conf.rawKeyspace()
          ),
          new CassandraAccountSink(cassandra, conf.targetKeyspace()),
          conf
        )
      case "trx" =>
        new TronJob(
          spark,
          new CassandraTrxSource(cassandra, conf.rawKeyspace()),
          new CassandraAccountSink(cassandra, conf.targetKeyspace()),
          conf
        )
      case _ =>
        throw new IllegalArgumentException(
          "Transformation for given network not found."
        )
    }

    transform.run(conf.minBlock.toOption, conf.maxBlock.toOption)

    // The transform is complete and every write is durable at this point.
    // SparkContext.stop() can still trip an RPC teardown race
    // (TransportResponseHandler "requests outstanding" + a
    // RejectedExecutionException from the already-shutting-down dispatcher
    // pool; observed on Spark 3.5 after a multi-hour run) which would turn
    // a fully successful run into a non-zero exit code. Teardown is
    // best-effort; the exit code reports the transform's outcome.
    println("Transformation finished successfully.")
    try {
      spark.stop()
    } catch {
      case e: Throwable =>
        println("Warn - ignoring exception during Spark shutdown: " + e)
    }
    sys.exit(0)
  }
}
