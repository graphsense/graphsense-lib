package org.graphsense.utxo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, max}
import org.graphsense.TransformHelpers
import org.graphsense.models._
import org.graphsense.storage._
import org.graphsense.utxo.{Fields => F}
import org.graphsense.utxo.config.UtxoConf
import org.graphsense.utxo.models._

object TransformationJob {

  def main(args: Array[String]): Unit = {

    val conf = new UtxoConf(args)

    val spark = SparkSession.builder
      .config(SidecarBulkWriter.sparkConf(conf.writer()))
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(conf.checkpointDir())

    println("Currency:                      " + conf.network())
    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())
    println("Address prefix length:         " + conf.addressPrefixLength())
    println("CoinJoin Filtering enabled:    " + conf.coinjoinFilter())
    if (conf.bech32Prefix().length > 0) {
      println("Bech32 address prefix:         " + conf.bech32Prefix())
    }
    println("Spark checkpoint directory:    " + conf.checkpointDir())

    import spark.implicits._

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
    val transformation =
      new Transformation(spark, conf.bucketSize(), conf.addressPrefixLength())

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block").persist()
    val transactions =
      transformation.addCoinbaseAddress(
        cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
      )

    println("Store configuration")
    val configuration =
      transformation.configuration(
        conf.targetKeyspace(),
        conf.bucketSize(),
        conf.addressPrefixLength(),
        conf.bech32Prefix(),
        conf.coinjoinFilter(),
        TransformHelpers.getFiatCurrencies(exchangeRatesRaw)
      )
    cassandra.store(
      conf.targetKeyspace(),
      "configuration",
      configuration
    )
    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()
    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates)

    val maxBlockExchangeRates =
      exchangeRates.select(max(col(F.blockId))).first.getInt(0)

    val maxBlockToProcess =
      Math.min(
        maxBlockExchangeRates,
        conf.maxBlock.toOption.getOrElse(maxBlockExchangeRates)
      )

    val transactionsFiltered =
      transactions.filter(col(F.blockId) <= maxBlockToProcess).persist()

    val maxBlock = blocks
      .filter(col(F.blockId) <= maxBlockToProcess)
      .select(
        max(col(F.blockId)).as("maxBlockId"),
        max(col(F.timestamp)).as("maxBlockTimestamp")
      )
      .withColumn("maxBlockDatetime", from_unixtime(col("maxBlockTimestamp")))
    val maxBlockTimestamp =
      maxBlock.select(col("maxBlockTimestamp")).first.getInt(0)
    val maxBlockDatetime =
      maxBlock.select(col("maxBlockDatetime")).first.getString(0)
    val maxTransactionId =
      transactionsFiltered.select(max(F.txId)).first.getLong(0)
    val noBlocks = maxBlockToProcess + 1
    val noTransactions = maxTransactionId + 1

    println(s"Max block timestamp: ${maxBlockDatetime}")
    println(s"Max block ID: ${maxBlockToProcess}")
    println(s"Max transaction ID: ${maxTransactionId}")

    println("Extracting transaction inputs")
    val regInputs =
      transformation.computeRegularInputs(transactionsFiltered).persist()

    println("Extracting transaction outputs")
    val regOutputs =
      transformation.computeRegularOutputs(transactionsFiltered).persist()

    println("Computing address IDs")
    val addressIds =
      transformation.computeAddressIds(regOutputs).persist()

    val addressByAddressPrefix = transformation.computeAddressByAddressPrefix(
      addressIds,
      bech32Prefix = conf.bech32Prefix()
    )
    cassandra.store(
      conf.targetKeyspace(),
      "address_ids_by_address_prefix",
      addressByAddressPrefix
    )

    println("Computing address transactions")
    val addressTransactions =
      transformation
        .computeAddressTransactions(
          regInputs,
          regOutputs,
          addressIds
        )
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "address_transactions",
      addressTransactions
    )

    val (inputs, outputs) =
      transformation.splitTransactions(addressTransactions)
    inputs.persist()
    outputs.persist()

    println("Computing address statistics")
    val basicAddresses =
      transformation
        .computeBasicAddresses(
          addressTransactions,
          inputs,
          outputs,
          exchangeRates
        )
        .persist()

    println("Computing plain address relations")
    val plainAddressRelations =
      transformation
        .computePlainAddressRelations(
          inputs,
          outputs,
          regInputs,
          transactionsFiltered
        )

    println("Computing address relations")
    val addressRelations =
      transformation
        .computeAddressRelations(
          plainAddressRelations,
          exchangeRates
        )
        .persist()
    val noAddressRelations = addressRelations.count()
    cassandra.store(
      conf.targetKeyspace(),
      "address_incoming_relations",
      addressRelations.sort(F.dstAddressIdGroup, F.dstAddressId)
    )
    cassandra.store(
      conf.targetKeyspace(),
      "address_outgoing_relations",
      addressRelations.sort(F.srcAddressIdGroup, F.srcAddressId)
    )

    spark.sparkContext.setJobDescription("Perform clustering")
    println("Computing address clusters")
    val addressCluster = transformation
      .computeAddressCluster(regInputs, addressIds, conf.coinjoinFilter())
      .persist()

    println("Computing addresses")
    val addresses =
      transformation.computeAddresses(
        basicAddresses,
        addressCluster,
        addressRelations,
        addressIds
      )
    val noAddresses = addresses.count()
    cassandra.store(conf.targetKeyspace(), "address", addresses)

    println("Computing cluster addresses")
    val clusterAddresses =
      transformation
        .computeClusterAddresses(addressCluster)
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_addresses",
      clusterAddresses
    )

    println("Computing cluster transactions")
    val clusterTransactions =
      transformation
        .computeClusterTransactions(
          inputs,
          outputs,
          transactionsFiltered,
          addressCluster
        )
        .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_transactions",
      clusterTransactions
    )

    val (clusterInputs, clusterOutputs) =
      transformation.splitTransactions(clusterTransactions)
    clusterInputs.persist()
    clusterOutputs.persist()

    println("Computing cluster statistics")
    val basicCluster =
      transformation
        .computeBasicCluster(
          clusterAddresses,
          clusterTransactions,
          clusterInputs,
          clusterOutputs,
          exchangeRates
        )
        .persist()
    val noCluster = basicCluster.count()

    println("Computing plain cluster relations")
    val plainClusterRelations =
      transformation
        .computePlainClusterRelations(plainAddressRelations, addressCluster)

    println("Computing cluster relations")
    val clusterRelations =
      transformation
        .computeClusterRelations(
          plainClusterRelations,
          exchangeRates
        )
        .persist()
    val noClusterRelations = clusterRelations.count()
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_incoming_relations",
      clusterRelations
        .drop(F.estimatedValueAdj)
        .as[ClusterRelation]
        .sort(
          F.dstClusterIdGroup,
          F.dstClusterId,
          F.srcClusterId
        )
    )
    cassandra.store(
      conf.targetKeyspace(),
      "cluster_outgoing_relations",
      clusterRelations
        .drop(F.estimatedValueAdj)
        .as[ClusterRelation]
        .sort(
          F.srcClusterIdGroup,
          F.srcClusterId,
          F.dstClusterId
        )
    )

    println("Computing cluster")
    val cluster =
      transformation
        .computeCluster(basicCluster, clusterRelations)
    cassandra.store(conf.targetKeyspace(), "cluster", cluster)

    println("Computing summary statistics")
    val summaryStatistics =
      transformation.summaryStatistics(
        maxBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noCluster,
        noClusterRelations
      )
    summaryStatistics.show()
    cassandra.store(
      conf.targetKeyspace(),
      "summary_statistics",
      summaryStatistics
    )

    // See the account job: shutdown is best-effort, a teardown race must not
    // turn a completed transform into a non-zero exit code.
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
