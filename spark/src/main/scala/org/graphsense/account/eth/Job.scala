package org.graphsense.account.eth

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, max}
import org.graphsense.Job
import org.graphsense.TransformHelpers
import org.graphsense.account.AccountSink
import org.graphsense.account.config.AccountConfig
import org.graphsense.account.models.{
  AddressIdByAddressPrefix,
  AddressIncomingRelationSecondaryIds,
  AddressOutgoingRelationSecondaryIds,
  AddressTransactionSecondaryIds,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}

class EthereumJob(
    spark: SparkSession,
    source: EthSource,
    sink: AccountSink,
    config: AccountConfig
) extends Job {
  import spark.implicits._

  private val transformation = new EthTransformation(
    spark,
    config.bucketSize(),
    config.blockBucketSizeAddressTxs(),
    config.addressrelationsIdsNbuckets()
  )

  override def run(from: Option[Int], to: Option[Int]): Unit = {
    println("Running ethereum specific transformations.")

    val exchangeRatesRaw = source.exchangeRates()
    val blocks = source.blocks()
    val transactions = source.transactions()
    val traces = source.traces()
    val tokenConfigurations = source.tokenConfigurations().persist()
    val tokenTransfers = source.tokenTransfers()

    println("Store configuration")
    val configuration =
      transformation.configuration(
        config.targetKeyspace(),
        config.bucketSize(),
        config.blockBucketSizeAddressTxs(),
        config.addressrelationsIdsNbuckets(),
        config.txPrefixLength(),
        config.addressPrefixLength(),
        TransformHelpers.getFiatCurrencies(exchangeRatesRaw)
      )

    sink.saveConfiguration(configuration)

    println("Store token configuration")
    sink.saveTokenConfiguration(tokenConfigurations)

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    sink.saveExchangeRates(exchangeRates)

    val maxBlockExchangeRates =
      exchangeRates.select(max(col("blockId"))).first.getInt(0)

    val maxBlockToProcess =
      Math.min(
        maxBlockExchangeRates,
        to.getOrElse(maxBlockExchangeRates)
      )

    val blocksFiltered =
      blocks.filter(col("blockId") <= maxBlockToProcess).persist()
    val transactionsFiltered =
      transactions.filter(col("blockId") <= maxBlockToProcess).persist()
    val tracesFiltered =
      traces.filter(col("blockId") <= maxBlockToProcess).persist()
    val tokenTransfersFiltered = tokenTransfers
      .filter(col("blockId") <= maxBlockToProcess)
      .persist()

    println("Computing token exchange rates")
    val tokenExchangeRates =
      transformation
        .computeTokenExchangeRates(
          blocksFiltered,
          source.tokenExchangeRates(),
          tokenTransfersFiltered,
          tokenConfigurations
        )
        .persist()

    sink.saveTokenExchangeRates(tokenExchangeRates)

    val maxBlock = blocksFiltered
      .select(
        max(col("blockId")).as("maxBlockId"),
        max(col("timestamp")).as("maxBlockTimestamp")
      )
      .withColumn("maxBlockDatetime", from_unixtime(col("maxBlockTimestamp")))
    val maxBlockTimestamp =
      maxBlock.select(col("maxBlockTimestamp")).first.getInt(0)
    val maxBlockDatetime =
      maxBlock.select(col("maxBlockDatetime")).first.getString(0)

    val noBlocks = maxBlockToProcess.toLong + 1
    val noTransactions = transactionsFiltered.count()

    println(s"Max block timestamp: ${maxBlockDatetime}")
    println(s"Max block ID: ${maxBlockToProcess}")
    println(s"Max transaction ID: ${noTransactions - 1}")

    println("Computing transaction IDs")
    spark.sparkContext.setJobDescription("Computing transaction IDs")
    val transactionIds =
      transformation.computeTransactionIds(transactionsFiltered).persist()
    val transactionIdsByTransactionIdGroup =
      transactionIds.toDF.transform(
        TransformHelpers.withSortedIdGroup[TransactionIdByTransactionIdGroup](
          "transactionId",
          "transactionIdGroup",
          config.bucketSize()
        )
      )

    sink.saveTransactionIdsByGroup(transactionIdsByTransactionIdGroup)

    val transactionIdsByTransactionPrefix =
      transactionIds.toDF.transform(
        TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
          "transaction",
          "transactionPrefix",
          config.txPrefixLength()
        )
      )
    sink.saveTransactionIdsByTxPrefix(transactionIdsByTransactionPrefix)

    println("Computing address IDs")
    spark.sparkContext.setJobDescription("Computing address IDs")
    val addressIds =
      transformation
        .computeAddressIds(tracesFiltered, tokenTransfersFiltered)
        .persist()
    val noAddresses = addressIds.count()
    val addressIdsByAddressPrefix =
      addressIds.toDF.transform(
        TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix",
          config.addressPrefixLength()
        )
      )

    sink.saveAddressIdsByPrefix(addressIdsByAddressPrefix)

    println("Computing contracts")
    spark.sparkContext.setJobDescription("Computing contracts")
    val contracts = transformation.computeContracts(tracesFiltered, addressIds)

    println("Computing balances")

    val balances = transformation
      .computeBalances(
        blocksFiltered,
        transactionsFiltered,
        tracesFiltered,
        addressIds,
        tokenTransfersFiltered,
        tokenConfigurations
      )
      .persist()
    sink.saveBalances(balances)
    println("Number of balances: " + balances.count())

    println("Encoding transactions")
    spark.sparkContext.setJobDescription("Encoding transactions")
    val encodedTransactions =
      transformation
        .computeEncodedTransactions(
          tracesFiltered,
          transactionIds,
          addressIds,
          exchangeRates
        )
        .persist()

    val encodedTokenTransfers = transformation
      .computeEncodedTokenTransfers(
        tokenTransfersFiltered,
        tokenConfigurations,
        transactionIds,
        addressIds,
        exchangeRates,
        tokenExchangeRates
      )
      .persist()

    println("Computing block transactions")
    spark.sparkContext.setJobDescription("Computing block transactions")
    val blockTransactions = transformation
      .computeBlockTransactions(encodedTransactions)

    sink.saveBlockTransactions(blockTransactions)

    println("Computing address transactions")
    spark.sparkContext.setJobDescription("Computing address transactions")
    val addressTransactions = transformation
      .computeAddressTransactions(encodedTransactions, encodedTokenTransfers)
      .persist()

    sink.saveAddressTransactions(addressTransactions)

    val addressTransactionsSecondaryIds =
      TransformHelpers
        .computeSecondaryPartitionIdLookup[AddressTransactionSecondaryIds](
          addressTransactions.toDF,
          "addressIdGroup",
          "addressIdSecondaryGroup"
        )

    sink.saveAddressTransactionBySecondaryId(addressTransactionsSecondaryIds)

    println("Computing address statistics")
    spark.sparkContext.setJobDescription("Computing address statistics")
    val addresses = transformation.computeAddresses(
      encodedTransactions,
      encodedTokenTransfers,
      addressTransactions,
      addressIds,
      contracts
    )

    sink.saveAddresses(addresses)

    println("Computing address relations")
    spark.sparkContext.setJobDescription("Computing address relations")
    val addressRelations =
      transformation
        .computeAddressRelations(
          encodedTransactions,
          encodedTokenTransfers
        )
        .persist()
    val noAddressRelations = addressRelations.count()

    sink.saveAddressIncomingRelations(
      addressRelations.sort("dstAddressIdGroup", "dstAddressIdSecondaryGroup")
    )
    sink.saveAddressOutgoingRelations(
      addressRelations.sort("srcAddressIdGroup", "srcAddressIdSecondaryGroup")
    )

    val addressIncomingRelationsSecondaryIds =
      TransformHelpers
        .computeSecondaryPartitionIdLookup[AddressIncomingRelationSecondaryIds](
          addressRelations.toDF,
          "dstAddressIdGroup",
          "dstAddressIdSecondaryGroup"
        )
    val addressOutgoingRelationsSecondaryIds =
      TransformHelpers
        .computeSecondaryPartitionIdLookup[AddressOutgoingRelationSecondaryIds](
          addressRelations.toDF,
          "srcAddressIdGroup",
          "srcAddressIdSecondaryGroup"
        )

    sink.saveAddressIncomingRelationsBySecondaryId(
      addressIncomingRelationsSecondaryIds
    )
    sink.saveAddressOutgoingRelationsBySecondaryId(
      addressOutgoingRelationsSecondaryIds
    )

    println("Computing summary statistics")
    spark.sparkContext.setJobDescription("Computing summary statistics")
    val summaryStatistics =
      transformation.summaryStatistics(
        maxBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations
      )
    summaryStatistics.show()

    sink.saveSummaryStatistics(summaryStatistics)

  }
}
