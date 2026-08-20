package org.graphsense.account.trx

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.{col, from_unixtime, max, min}
import org.graphsense.Job
import org.graphsense.account.AccountSink
import org.graphsense.account.config.AccountConfig
import org.graphsense.account.models._
import org.graphsense.TransformHelpers
import org.graphsense.models.ExchangeRates
import org.graphsense.account.trx.models.{Trace, TxFee}
import org.graphsense.account.models.TokenConfiguration
import org.graphsense.Util._
import org.graphsense.account.models.AddressTransactionSecondaryIds

/** The block range a transform run processes, pinned to the gs-cache.
  *
  * The cutoff (maxBlock) is derived from the exchange rates, which advance with
  * every daily rate ingest. A rerun that reuses cached datasets (computeCached)
  * MUST use the same cutoff as the run that created them: otherwise everything
  * computed live from raw (filtered counts, summary_statistics, max block
  * timestamp) silently runs ahead of the cached table content — the stats row
  * then claims a horizon the data does not have, and a delta updater onboarded
  * from it skips the difference. Caching the resolved range pins every retry to
  * the first run's cutoff; deleting the cache directory resets it.
  */
final case class BlockRangeToProcess(minBlock: Int, maxBlock: Int)

/** The job arguments the cached datasets were built with, pinned to the
  * gs-cache alongside them.
  *
  * computeCached reuses whatever parquet it finds — it cannot tell that a
  * cached dataset was produced under different arguments than the current
  * run's. That bit hard on the 2026-07/08 TRX full transform:
  * `--block-bucket-size-address-txs` was changed 450000 → 50000 between runs,
  * the addressTransactions cache survived, and three reruns wrote
  * 450000-bucketed rows into a keyspace whose `configuration` row said 50000 —
  * breaking every secondary-bucket read. Pinning the arguments and refusing to
  * run on a mismatch turns that silent corruption into an immediate, explicit
  * failure.
  */
final case class CachePinnedConfig(
    bucketSize: Int,
    blockBucketSizeAddressTxs: Int,
    addressrelationsIdsNbuckets: Int,
    txPrefixLength: Int,
    addressPrefixLength: Int
)

class TronJob(
    spark: SparkSession,
    source: TrxSource,
    sink: AccountSink,
    config: AccountConfig
) extends Job {
  import spark.implicits._

  private val transformation = new TrxTransformation(
    spark,
    config.bucketSize(),
    config.blockBucketSizeAddressTxs(),
    config.addressrelationsIdsNbuckets()
  )

  private val debug = config.debug()

  val base_path =
    config.cacheDirectory.toOption.map(dir => dir + config.targetKeyspace())

  def computeCached[
      R: Encoder
  ](
      dataset_name: String
  )(block: => Dataset[R]): Dataset[R] = {
    TransformHelpers.computeCached(
      base_path,
      spark,
      config.forceOverwrite.toOption.getOrElse(false)
    )(dataset_name)(block)
  }

  def timeJob[R](title: String)(block: => R): R = {
    spark.sparkContext.setJobDescription(title)
    time(title)(block)
  }

  def prepareAndLoad(): (
      Dataset[ExchangeRates],
      Dataset[Block],
      Dataset[Transaction],
      Dataset[TxFee],
      Dataset[Trace],
      Dataset[TokenTransfer],
      Dataset[TokenConfiguration],
      Long,
      Long,
      Int,
      Long
  ) = {
    // Refuse to reuse caches built under different job arguments; see
    // CachePinnedConfig. Runs before anything is written to the keyspace.
    val currentConfig = CachePinnedConfig(
      config.bucketSize(),
      config.blockBucketSizeAddressTxs(),
      config.addressrelationsIdsNbuckets(),
      config.txPrefixLength(),
      config.addressPrefixLength()
    )
    val pinnedConfig = computeCached("cachePinnedConfig") {
      Seq(currentConfig).toDS()
    }.first()
    if (pinnedConfig != currentConfig) {
      throw new IllegalStateException(
        f"The gs-cache at ${base_path.getOrElse("?")} was built with " +
          f"different job arguments (cached: ${pinnedConfig}, current: " +
          f"${currentConfig}). Reusing it would write data inconsistent " +
          "with the keyspace configuration. Either restore the arguments " +
          "or delete the cache directory to recompute."
      )
    }

    val exchangeRatesRaw = source.exchangeRates()
    val blocks = source.blocks()
    val tokenConfigurations = source.tokenConfigurations().persist()

    timeJob("Store configuration") {
      val conf = transformation.configuration(
        config.targetKeyspace(),
        config.bucketSize(),
        config.blockBucketSizeAddressTxs(),
        config.addressrelationsIdsNbuckets(),
        config.txPrefixLength(),
        config.addressPrefixLength(),
        TransformHelpers.getFiatCurrencies(exchangeRatesRaw)
      )

      sink.saveConfiguration(conf)
    }

    timeJob("Store token configuration") {
      sink.saveTokenConfiguration(tokenConfigurations)
    }

    val exchangeRates = timeJob("Computing exchange rates") {
      val rates =
        transformation
          .computeExchangeRates(blocks, exchangeRatesRaw)
          .persist()

      sink.saveExchangeRates(rates)
      rates
    }

    // Resolved once per cache lifetime; see BlockRangeToProcess. On a warm
    // cache the values load from parquet and the exchange-rate horizon of
    // THIS run is irrelevant — retries stay consistent with the cached
    // datasets downstream.
    val blockRange = computeCached("blockRangeToProcess") {
      val minBlock = config.minBlock.toOption match {
        case None => exchangeRates.select(min(col("blockId"))).first.getInt(0)
        case Some(user_min_block) =>
          scala.math.max(
            exchangeRates.select(min(col("blockId"))).first.getInt(0),
            user_min_block
          )
      }
      val maxBlock = config.maxBlock.toOption match {
        case None => exchangeRates.select(max(col("blockId"))).first.getInt(0)
        case Some(user_max_block) =>
          scala.math.min(
            exchangeRates.select(max(col("blockId"))).first.getInt(0),
            user_max_block
          )
      }
      Seq(BlockRangeToProcess(minBlock, maxBlock)).toDS()
    }.first()

    val minBlockToProcess = blockRange.minBlock
    val maxBlockToProcess = blockRange.maxBlock

    def warnRangeOverride(flag: String, requested: Int): Unit = println(
      f"Warn - ${flag} ${requested} is not the effective value: the run " +
        f"processes [${minBlockToProcess}, ${maxBlockToProcess}] (clamped " +
        "to the exchange-rate horizon on first resolution, then pinned by " +
        "the cached blockRangeToProcess dataset; delete the cache " +
        "directory to re-resolve)."
    )
    config.minBlock.toOption
      .filter(_ != minBlockToProcess)
      .foreach(warnRangeOverride("--min-block", _))
    config.maxBlock.toOption
      .filter(_ != maxBlockToProcess)
      .foreach(warnRangeOverride("--max-block", _))

    val (
      blks,
      txs,
      traces,
      tokenTxs,
      noBlocks,
      noTransactions,
      maxBlockTimestamp,
      maxTxsPerBlock
    ) = timeJob(
      s"Filter source data from ${minBlockToProcess} to ${maxBlockToProcess}"
    ) {

      val blocksFiltered =
        blocks
          .transform(
            TransformHelpers.filterBlockRange(
              Some(minBlockToProcess),
              Some(maxBlockToProcess)
            )
          )
          .persist()

      val txsFiltered = source
        .transactions()
        .transform(
          TransformHelpers.filterBlockRange(
            Some(minBlockToProcess),
            Some(maxBlockToProcess)
          )
        )
        .transform(transformation.onlySuccessfulTxs)
        .persist()

      val minBlock = blocksFiltered
        .select(
          min(col("blockId")).as("minBlockId"),
          min(col("timestamp")).as("minBlockTimestamp")
        )
        .withColumn("minBlockDatetime", from_unixtime(col("minBlockTimestamp")))
      minBlock.select(col("minBlockTimestamp")).first.getInt(0)
      val minBlockDatetime =
        minBlock.select(col("minBlockDatetime")).first.getString(0)

      val maxBlock = blocksFiltered
        .select(
          max(col("blockId")).as("maxBlockId"),
          max(col("timestamp")).as("maxBlockTimestamp"),
          max(col("transactionCount")).as("maxTxsPerBlock")
        )
        .withColumn("maxBlockDatetime", from_unixtime(col("maxBlockTimestamp")))
      val maxBlockTimestamp =
        maxBlock.select(col("maxBlockTimestamp")).first.getInt(0)
      val maxBlockDatetime =
        maxBlock.select(col("maxBlockDatetime")).first.getString(0)
      val maxTxsPerBlock =
        maxBlock.select(col("maxTxsPerBlock")).first.get(0).toString.toLong

      val noBlocks = blocksFiltered.count()
      val noTransactions = txsFiltered.count()

      printStat("Min block timestamp", minBlockDatetime)
      printStat("Min block ID", minBlockToProcess)
      printStat("Max block timestamp", maxBlockDatetime)
      printStat("Max block ID", maxBlockToProcess)
      printStat("Transaction count", noTransactions)
      (
        blocksFiltered,
        txsFiltered,
        source
          .traces()
          .transform(
            TransformHelpers.filterBlockRange(
              Some(minBlockToProcess),
              Some(maxBlockToProcess)
            )
          )
          .transform(transformation.onlySuccessfulTrace)
          .persist(),
        source
          .tokenTransfers()
          .transform(
            TransformHelpers.filterBlockRange(
              Some(minBlockToProcess),
              Some(maxBlockToProcess)
            )
          )
          .persist(),
        noBlocks,
        noTransactions,
        maxBlockTimestamp,
        maxTxsPerBlock
      )
    }

    (
      exchangeRates,
      blks,
      txs,
      source.txFee(), // For now unfiltered.
      traces,
      tokenTxs,
      tokenConfigurations,
      noBlocks,
      noTransactions,
      maxBlockTimestamp,
      maxTxsPerBlock
    )
  }

  override def run(from: Option[Int], to: Option[Int]): Unit = {

    // spark.conf.set("spark.sql.ansi.enabled", true)

    println("Running tron specific transformations.")

    val (
      exchangeRates,
      blocks,
      txs,
      txFees,
      traces,
      tokenTxs,
      tokenConfigurations,
      noBlocks,
      noTransactions,
      maxBlockTimestamp,
      maxTxsPerBlock
    ) = prepareAndLoad()

    val (addressIds, noAddresses) =
      timeJob("Computing Address Ids") {

        val ids = computeCached("addressIds") {
          transformation
            .computeAddressIds(
              traces,
              txs,
              tokenTxs,
              maxTxsPerBlock
            )
        }

        val noAddresses = ids.count()
        printStat("#addresses", noAddresses.toString())

        printDatasetStats(ids, "addressIds")

        (ids, noAddresses)
      }
    printStat("#addresses", noAddresses)

    /* computing and storing balances */
    timeJob("Computing balances") {
      if (
        sink
          .areBalancesEmpty() || config.forceOverwrite.toOption.getOrElse(false)
      ) {

        val balances = transformation
          .computeBalances(
            txs,
            txFees,
            traces,
            addressIds,
            tokenTxs,
            tokenConfigurations
          )
          .persist()

        printDatasetStats(balances, "balances")

        if (debug > 0) {
          println("null balances")
          balances.filter(col("addressId").isNull).show(100)
          printStat("#balances", balances.count())
        }

        sink.saveBalances(balances.filter(col("addressId").isNotNull))

        balances.unpersist(true)
        txFees.unpersist(true)
      } else {
        println("Warning - balances not empty skipping stage.")
      }
    }

    /* computing and storing address id prefixes */
    timeJob("Computing and storing addr id lookups") {

      if (
        sink.areAddressIdsEmpty() || config.forceOverwrite.toOption.getOrElse(
          false
        )
      ) {
        val addressIdsByAddressPrefix =
          addressIds.toDF.transform(
            TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
              "address",
              "addressPrefix",
              config.addressPrefixLength()
            )
          )

        printDatasetStats(
          addressIdsByAddressPrefix,
          "addressIdsByAddressPrefix"
        )

        if (debug > 0) {
          println("null addressIdsByAddressPrefix addresses")
          addressIdsByAddressPrefix.filter(col("addressPrefix").isNull).show(10)
        }

        sink.saveAddressIdsByPrefix(
          addressIdsByAddressPrefix
        )
      } else {
        println("Warning - AddressId not empty skipping stage.")
      }
    }

    val transactionIds = timeJob("Computing transaction IDs") {
      computeCached("transactionIds") {
        val filtered_txs = txs
          .transform(transformation.onlySuccessfulTxs)
          .transform(transformation.removeUnknownRecipientTxs)
          .transform(transformation.txContractCreationAsToAddress)
          .as[Transaction]
        transformation.computeTransactionIds(filtered_txs)
      }
    }

    if (debug > 1) {
      val txs = transactionIds.count()
      val blub =
        transactionIds.dropDuplicates("transaction", "transactionId").count()
      printStat("nr txid", txs)
      printStat("nr txid without duplicates", blub)
    }

    /* computing and storing address id prefixes */
    timeJob("Computing and storing txid lookups") {

      if (
        sink.areTransactionIdsGroupEmpty() || config.forceOverwrite.toOption
          .getOrElse(false)
      ) {
        val transactionIdsByTransactionIdGroup =
          transactionIds.toDF.transform(
            TransformHelpers
              .withSortedIdGroup[TransactionIdByTransactionIdGroup](
                "transactionId",
                "transactionIdGroup",
                config.bucketSize()
              )
          )

        sink.saveTransactionIdsByGroup(transactionIdsByTransactionIdGroup)
      } else {
        println(
          "Warning - transactionIdsByTransactionIdGroup not empty skipping stage."
        )
      }
      if (
        sink.areTransactionIdsPrefixEmpty() || config.forceOverwrite.toOption
          .getOrElse(false)
      ) {
        val transactionIdsByTransactionPrefix =
          transactionIds.toDF.transform(
            TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
              "transaction",
              "transactionPrefix",
              config.txPrefixLength()
            )
          )
        sink.saveTransactionIdsByTxPrefix(transactionIdsByTransactionPrefix)
      } else {
        println(
          "Warning - transactionIdsByTransactionPrefix not empty skipping stage."
        )
      }
    }

    val contracts = timeJob("Computing contracts") {
      computeCached("contracts") {
        transformation.computeContracts(traces, txs, addressIds)
      }
    }
    printDatasetStats(contracts, "contracts")

    if (debug > 0) {
      printStat("# contracts", contracts.count())
    }

    val encodedTransactions = timeJob("Encoding transactions") {
      computeCached("encodedTransactions") {
        transformation
          .computeEncodedTransactions(
            traces,
            transactionIds,
            txs,
            addressIds,
            exchangeRates
          )
      }
    }

    printDatasetStats(encodedTransactions, "encodedTransactions")

    /*
      Caution this is a functional count it allows
      to unpersist tx and traces here since
      dataset is forced to be cached
     */
    printStat("#encoded Txs", encodedTransactions.count())
    txs.unpersist(true)
    traces.unpersist(true)

    val tokenExchangeRates = timeJob("Computing token exchange rates") {
      val rates = transformation
        .computeTokenExchangeRates(
          blocks,
          source.tokenExchangeRates(),
          tokenTxs,
          tokenConfigurations
        )
        .persist()

      sink.saveTokenExchangeRates(rates)
      rates
    }

    val encodedTokenTransfers =
      timeJob("Compute encoded token txs") {
        computeCached("encodedTokenTransfers") {
          transformation
            .computeEncodedTokenTransfers(
              tokenTxs,
              tokenConfigurations,
              transactionIds,
              addressIds,
              exchangeRates,
              tokenExchangeRates
            )
        }
      }

    printDatasetStats(encodedTokenTransfers, "encodedTokenTransfers")

    /*
      Caution this is a functional count it allows
      to unpersist exchangeRates, txids, tokentxs here since
      dataset is forced to be cached
     */
    printStat("#encoded token Txs", encodedTokenTransfers.count())

    exchangeRates.unpersist(true)
    tokenExchangeRates.unpersist(true)
    transactionIds.unpersist(true)
    tokenTxs.unpersist(true)

    timeJob("Computing and storing block transactions") {
      if (
        sink.areBlockTransactionsEmpty() || config.forceOverwrite.toOption
          .getOrElse(false)
      ) {
        val blockTransactions =
          computeCached("blockTransactions") {
            transformation
              .computeBlockTransactions(encodedTransactions)
          }

        printDatasetStats(blockTransactions, "blockTransactions")

        sink.saveBlockTransactions(blockTransactions)
        blockTransactions.unpersist(true)
      } else {
        println("Warning - blockTransactions not empty skipping stage.")
      }

    }

    val addressTransactions = timeJob("Computing address transactions") {
      computeCached("addressTransactions") {
        transformation
          .computeAddressTransactions(
            encodedTransactions,
            encodedTokenTransfers
          )
      }
    }

    if (sink.areAddressTransactionsEmtpy()) {
      printDatasetStats(addressTransactions, "addressTransactions")

      if (debug > 0) {
        printStat("#address Txs", addressTransactions.count())
      }
    }

    timeJob("Saving address transactions and lookups") {
      if (sink.areAddressTransactionsEmtpy()) {
        sink.saveAddressTransactions(addressTransactions)

        if (debug > 0) {
          addressTransactions.show(100)
        }
      } else {
        println("Warning - addressTransactions not empty skipping stage.")
      }

      if (sink.areAddressTransactionsSecondaryGroupEmtpy()) {
        val addressTransactionsSecondaryIds =
          TransformHelpers
            .computeSecondaryPartitionIdLookup[AddressTransactionSecondaryIds](
              addressTransactions.toDF,
              "addressIdGroup",
              "addressIdSecondaryGroup"
            )

        sink.saveAddressTransactionBySecondaryId(
          addressTransactionsSecondaryIds
        )
      } else {
        println(
          "Warning - addressTransactionsSecondaryIds not empty skipping stage."
        )
      }
    }

    timeJob("Computing address and storing statistics") {
      if (
        sink
          .areAddressEmpty() || config.forceOverwrite.toOption.getOrElse(false)
      ) {
        val addresses =
          computeCached("addresses") {
            transformation.computeAddresses(
              encodedTransactions,
              encodedTokenTransfers,
              addressTransactions,
              addressIds,
              contracts
            )
          }

        printDatasetStats(addresses, "addresses")

        sink.saveAddresses(addresses)
        addresses.unpersist(true)
      } else {
        println("Warning - addresses not empty skipping stage.")
      }
    }

    addressTransactions.unpersist(true)
    addressIds.unpersist(true)
    contracts.unpersist(true)

    val noAddressRelations =
      timeJob("Computing and storing address relations") {
        val addressRelations =
          computeCached("addressRelations") {
            transformation
              .computeAddressRelations(
                encodedTransactions,
                encodedTokenTransfers
              )
          }

        printDatasetStats(addressRelations, "addressRelations")

        if (
          sink.areAddressIncomingRelationsEmpty() || config.forceOverwrite.toOption
            .getOrElse(false)
        ) {
          sink.saveAddressIncomingRelations(
            addressRelations.sort(
              "dstAddressIdGroup",
              "dstAddressIdSecondaryGroup"
            )
          )
        } else {
          println(
            "Warning - saveAddressIncomingRelations not empty skipping stage."
          )
        }
        if (
          sink.areAddressOutgoingRelationsEmpty() || config.forceOverwrite.toOption
            .getOrElse(false)
        ) {
          sink.saveAddressOutgoingRelations(
            addressRelations.sort(
              "srcAddressIdGroup",
              "srcAddressIdSecondaryGroup"
            )
          )
        } else {
          println(
            "Warning - saveAddressOutgoingRelations not empty skipping stage."
          )
        }

        if (
          sink.areAddressIncomingRelationsSecondaryIdsEmpty() || config.forceOverwrite.toOption
            .getOrElse(false)
        ) {
          val addressIncomingRelationsSecondaryIds =
            TransformHelpers
              .computeSecondaryPartitionIdLookup[
                AddressIncomingRelationSecondaryIds
              ](
                addressRelations.toDF,
                "dstAddressIdGroup",
                "dstAddressIdSecondaryGroup"
              )

          sink.saveAddressIncomingRelationsBySecondaryId(
            addressIncomingRelationsSecondaryIds
          )
        } else {
          println(
            "Warning - addressIncomingRelationsSecondaryIds not empty skipping stage."
          )
        }
        if (
          sink.areAddressOutgoingRelationsSecondaryIdsEmpty() || config.forceOverwrite.toOption
            .getOrElse(false)
        ) {
          val addressOutgoingRelationsSecondaryIds =
            TransformHelpers
              .computeSecondaryPartitionIdLookup[
                AddressOutgoingRelationSecondaryIds
              ](
                addressRelations.toDF,
                "srcAddressIdGroup",
                "srcAddressIdSecondaryGroup"
              )

          sink.saveAddressOutgoingRelationsBySecondaryId(
            addressOutgoingRelationsSecondaryIds
          )
        } else {
          println(
            "Warning - addressOutgoingRelationsSecondaryIds not empty skipping stage."
          )
        }

        addressRelations.count()
      }

    timeJob("Computing summary statistics") {
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

}
