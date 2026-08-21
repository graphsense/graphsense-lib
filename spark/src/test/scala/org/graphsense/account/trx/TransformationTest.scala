package org.graphsense.account.trx

import org.apache.spark.sql.functions.{col, max}
import org.graphsense.account.models.{
  AddressIdByAddressPrefix,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.TestBase
import org.graphsense.TransformHelpers
import org.apache.spark.sql.Dataset
import org.graphsense.account.models._
import org.graphsense.models.ExchangeRates
import org.graphsense.account.trx.models.{Trace, TxFee}

class TransformationTest extends TestBase {
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  private val bucketSize = 2
  private val prefixLength = 4
  private val bucket_size_address_txs = 150000
  private val addressrelations_ids_nbuckets = 100
  private val t =
    new TrxTransformation(
      spark,
      bucketSize,
      bucket_size_address_txs,
      addressrelations_ids_nbuckets
    )

  case class SourceData(
      exchangeRates: Dataset[ExchangeRates],
      blocks: Dataset[Block],
      transactions: Dataset[Transaction],
      traces: Dataset[Trace],
      tokenConfigurations: Dataset[TokenConfiguration],
      tokenTransfers: Dataset[TokenTransfer],
      txFees: Dataset[TxFee]
  )

  case class ComputeOutput(
      addressIds: Dataset[AddressId],
      transactionIds: Dataset[TransactionId],
      transactionIdsPrefix: Dataset[TransactionIdByTransactionPrefix],
      balances: Dataset[Balance],
      contracts: Dataset[Contract],
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer],
      addressTransactions: Dataset[AddressTransaction],
      blockTransactions: Dataset[BlockTransaction],
      addressRelations: Dataset[AddressRelation]
  )

  case class TestData(source: SourceData, output: ComputeOutput)

  def testGenericInvariants(data: TestData): Unit = {

    note("Address ids are unique")
    assert(
      data.output.addressIds
        .count() == data.output.addressIds.dropDuplicates("addressId").count()
    )

    // print entire data.output.addressIds and data.output.blockTransactions

    note("Block Transactions have one record per tx")
    assert(
      data.output.transactionIds.count() == data.output.blockTransactions
        .count(),
      "txids.count == blocktxs.count"
    )
    assert(
      data.source.transactions
        .transform(t.onlySuccessfulTxs)
        .transform(t.removeUnknownRecipientTxs)
        .transform(t.txContractCreationAsToAddress)
        .count() == data.output.blockTransactions.count(),
      "tx.count == blocktx.count"
    )

    assert(
      data.output.contracts.na
        .drop()
        .count() === data.output.contracts.count(),
      "no null values in contracts"
    )

    assert(
      data.output.blockTransactions.na
        .drop()
        .count() === data.output.blockTransactions.count(),
      "no null values in blockTransactions"
    )

    // data.output.addressRelations.show(100)

    assert(
      data.output.addressRelations
        .drop("tokenValues")
        .na
        .drop()
        .count() === data.output.addressRelations.count(),
      "no null values in addressRelations"
    )

    assert(
      data.output.transactionIds.na
        .drop()
        .count() === data.output.transactionIds.count(),
      "no null values in transaction ids"
    )

    // data.output.encodedTransactions
    //   .filter(
    //     $"transactionId".isNull || $"blockId".isNull || $"srcAddressId".isNull || $"dstAddressId".isNull || $"value".isNull || $"fiatValues".isNull
    //   )
    //   .show(100)
    assert(
      data.output.encodedTransactions
        .drop("traceIndex")
        .na
        .drop()
        .count() === data.output.encodedTransactions.count(),
      "no null values in encodedTransactions"
    )

    assert(
      data.output.encodedTransactions
        .filter($"transactionId".isNull)
        .count() == 0
    )

    assert(
      data.output.encodedTokenTransfers.na
        .drop()
        .count() === data.output.encodedTokenTransfers.count(),
      "no null values in encodedTokenTransfers"
    )
    ()
  }

  def compute_transform(dataset: String): TestData = {
    val inputDir = s"src/test/resources/account/trx/${dataset}/"
    inputDir + "reference/"
    val ds = new TestTrxSource(spark, inputDir)

    // read raw data
    val blocks = ds.blocks()
    val transactions = ds.transactions()
    val traces = ds.traces()
    val exchangeRatesRaw = ds.exchangeRates()
    val tokenConfigurations = ds.tokenConfigurations()
    val tokenTransfers = ds.tokenTransfers()
    val txFees = ds.txFee()

    // read ref values
    val lastBlockMaxTxCount = blocks
      .select(max(col("transactionCount")))
      .first
      .get(0)
      .toString
      .toLong

    // transactions.count()

    val exchangeRates =
      t.computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    val filtered_txs = transactions
      .transform(t.onlySuccessfulTxs)
      .transform(t.removeUnknownRecipientTxs)
      .transform(t.txContractCreationAsToAddress)
      .as[Transaction]
    val transactionIds = t.computeTransactionIds(filtered_txs)
    transactionIds.toDF.transform(
      TransformHelpers.withSortedIdGroup[TransactionIdByTransactionIdGroup](
        "transactionId",
        "transactionIdGroup",
        bucket_size_address_txs
      )
    )
    val transactionIdsPrefix = transactionIds.toDF.transform(
      TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
        "transaction",
        "transactionPrefix",
        prefixLength
      )
    )
    val addressIds = t
      .computeAddressIds(
        traces,
        transactions,
        tokenTransfers,
        lastBlockMaxTxCount
      )
      .sort("addressId")

    val addressIdsHash = t
      .computeAddressIdsByHash(traces, transactions, tokenTransfers)
      .sort("addressId")

    assert(addressIds.count() == addressIdsHash.count())
    assert(addressIds.count() == addressIds.dropDuplicates("addressId").count())
    assert(
      addressIdsHash
        .count() == addressIdsHash.dropDuplicates("addressId").count()
    )
    assertDataFrameEquality(
      addressIds.sort("address").select($"address"),
      addressIdsHash.sort("address").select($"address")
    )

    val addressIdPrefixes = addressIds.toDF
      .transform(
        TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix",
          prefixLength
        )
      )
      .sort("addressId")

    assert(addressIds.count() == addressIdPrefixes.count())

    val balances = t.computeBalances(
      transactions,
      txFees,
      traces,
      addressIds,
      tokenTransfers,
      tokenConfigurations
    )

    // assert(balances.count() == balancesWithFee.count())

    val encodedTransactions =
      t.computeEncodedTransactions(
        traces,
        transactionIds,
        transactions,
        addressIds,
        exchangeRates
      ).persist()

    val encodedTokenTransfers =
      t.computeEncodedTokenTransfers(
        tokenTransfers,
        tokenConfigurations,
        transactionIds,
        addressIds,
        exchangeRates
      ).persist()

    val blockTransactions = t
      .computeBlockTransactions(encodedTransactions)
      .sort("blockId")

    val addressTransactions = t
      .computeAddressTransactions(
        encodedTransactions,
        encodedTokenTransfers
      )
      .persist()

    val contracts = t
      .computeContracts(
        traces,
        transactions,
        addressIds
      )
      .persist()

    t
      .computeAddresses(
        encodedTransactions,
        encodedTokenTransfers,
        addressTransactions,
        addressIds,
        contracts
      )
      .persist()

    val addressRelations = t
      .computeAddressRelations(encodedTransactions, encodedTokenTransfers)
      .sort("srcAddressId", "dstAddressId")

    val source = SourceData(
      exchangeRates,
      blocks,
      transactions,
      traces,
      tokenConfigurations,
      tokenTransfers,
      txFees
    )
    val sink = ComputeOutput(
      addressIds,
      transactionIds,
      transactionIdsPrefix,
      balances,
      contracts,
      encodedTransactions,
      encodedTokenTransfers,
      addressTransactions,
      blockTransactions,
      addressRelations
    )
    TestData(source, sink)
  }

  test("Check 0to10000 transform") {

    val data = compute_transform("0to10000")

    note("Test generic invariants")

    testGenericInvariants(data)

    note("Test blocks")

    // Compute values
    val lastBlockTimestamp = data.source.blocks
      .select(max(col("timestamp")))
      .first
      .getInt(0)

    note("Check statistics")
    assert(data.source.blocks.count.toInt == 10001, "expected 10001 blocks")
    assert(lastBlockTimestamp == 1529921544)
    assert(
      data.source.transactions.count() == 2172,
      "expected 2172 transaction"
    )
    assert(data.output.addressIds.count() == 1772, "expected 1772 addresses")
    assert(
      data.output.addressRelations.count() == 1848,
      "expected 1848 address relations"
    )

    assert(
      data.output.contracts.count() == 0,
      "more contracts than ... expected"
    )

  }

  test("Check 56804500to56805500 transform") {
    val data = compute_transform("56804500to56805500")

    note("Test generic invariants")

    testGenericInvariants(data)

    note("Test blocks")

    // Compute values
    val lastBlockTimestamp = data.source.blocks
      .select(max(col("timestamp")))
      .first
      .getInt(0)

    note("Check statistics")
    assert(data.source.blocks.count.toInt == 1001, "expected 1001 blocks")
    assert(lastBlockTimestamp == 1701049164)
    assert(
      data.source.transactions.count() == 77090,
      "expected 77090 transaction"
    )
    assert(data.output.addressIds.count() == 50527, "expected 50527 addresses")
    assert(
      data.output.addressRelations.count() == 77633,
      "expected 77633 address relations"
    )

    assert(
      data.output.contracts.count() == 85,
      "more contracts than 85 expected"
    )

    assert(
      data.output.encodedTransactions
        .filter($"traceIndex".isNotNull)
        .count() > 0,
      "has encoded txs from traces"
    )

    assert(
      data.output.addressRelations
        .filter($"tokenValues".isNotNull)
        .count() == 27187,
      "has token value edges"
    )

  }

}
