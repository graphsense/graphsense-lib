package org.graphsense.account.eth

import org.apache.spark.sql.functions.{col, max}
import org.graphsense.account.models.{
  Address,
  AddressId,
  AddressIdByAddressPrefix,
  AddressRelation,
  AddressTransaction,
  BlockTransaction,
  Contract,
  EncodedTokenTransfer,
  EncodedTransaction,
  TokenTransfer,
  TransactionId,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.TestBase
import org.graphsense.TransformHelpers
import org.graphsense.models.ExchangeRates

class TransformationTest extends TestBase {
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  private val inputDir = "src/test/resources/account/eth/simple_graph/"
  private val refDir = inputDir + "reference/"
  private val ds = new TestEthSource(spark, inputDir)
  private val bucketSize = 2
  private val prefixLength = 4
  private val bucket_size_address_txs = 150000
  private val addressrelations_ids_nbuckets = 100
  private val t =
    new EthTransformation(
      spark,
      bucketSize,
      bucket_size_address_txs,
      addressrelations_ids_nbuckets
    )

  // read raw data
  val blocks = ds.blocks()
  val transactions = ds.transactions()
  val traces = ds.traces()
  val exchangeRatesRaw = ds.exchangeRates()

  val tokenTransfers = spark.emptyDataset[TokenTransfer]
  val encodedTokenTransfers = spark.emptyDataset[EncodedTokenTransfer]
  val contracts = spark.emptyDataset[Contract]

  // read ref values
  val transactionIdsRef =
    readTestDataBase64[TransactionId](refDir + "transactions_ids.json")

  val transactionIdsGroupRef =
    readTestDataBase64[TransactionIdByTransactionIdGroup](
      refDir + "transactions_ids_by_id_group.json"
    )

  val transactionIdsPrefixRef =
    readTestDataBase64[TransactionIdByTransactionPrefix](
      refDir + "transactions_ids_by_prefix.json"
    )

  val addressIdsRef =
    readTestData[AddressId](refDir + "address_ids.csv")

  val addressIdsPrefixRef = readTestData[AddressIdByAddressPrefix](
    refDir + "address_ids_by_prefix.csv"
  )

  val exchangeRatesRef =
    readTestData[ExchangeRates](refDir + "exchange_rates.json")

  val encodedTransactionsRef =
    readTestData[EncodedTransaction](
      refDir + "encoded_transactions.json"
    )

  val addressTransactionsRef =
    readTestDataBase64[AddressTransaction](
      refDir + "address_transactions.json"
    )

  val blockTransactionsRef =
    readTestData[BlockTransaction](refDir + "block_transactions.json")

  val addressesRef =
    readTestDataBase64[Address](refDir + "addresses.json")

  val addressRelationsRef =
    readTestDataBase64[AddressRelation](refDir + "address_relations.json")

  // Compute values
  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .select(max(col("timestamp")))
    .first
    .getInt(0)
  val noTransactions = transactions.count()

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw)
      .persist()

  val transactionIds = t.computeTransactionIds(transactions)
  val transactionIdsByTransactionIdGroup =
    transactionIds.toDF.transform(
      TransformHelpers.withSortedIdGroup[TransactionIdByTransactionIdGroup](
        "transactionId",
        "transactionIdGroup",
        bucket_size_address_txs
      )
    )
  val transactionIdsByTransactionPrefix =
    transactionIds.toDF.transform(
      TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
        "transaction",
        "transactionPrefix",
        prefixLength
      )
    )
  val addressIds = t
    .computeAddressIds(traces, tokenTransfers)
    .sort("addressId")

  val addressIdsByAddressPrefix =
    addressIds.toDF
      .transform(
        TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix",
          prefixLength
        )
      )
      .sort("addressId")

  val encodedTransactions =
    t.computeEncodedTransactions(
      traces,
      transactionIds,
      addressIds,
      exchangeRates
    ).sort("blockId")
      .persist()

  val blockTransactions = t
    .computeBlockTransactions(encodedTransactions)
    .sort("blockId")

  val addressTransactions = t
    .computeAddressTransactions(
      encodedTransactions,
      encodedTokenTransfers
    )
    .persist()

  val addresses = t
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

  // redo regression checks, csv and json
  transactionIds.write
    .mode("overwrite")
    .json("test_ref/simple_transactions_ids.json")
  addressIds.write.mode("overwrite").json("test_ref/simple_address_ids.json")
  exchangeRates.write
    .mode("overwrite")
    .json("test_ref/simple_exchange_rates.json")
  addresses.write.mode("overwrite").json("test_ref/simple_addresses.json")
  transactionIdsByTransactionIdGroup.write
    .mode("overwrite")
    .json("test_ref/simple_transactions_ids_by_id_group.json")
  transactionIdsByTransactionPrefix.write
    .mode("overwrite")
    .json("test_ref/simple_transactions_ids_by_prefix.json")
  addressIdsByAddressPrefix.write
    .mode("overwrite")
    .json("test_ref/simple_addressIdsByAddressPrefix.json")
  encodedTransactions
    .filter($"transactionId".isNotNull)
    .write
    .mode("overwrite")
    .json("test_ref/simple_encoded_transactions.json")
  addressTransactions.write
    .mode("overwrite")
    .json("test_ref/simple_address_transactionsS.json")
  blockTransactions.write
    .mode("overwrite")
    .json("test_ref/simple_block_transactions.json")
  addressRelations.write
    .mode("overwrite")
    .json("test_ref/simple_address_relations.json")

  note("Test lookup tables")

  test("Transaction IDs") {
    assertDataFrameEquality(transactionIds, transactionIdsRef)
  }
  test("Transaction IDs by ID group") {
    assertDataFrameEquality(
      transactionIdsByTransactionIdGroup,
      transactionIdsGroupRef
    )
  }
  test("Transaction IDs by transaction prefix") {
    assertDataFrameEquality(
      transactionIdsByTransactionPrefix,
      transactionIdsPrefixRef
    )
  }

  assert(
    blockTransactions
      .count() === blockTransactions.dropDuplicates("txId").count(),
    "duplicates in block transactions"
  )

  test("Address IDs") {
    assertDataFrameEquality(addressIds, addressIdsRef)
  }
  test("Address IDs by address prefix") {
    assertDataFrameEquality(addressIdsByAddressPrefix, addressIdsPrefixRef)
  }

  note("Test exchange rates")

  test("Exchange rates") {
    assertDataFrameEquality(exchangeRates, exchangeRatesRef)
  }

  test("Encoded transactions") {
    assertDataFrameEquality(
      encodedTransactions.filter($"transactionId".isNotNull),
      encodedTransactionsRef
    )
  }

  note("Test blocks")

  test("Block transactions") {
    assertDataFrameEquality(blockTransactions, blockTransactionsRef)
  }

  note("Test address graph")

  test("Address transactions") {
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }

  test("Addresses") {
    assertDataFrameEqualityGeneric(
      addresses,
      addressesRef,
      ignoreCols = List(
        "noIncomingTxsZeroValue",
        "noOutgoingTxsZeroValue",
        "inDegreeZeroValue",
        "outDegreeZeroValue"
      )
    )
  }

  test("Address relations") {
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }

  test("Check statistics") {
    assert(blocks.count.toInt == 84, "expected 84 blocks")
    assert(lastBlockTimestamp == 1438919571)
    assert(transactions.count() == 10, "expected 10 transaction")
    assert(addressIds.count() == 59, "expected 59 addresses")
    assert(addressRelations.count() == 9, "expected 9 address relations")
  }
}
