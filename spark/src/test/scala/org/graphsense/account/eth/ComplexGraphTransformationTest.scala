package org.graphsense.account.eth

import org.apache.spark.sql.functions.{col, max}
import org.graphsense.account.models.{
  Address,
  AddressRelation,
  AddressTransaction,
  Contract,
  EncodedTokenTransfer,
  TokenTransfer
}
import org.graphsense.TestBase

class ComplexGraphTransformationTest extends TestBase {
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  private val inputDir = "src/test/resources/account/eth/complex_graph/"
  private val refDir = inputDir + "reference/"
  private val ds = new TestEthSource(spark, inputDir)
  private val t = new EthTransformation(spark, 2, 100000, 100)

  // read raw data
  val blocks = ds.blocks()
  val txs = ds.transactions()
  val traces = ds.traces()
  val exchangeRatesRaw = ds.exchangeRates()
  val tokenTransfers = spark.emptyDataset[TokenTransfer]
  val encodedTokenTransfers = spark.emptyDataset[EncodedTokenTransfer]
  val contracts = spark.emptyDataset[Contract]

  // read ref values
  val addressTransactionsRef =
    readTestDataBase64[AddressTransaction](
      refDir + "address_transactions.json"
    )

  val addressesRef =
    readTestDataBase64[Address](refDir + "addresses.json")
  val addressRelationsRef =
    readTestDataBase64[AddressRelation](refDir + "address_relations.json")

  // Compute values

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw).persist()
  val txIds = t.computeTransactionIds(txs)
  val addressIds =
    t.computeAddressIds(traces, tokenTransfers)
  val encodedTxs =
    t.computeEncodedTransactions(traces, txIds, addressIds, exchangeRates)
  val addressTransactions = t.computeAddressTransactions(
    encodedTxs,
    encodedTokenTransfers
  )

  val addresses =
    t.computeAddresses(
      encodedTxs,
      encodedTokenTransfers,
      addressTransactions,
      addressIds,
      contracts
    ).persist()
  val addressRelations = t
    .computeAddressRelations(encodedTxs, encodedTokenTransfers)
    .sort("srcAddressId", "dstAddressId")

  // redo regression checks
  addressTransactions.write
    .mode("overwrite")
    .json("test_ref/address_transactions.json")
  addresses.write.mode("overwrite").json("test_ref/addresses.json")
  addressRelations.write
    .mode("overwrite")
    .json("test_ref/address_relations.json")

  // test equality

  note("Testing address graph:")
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
    val lastBlockTimestamp = blocks
      .select(max(col("timestamp")))
      .first
      .getInt(0)

    assert(blocks.count.toInt == 94, "expected 94 blocks")
    assert(lastBlockTimestamp == 1438919571)
    assert(txs.count() == 10, "expected 10 transaction")
    assert(addressIds.count() == 59, "expected 59 addresses")
    assert(addressRelations.count() == 9, "expected 9 address relations")
  }

}
