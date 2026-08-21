package org.graphsense.account.eth

import org.apache.spark.sql.functions.col
import org.graphsense.account.models.Balance
import org.graphsense.TestBase

class BalancesTest extends TestBase {

  private val inputDir = "src/test/resources/account/eth/balance/"
  private val refDir = inputDir + "reference/"

  private val ds = new TestEthSource(spark, inputDir)
  private val t = new EthTransformation(spark, 2, 100000, 100)

  import spark.implicits._

  test("with mining activities and two unsuccessful transactions") {
    // two transactions did not succeed (signalled by invalid calltype, or status 0)

    val blocks = ds.blocks()
    val tx = ds.transactions()
    val traces = ds.traces()
    val tokenConfigs = ds.tokenConfigurations()
    val tokenTransfers = ds.tokenTransfers()

    val addressIds = t.computeAddressIds(traces, tokenTransfers)

    val balances =
      t.computeBalances(
        blocks,
        tx,
        traces,
        addressIds,
        tokenTransfers,
        tokenConfigs
      ).sort(col("currency"), col("addressId"))

    val balancesRef =
      readTestData[Balance](refDir + "balances.csv")

    // redo regression checks
    balancesRef.write.mode("overwrite").json("test_ref/balances.json")

    assertDataFrameEquality(balances, balancesRef)
  }
}
