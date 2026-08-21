package org.graphsense.account.eth

import org.apache.spark.sql.{Dataset, SparkSession}
import org.graphsense.account.{AccountSourceWithTokens, TokenSet}
import org.graphsense.account.contract.tokens.TokenTransferHelper
import org.graphsense.account.eth.tokens.EthTokenSet
import org.graphsense.account.eth.models.Trace
import org.graphsense.Helpers.readTestData
import org.graphsense.account.models.{
  Block,
  Log,
  TokenConfiguration,
  Transaction
}
import org.graphsense.models.{ExchangeRatesRaw, TokenExchangeRatesRaw}

object TestTokenSet extends TokenSet {

  override def getSupportedTokens(): List[TokenConfiguration] = {
    val fixed_set_of_tokens = Set("USDT", "USDC", "WETH")
    EthTokenSet
      .getSupportedTokens()
      .filter(x => fixed_set_of_tokens contains x.currencyTicker)
  }

}

class TestEthSource(spark: SparkSession, inputDir: String)
    extends EthSource
    with AccountSourceWithTokens {
  import spark.implicits._

  override def getTokenSet(): TokenSet = {
    TestTokenSet
  }

  override def tokenTransferHelper = new TokenTransferHelper(spark)

  def blocks(): Dataset[Block] = {
    readTestData[Block](spark, inputDir + "blocks.csv")
  }
  def transactions(): Dataset[Transaction] = {
    readTestData[Transaction](spark, inputDir + "transactions.csv")
  }
  def logs(): Dataset[Log] = {
    readTestData[Log](spark, inputDir + "logs.json")
  }
  def exchangeRates(): Dataset[ExchangeRatesRaw] = {
    readTestData[ExchangeRatesRaw](spark, inputDir + "exchange_rates.json")
  }

  def tokenExchangeRates(): Dataset[TokenExchangeRatesRaw] = {
    spark.emptyDataset[TokenExchangeRatesRaw]
  }

  def traces(): Dataset[Trace] = {
    readTestData[Trace](spark, inputDir + "traces.csv")
  }

}
