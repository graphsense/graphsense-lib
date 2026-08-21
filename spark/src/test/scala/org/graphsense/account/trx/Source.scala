package org.graphsense.account.trx

import org.apache.spark.sql.{Dataset, SparkSession}
import org.graphsense.account.{AccountSourceWithTokens, TokenSet}
import org.graphsense.account.contract.tokens.TokenTransferHelper
import org.graphsense.account.trx.tokens.TrxTokenSet
import org.graphsense.account.trx.models.Trace
import org.graphsense.Helpers.readTestDataBase64
import org.graphsense.account.models.{
  Block,
  Log,
  TokenConfiguration,
  Transaction
}
import org.graphsense.models.{ExchangeRatesRaw, TokenExchangeRatesRaw}
import org.graphsense.account.trx.models.Trc10
import org.graphsense.account.trx.models.TxFee
import org.graphsense.account.models.TokenTransfer

object TestTokenSet extends TokenSet {

  override def getSupportedTokens(): List[TokenConfiguration] = {
    val fixed_set_of_tokens = Set("USDT", "USDC", "WTRX")
    TrxTokenSet
      .getSupportedTokens()
      .filter(x => fixed_set_of_tokens contains x.currencyTicker)
  }

}

class TestTrxSource(spark: SparkSession, inputDir: String)
    extends TrxSource
    with AccountSourceWithTokens {

  import spark.implicits._

  override def getTokenSet(): TokenSet = {
    TestTokenSet
  }

  override def tokenTransferHelper = new TokenTransferHelper(spark)

  override def trc10Tokens(): Dataset[Trc10] = {
    throw new NotImplementedError("Not yet implemented")
  }

  override def txFee(): Dataset[TxFee] = {
    readTestDataBase64[TxFee](spark, inputDir + "tx_fee.json")
  }

  def blocks(): Dataset[Block] = {
    readTestDataBase64[Block](spark, inputDir + "blocks.json")
  }
  def transactions(): Dataset[Transaction] = {
    readTestDataBase64[Transaction](spark, inputDir + "transactions.json")
  }

  override def tokenTransfers(): Dataset[TokenTransfer] = {
    readTestDataBase64[TokenTransfer](spark, inputDir + "token_transfers.json")
  }
  def logs(): Dataset[Log] = {
    throw new NotImplementedError("Not yet implemented")
    // readTestData[Log](spark, inputDir + "logs.json")
  }
  def exchangeRates(): Dataset[ExchangeRatesRaw] = {
    readTestDataBase64[ExchangeRatesRaw](
      spark,
      inputDir + "exchange_rates.json"
    )
  }

  def tokenExchangeRates(): Dataset[TokenExchangeRatesRaw] = {
    spark.emptyDataset[TokenExchangeRatesRaw]
  }

  def traces(): Dataset[Trace] = {
    readTestDataBase64[Trace](spark, inputDir + "traces.json")
  }

}
