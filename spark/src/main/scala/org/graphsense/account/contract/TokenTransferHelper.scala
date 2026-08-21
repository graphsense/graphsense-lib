package org.graphsense.account.contract.tokens

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.graphsense.account.models.{Log, TokenConfiguration, TokenTransfer}
import scala.util.{Failure, Success, Try}

class TokenTransferHelper(spark: SparkSession) {

  import spark.implicits._

  def getTokenConfigurations(
      supportedTokens: List[TokenConfiguration]
  ): Dataset[TokenConfiguration] = {
    spark
      .createDataFrame(
        supportedTokens
      )
      .as[TokenConfiguration]
  }

  def getTokenTransfers(
      logs: Dataset[Log],
      forTokens: Seq[Array[Byte]]
  ): Dataset[TokenTransfer] = {
    logs
      .filter(col("topic0") === lit(Erc20Decoder.transferTopicHash))
      .filter(col("address").isin(forTokens: _*))
      .map(x => Erc20Decoder.decodeTransfer(x))
      .filter((x: Try[TokenTransfer]) => x.isSuccess)
      .map(x => x.get)
  }

  def getNonDecodableTransferLogs(
      logs: Dataset[Log],
      forTokens: Seq[Array[Byte]]
  ): Dataset[Log] = {
    logs
      .filter(col("topic0") === lit(Erc20Decoder.transferTopicHash))
      .filter(col("address").isin(forTokens: _*))
      .filter(x =>
        Erc20Decoder.decodeTransfer(x) match {
          case Success(_) => false
          case Failure(_) => true
        }
      )
  }

  def humanReadableTokenTransfers(
      transfers: Dataset[TokenTransfer]
  ): DataFrame = {
    val htoStr = udf((x: Array[Byte]) => x)
    val transfersStr = transfers
      .withColumn("blockId", transfers("blockId"))
      .withColumn("transactionIndex", transfers("transactionIndex"))
      .withColumn("logIndex", transfers("logIndex"))
      .withColumn("txhash", htoStr(transfers("txhash")))
      .withColumn("tokenAddress", htoStr(transfers("tokenAddress")))
      .withColumn("from", htoStr(transfers("from")))
      .withColumn("to", htoStr(transfers("to")))
      .withColumn("value", transfers("value"))
    transfersStr.toDF
  }
}
