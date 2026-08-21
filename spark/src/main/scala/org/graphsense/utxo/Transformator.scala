package org.graphsense.utxo

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  count,
  explode,
  lead,
  lit,
  round,
  row_number,
  struct,
  substring,
  sum,
  transform,
  when
}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}
import org.graphframes.GraphFrame
import org.graphsense.TransformHelpers
import org.graphsense.models._
import org.graphsense.utxo.{Fields => F}
import org.graphsense.utxo.models._

class Transformator(spark: SparkSession, bucketSize: Int) extends Serializable {

  import spark.implicits._

  def withAddressPrefix[T](
      addressColumn: String,
      prefixColumn: String,
      length: Int = 5,
      bech32Prefix: String = ""
  )(ds: Dataset[T]): DataFrame = {
    if (bech32Prefix.length == 0) {
      ds.withColumn(prefixColumn, substring(col(addressColumn), 0, length))
    } else {
      ds.withColumn(
        prefixColumn,
        when(
          substring(
            col(addressColumn),
            0,
            bech32Prefix.length
          ) === bech32Prefix,
          substring(
            col(addressColumn),
            bech32Prefix.length + 1,
            length
          )
        ).otherwise(substring(col(addressColumn), 0, length))
      )
    }
  }

  def toFiatCurrency(valueColumn: String, fiatValueColumn: String)(
      df: DataFrame
  ) = {
    df.withColumn(
      fiatValueColumn,
      transform(
        col(fiatValueColumn),
        (x: Column) =>
          ((col(valueColumn) * x / 1e6 + 0.5).cast(LongType) / 100.0)
            .cast(FloatType)
      )
    )
  }

  def plainAddressCluster(
      basicTxInputAddresses: DataFrame,
      removeCoinJoin: Boolean
  ) = {
    val addressCount = count(F.addressId).over(Window.partitionBy(F.txId))

    // filter transactions with multiple input addresses
    val collectiveInputAddresses = {
      if (removeCoinJoin) {
        println(
          "Clustering without coinjoin inputs"
        )
        basicTxInputAddresses.filter(
          col(F.coinjoin) === false
        )
      } else {
        println(
          "Clustering with coinjoin inputs"
        )
        basicTxInputAddresses
      }
    }.select(col(F.txId), col(F.addressId), addressCount.as("count"))
      .filter(col("count") > 1)
      .select(col(F.txId), col(F.addressId))

    // compute number of transactions per address
    val transactionCount =
      collectiveInputAddresses.groupBy(F.addressId).count()

    val basicAddressCluster = {
      val txWindow = Window.partitionBy(F.txId).orderBy("src")
      val inputGroups = transactionCount
        .filter(col("count") > 1)
        .select(F.addressId)
        .join(collectiveInputAddresses, F.addressId)
      val inputNodes = inputGroups.select(col(F.addressId).as("id")).distinct
      val inputEdges = inputGroups
        .withColumnRenamed(F.addressId, "src")
        .withColumn("dst", lead("src", 1).over(txWindow))
        .select(col("src"), col("dst"), col("txId").as("id"))
        .filter(col("dst").isNotNull)
      val inputGraph = GraphFrame(inputNodes, inputEdges)
      val connComp = inputGraph.connectedComponents.run()
      connComp.select(
        col("id"),
        col("component").as(F.clusterId).cast(IntegerType)
      )
    }

    val reprAddrId = "reprAddrId"

    // the initial representative must be one address per transaction, which also occurs
    // in other transactions (tx count >= 2 in collectiveInputAddresses);
    // choose address with highest tx count
    val initialRepresentative = {

      val transactionWindow = Window
        .partitionBy(F.txId)
        .orderBy(col("count").desc, col(F.addressId).asc)
      val rowNumber = row_number().over(transactionWindow)

      // for each tx, set address which is in most other transactions
      val addressMax = collectiveInputAddresses
        .join(transactionCount, F.addressId)
        .select(
          col(F.txId),
          col(F.addressId).as(reprAddrId),
          rowNumber.as("rank")
        )
        .filter(col("rank") === 1)
        .drop("rank")

      transactionCount
        .filter(col("count") === 1) // restrict to "trivial" addresses
        .select(F.addressId)
        .join(collectiveInputAddresses, F.addressId)
        .join(addressMax, F.txId)
        .select(F.addressId, reprAddrId)
    }

    val addressClusterRemainder =
      initialRepresentative
        .withColumnRenamed(F.addressId, "id")
        .join(
          basicAddressCluster.toDF(reprAddrId, F.clusterId),
          Seq(reprAddrId),
          "left_outer"
        )
        .select(
          col("id"),
          coalesce(col(F.clusterId), col(reprAddrId)).as(F.clusterId)
        )

    basicAddressCluster.union(addressClusterRemainder)
  }

  def addressCluster(
      regularInputs: Dataset[RegularInput],
      addressIds: Dataset[AddressId],
      removeCoinJoin: Boolean
  ) = {

    val inputIds = regularInputs
      .join(addressIds, Seq(F.address))
      .select(F.txId, F.addressId, F.coinjoin)

    // perform multiple-input clustering
    val addressCluster = plainAddressCluster(inputIds, removeCoinJoin)

    val singleAddressCluster = addressIds
      .select(F.addressId)
      .distinct
      .join(
        addressCluster.withColumnRenamed("id", F.addressId),
        Seq(F.addressId),
        "left_anti"
      )
      .withColumn(F.clusterId, col(F.addressId))

    singleAddressCluster
      .union(addressCluster)
      .join(addressIds, F.addressId)
      .select(F.addressId, F.clusterId)
      .as[AddressCluster]
  }

  def plainAddressRelations(
      inputs: Dataset[AddressTransaction],
      outputs: Dataset[AddressTransaction],
      regularInputs: Dataset[RegularInput],
      transactions: Dataset[Transaction]
  ): Dataset[PlainAddressRelation] = {

    val regularInputSum =
      regularInputs.groupBy(F.txId).agg(sum(F.value).as("regularSum"))
    val addressInputSum =
      inputs.groupBy(F.blockId, F.txId).agg(sum(F.value).as("addressSum"))
    val totalInput = transactions
      .withColumn("input", explode(col("inputs")))
      .select(F.txId, "input.value")
      .groupBy(F.txId)
      .agg(sum(F.value).as(F.totalInput))
    val reducedInputSum =
      addressInputSum
        .join(regularInputSum, F.txId)
        .join(totalInput, F.txId)
        .select(
          col(F.blockId),
          col(F.txId),
          // regularSum == addressSum, unless input address is used as output in same tx
          (col(F.totalInput) - col("regularSum") + col("addressSum"))
            .as(F.totalInput)
        )

    inputs
      .select(
        col(F.txId),
        col(F.addressId).as(F.srcAddressId),
        col(F.value).as("inValue")
      )
      .join(
        outputs.select(
          col(F.txId),
          col(F.addressId).as(F.dstAddressId),
          col(F.value).as("outValue")
        ),
        F.txId
      )
      .join(reducedInputSum, F.txId)
      .withColumn(
        F.estimatedValue,
        round(
          when(
            col(F.totalInput) =!= 0,
            col("inValue") / col(F.totalInput) * col("outValue")
          ).otherwise(lit(0))
        )
          .cast(LongType)
      )
      .drop(F.totalInput, "inValue", "outValue")
      .as[PlainAddressRelation]
  }

  def addressRelations(
      plainAddressRelations: Dataset[PlainAddressRelation],
      exchangeRates: Dataset[ExchangeRates],
      noFiatCurrencies: Int
  ): Dataset[AddressRelation] = {

    plainAddressRelations
      .join(exchangeRates, Seq(F.blockId), "left")
      .transform(
        toFiatCurrency(F.estimatedValue, F.fiatValues)
      )
      .drop(F.blockId)
      .groupBy(F.srcAddressId, F.dstAddressId)
      .agg(
        count(F.txId).cast(IntegerType).as(F.noTransactions),
        struct(
          sum(col(F.estimatedValue)).as(F.value),
          array(
            (0 until noFiatCurrencies)
              .map(i => sum(col(F.fiatValues).getItem(i)).cast(FloatType)): _*
          ).as(F.fiatValues)
        ).as(F.estimatedValue)
      )
      // add partitioning columns for outgoing addresses
      .transform(
        TransformHelpers
          .withIdGroup(F.srcAddressId, F.srcAddressIdGroup, bucketSize)
      )
      // add partitioning columns for incoming addresses
      .transform(
        TransformHelpers
          .withIdGroup(F.dstAddressId, F.dstAddressIdGroup, bucketSize)
      )
      .as[AddressRelation]
  }

  def plainClusterRelations(
      plainAddressRelations: Dataset[PlainAddressRelation],
      addressCluster: Dataset[AddressCluster]
  ) = {
    plainAddressRelations
      .join(
        addressCluster.select(
          col(F.addressId).as(F.srcAddressId),
          col(F.clusterId).as(F.srcClusterId)
        ),
        Seq(F.srcAddressId),
        "left"
      )
      .join(
        addressCluster.select(
          col(F.addressId).as(F.dstAddressId),
          col(F.clusterId).as(F.dstClusterId)
        ),
        Seq(F.dstAddressId),
        "left"
      )
      .groupBy(F.blockId, F.txId, F.srcClusterId, F.dstClusterId)
      .agg(sum(col(F.estimatedValue)).as(F.estimatedValue))
      .as[PlainClusterRelation]
  }

  def clusterRelations(
      plainClusterRelations: Dataset[PlainClusterRelation],
      exchangeRates: Dataset[ExchangeRates],
      noFiatCurrencies: Int
  ) = {

    plainClusterRelations
      .join(exchangeRates, Seq(F.blockId), "left")
      .transform(
        toFiatCurrency(F.estimatedValue, F.fiatValues)
      )
      .drop(F.blockId)
      .groupBy(F.srcClusterId, F.dstClusterId)
      .agg(
        count(F.txId).cast(IntegerType).as(F.noTransactions),
        struct(
          sum(
            when(
              col(F.srcClusterId) =!= col(F.dstClusterId),
              col(F.estimatedValue)
            )
          ).as(F.value),
          array(
            (0 until noFiatCurrencies)
              .map(i =>
                sum(
                  when(
                    col(F.srcClusterId) =!= col(F.dstClusterId),
                    col(F.fiatValues).getItem(i)
                  )
                ).cast(FloatType)
              ): _*
          ).as(F.fiatValues)
        ).as(F.estimatedValueAdj),
        struct(
          sum(col(F.estimatedValue)).as(F.value),
          array(
            (0 until noFiatCurrencies)
              .map(i => sum(col(F.fiatValues).getItem(i)).cast(FloatType)): _*
          ).as(F.fiatValues)
        ).as(F.estimatedValue)
      )
      // add partitioning columns for outgoing clusters
      .transform(
        TransformHelpers
          .withIdGroup(F.srcClusterId, F.srcClusterIdGroup, bucketSize)
      )
      // add partitioning columns for incoming clusters
      .transform(
        TransformHelpers
          .withIdGroup(F.dstClusterId, F.dstClusterIdGroup, bucketSize)
      )
      .as[ClusterRelationAdj]
  }
}
