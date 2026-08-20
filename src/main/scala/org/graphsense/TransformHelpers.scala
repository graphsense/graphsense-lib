package org.graphsense

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  floor,
  hex,
  lit,
  map_keys,
  max,
  row_number,
  struct,
  substring,
  sum,
  typedLit,
  when
}
import org.apache.spark.sql.types.{DataType, FloatType, IntegerType}
import org.graphsense.models.ExchangeRatesRaw
import org.apache.spark.sql.AnalysisException
import org.graphsense.Util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.Path

object TransformHelpers {

  /* --- write-completion markers -------------------------------------------
   *
   * The job's "should I write this table" guards ask whether the TARGET table
   * is empty, which cannot distinguish "fully written" from "half written and
   * the run died": a retry silently skips a partial table and leaves the
   * keyspace corrupt (this cost the 2026-07 TRX full transform several runs).
   * A marker file per table, written only after `store()` returned, records
   * completion explicitly. Markers live in the gs-cache directory next to the
   * cached datasets — the cache is already the per-target-keyspace retry
   * state (see the pinned block range and job config), and deleting it resets
   * everything together. Without a cache directory there is nowhere to keep
   * them and the guards fall back to the emptiness check.
   */

  private def writeMarkerDir(basePath: String): Path =
    new Path(basePath, "_written")

  private def writeMarker(basePath: String, table: String): Path =
    new Path(writeMarkerDir(basePath), table)

  private def exists(spark: SparkSession, path: Path): Boolean =
    path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)

  /** Whether this cache already tracks write completion. Captured ONCE before a
    * run writes anything: if markers were not in use yet, tables already
    * holding data predate them and are adopted as complete (that is the
    * pre-marker behaviour); if they were, a table without a marker is a partial
    * write and must be redone.
    */
  def writeMarkersInitialized(
      basePath: Option[String],
      spark: SparkSession
  ): Boolean =
    basePath.exists(base => exists(spark, writeMarkerDir(base)))

  def initWriteMarkers(basePath: Option[String], spark: SparkSession): Unit =
    basePath.foreach { base =>
      val dir = writeMarkerDir(base)
      dir.getFileSystem(spark.sparkContext.hadoopConfiguration).mkdirs(dir)
    }

  def isWriteComplete(
      basePath: Option[String],
      spark: SparkSession,
      table: String
  ): Boolean =
    basePath.exists(base => exists(spark, writeMarker(base, table)))

  def markWriteComplete(
      basePath: Option[String],
      spark: SparkSession,
      table: String
  ): Unit =
    basePath.foreach { base =>
      val marker = writeMarker(base, table)
      val fs = marker.getFileSystem(spark.sparkContext.hadoopConfiguration)
      fs.create(marker, true).close()
    }

  def toDSEager[
      R: Encoder
  ](ds: => DataFrame): Dataset[R] = {
    // https://stackoverflow.com/questions/70049444/spark-dataframe-as-function-does-not-drop-columns-not-present-in-matched-case
    // ds.as[R].map(identity)
    ds.as[R]
  }

  def namedCache[T](
      name: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  )(df: Dataset[T]): Dataset[T] = {
    df.sparkSession.sharedState.cacheManager
      .cacheQuery(df, Some(name), storageLevel)
    df
  }

  def computeCached[
      R: Encoder
  ](base_path: Option[String], spark: SparkSession, overwrite: Boolean = true)(
      dataset_name: String
  )(block: => Dataset[R]): Dataset[R] = {
    base_path match {
      case Some(path) => {
        val path_complete = path + "/" + dataset_name
        try {
          val df_loaded =
            time(f"Try Reading cached dataset ${path_complete} from parquet") {
              spark.read.parquet(path_complete)
            }
          return df_loaded.as[R]
        } catch {
          case e: AnalysisException => {
            println(
              f"Warn - Could not load cached dataset ${path_complete}: " + e
            )
            val df =
              namedCache(dataset_name)(time(f"Computing ${path_complete}") {
                block
              })

            val writeMode = overwrite match {
              case true  => "overwrite"
              case false => "error"
            }

            time(f"Writing cache dataset at ${path_complete} as parquet") {
              df.write.mode(writeMode).parquet(path_complete)
            }

            return df
          }
        }
      }
      case None => {
        val df = namedCache(dataset_name)(
          time(f"Computing ${dataset_name} (gs-cache-dir not set)") {
            block
          }
        )
        df
      }
    }
  }

  def filterBlockRange[T](
      start: Option[Int],
      end: Option[Int],
      blockIdCol: String = "blockId"
  )(ds: Dataset[T]): Dataset[T] = {
    (start.getOrElse(0), end) match {
      case (minBlock, Some(maxBlock)) =>
        ds.filter(
          col(blockIdCol) >= minBlock && col(blockIdCol) <= maxBlock
        )
      case (minBlock, None) =>
        ds.filter(
          col(blockIdCol) >= minBlock
        )
    }

  }

  def getFiatCurrencies(
      exchangeRatesRaw: Dataset[ExchangeRatesRaw]
  ): Seq[String] = {
    val currencies =
      exchangeRatesRaw.select(map_keys(col("fiatValues"))).distinct
    if (currencies.count() > 1L)
      throw new Exception("Non-unique map keys in raw exchange rates table")
    else if (currencies.count() == 0L)
      throw new Exception(
        "No fiat currencies found. Exchange rates table might be empty."
      )
    currencies.rdd.map(r => r(0).asInstanceOf[Seq[String]]).collect()(0)
  }

  def getZeroCurrencyValue(
      length: Int,
      castValueTo: DataType = IntegerType
  ) = {
    struct(
      lit(0).cast(castValueTo).as("value"),
      typedLit(Array.fill[Float](length)(0))
        .as("fiatValues")
    )
  }

  def zeroCurrencyValueIfNull(
      columnName: String,
      length: Int,
      castValueTo: DataType = IntegerType
  )(
      df: DataFrame
  ): DataFrame = {
    df.withColumn(
      columnName,
      coalesce(
        col(columnName),
        struct(
          lit(0).cast(castValueTo).as("value"),
          typedLit(Array.fill[Float](length)(0))
            .as("fiatValues")
        )
      )
    )
  }

  def zeroCurrencyValueIfNullSafe(
      columnName: String,
      length: Int,
      castValueTo: DataType = IntegerType
  )(
      df: DataFrame
  ): DataFrame = {
    val zero = getZeroCurrencyValue(length, castValueTo)
    df.withColumn(
      columnName,
      when(
        col(f"${columnName}.value").isNull || col(
          f"${columnName}.fiatValues"
        ).isNull,
        zero
      )
        .otherwise(
          coalesce(
            col(columnName),
            zero
          )
        )
    )
  }

  def aggregateValues(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int,
      groupColumns: String*
  )(df: DataFrame): DataFrame = {
    df.groupBy(groupColumns.head, groupColumns.tail: _*)
      .agg(
        createAggCurrencyStruct(valueColumn, fiatValueColumn, length)
      )
  }

  def createAggCurrencyStruct(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int
  ): Column = {
    struct(
      sum(col(valueColumn)).as(valueColumn),
      array(
        (0 until length)
          .map(i => sum(col(fiatValueColumn).getItem(i)).cast(FloatType)): _*
      ).as(fiatValueColumn)
    ).as(valueColumn)
  }

  def createAggCurrencyStructPerCurrency(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int
  ): Column = {
    struct(
      col("currency"),
      createAggCurrencyStruct(valueColumn, fiatValueColumn, length)
    )
  }

  def withIdGroup[T](
      idColumn: String,
      idGroupColumn: String,
      size: Int
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(idGroupColumn, floor(col(idColumn) / size).cast("int"))
  }

  def withSortedIdGroup[T: Encoder](
      idColumn: String,
      idGroupColumn: String,
      size: Int
  )(df: DataFrame): Dataset[T] = {
    df.transform(withIdGroup(idColumn, idGroupColumn, size))
      .as[T]
      .sort(idGroupColumn)
  }

  def withPrefix[T](
      hashColumn: String,
      hashPrefixColumn: String,
      length: Int = 4
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(hashPrefixColumn, substring(hex(col(hashColumn)), 0, length))
  }

  def withSortedPrefix[T: Encoder](
      hashColumn: String,
      prefixColumn: String,
      length: Int = 4
  )(df: DataFrame): Dataset[T] = {
    df.transform(withPrefix(hashColumn, prefixColumn, length))
      .as[T]
      .sort(prefixColumn)
  }

  def withTxReference[T](ds: Dataset[T]): DataFrame = {
    ds.withColumn(
      "txReference",
      struct(
        col("traceIndex"),
        col("logIndex")
      )
    )
  }

  def withSecondaryIdGroup[T](
      idColumn: String,
      secondaryIdColumn: String,
      windowOrderColumn: String,
      skewedPartitionFactor: Float = 2.5f
  )(ds: Dataset[T]): DataFrame = {
    val partitionSize =
      ds.select(col(idColumn)).groupBy(idColumn).count().persist()
    val noPartitions = partitionSize.count()
    val approxMedian = partitionSize
      .sort(col("count").asc)
      .select(col("count"))
      .rdd
      .zipWithIndex
      .filter(_._2 == noPartitions / 2)
      .map(_._1)
      .first()
      .getLong(0)
    val window = Window.partitionBy(idColumn).orderBy(windowOrderColumn)
    ds.withColumn(
      secondaryIdColumn,
      floor(
        row_number().over(window) / (approxMedian * skewedPartitionFactor)
      ).cast(IntegerType)
    )
  }

  def withSecondaryIdGroupApprox[T](
      idColumn: String,
      secondaryIdColumn: String,
      windowOrderColumn: String,
      skewedPartitionFactor: Float = 2.5f
  )(ds: Dataset[T]): DataFrame = {
    val approxMedian =
      ds.select(col(idColumn))
        .groupBy(idColumn)
        .count()
        .stat
        .approxQuantile("count", Array(0.5), 0.1)(0)
    val window = Window.partitionBy(idColumn).orderBy(windowOrderColumn)
    ds.withColumn(
      secondaryIdColumn,
      floor(
        row_number().over(window) / (approxMedian * skewedPartitionFactor)
      ).cast(IntegerType)
    )
  }

  def withSecondaryIdGroupSimpleAddress[T](
      windowOrderColumn: String,
      secondaryIdColumn: String,
      buckets: Int
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(
      secondaryIdColumn,
      floor(
        col(windowOrderColumn) % buckets
      ).cast(IntegerType)
    )
  }

  def withSecondaryIdGroupSimpleAddressTransaction[T](
      secondaryIdColumn: String,
      bucket_size: Int
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(
      secondaryIdColumn,
      floor(
        col("blockId") / bucket_size
      ).cast(IntegerType)
    )
  }

  def computeSecondaryPartitionIdLookup[T: Encoder](
      df: DataFrame,
      primaryPartitionColumn: String,
      secondaryPartitionColumn: String
  ): Dataset[T] = {
    df.groupBy(primaryPartitionColumn)
      .agg(max(secondaryPartitionColumn).as("maxSecondaryId"))
      // to save storage space, store only records with multiple secondary IDs
      .filter(col("maxSecondaryId") > 0)
      .sort(primaryPartitionColumn)
      .as[T]
  }

}
