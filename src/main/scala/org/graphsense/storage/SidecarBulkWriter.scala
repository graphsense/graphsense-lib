package org.graphsense.storage

import org.apache.cassandra.spark.KryoRegister
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
  broadcast,
  col,
  lit,
  struct,
  transform,
  transform_values,
  when
}
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  DecimalType,
  MapType,
  StringType,
  StructType
}

/** Writes a transformed table by generating SSTables on the Spark executors and
  * streaming them into Cassandra through the Cassandra Sidecar, using the
  * cassandra-analytics bulk-writer data source. This bypasses the CQL
  * coordinator / commitlog / memtable write path that the default
  * [[CassandraStorage]] connector write uses.
  */
class SidecarBulkWriter(
    sidecarContactPoints: String,
    localDc: String,
    consistencyLevel: String
) {

  /** Write `df` through the bulk writer, except for oversized Cassandra
    * partitions, which are routed through the plain Spark Cassandra connector
    * (CQL write path) instead.
    *
    * cassandra-analytics 0.3.0's sorted-mode CQLSSTableWriter accumulates each
    * whole Cassandra partition as an in-heap PartitionUpdate BTree before
    * appending it to the sstable, and a partition key hashes to a single token,
    * so no split count can divide it across upload tasks. A partition beyond a
    * few hundred million rows therefore OOMs its executor no matter how the
    * write is tuned (observed on TRX `address_transactions`, 2026-07: two
    * burst-shaped ~455M-row partitions killed 160g executors across three runs,
    * surviving both a 9x smaller block bucket and 4x more splits). The CQL path
    * streams rows without materializing a partition, so it handles any
    * partition size -- just slower, which is fine for the tiny fraction of
    * pathological partitions.
    */
  def write(
      keyspace: String,
      table: String,
      df: DataFrame,
      tableColumns: Seq[String],
      partitionKeyColumns: Seq[String]
  ): Unit = {
    // Two-step alignment: the varint cast stays a pure Catalyst projection, so
    // the oversized-partition detection on top of it prunes down to just the
    // partition-key columns at the source; renameStructFields goes through an
    // RDD roundtrip that would block that pruning, so it is applied last.
    val projected = SidecarBulkWriter.castVarintsToString(
      SidecarBulkWriter.projectToTableColumns(df, tableColumns)
    )
    val aligned = SidecarBulkWriter.renameStructFields(projected)
    oversizedPartitionKeys(projected, table, partitionKeyColumns) match {
      case None => bulkWrite(keyspace, table, aligned)
      case Some(keys) =>
        bulkWrite(
          keyspace,
          table,
          aligned.join(broadcast(keys), partitionKeyColumns, "left_anti")
        )
        cqlWrite(
          keyspace,
          table,
          aligned.join(broadcast(keys), partitionKeyColumns, "left_semi")
        )
        keys.unpersist()
    }
  }

  private def bulkWrite(
      keyspace: String,
      table: String,
      df: DataFrame
  ): Unit = {
    df.write
      .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
      .option("sidecar_contact_points", sidecarContactPoints)
      .option("keyspace", keyspace)
      .option("table", table)
      .option("local_dc", localDc)
      .option("bulk_writer_cl", consistencyLevel)
      .option("number_splits", numberSplits(df, table).toString)
      .option("data_transport", "DIRECT")
      .mode("append")
      .save()
  }

  // CQL fallback for oversized partitions. The connector's TypeConverters
  // accept the String form castVarintsToString produced for varint columns
  // (top-level and inside UDTs), and match UDT fields by their snake_case
  // names, so the same aligned DataFrame works on both write paths.
  private def cqlWrite(keyspace: String, table: String, df: DataFrame): Unit = {
    df.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", keyspace)
      .option("table", table)
      .option("spark.cassandra.output.consistency.level", consistencyLevel)
      .mode("append")
      .save()
  }

  /** Partition keys of `df` holding more rows than
    * `spark.graphsense.sidecar.maxRowsPerPartition` (default 50M; <= 0 disables
    * the check), as a persisted DataFrame keyed by the partition-key columns,
    * or None if there are none. 50M keeps a wide margin below the observed
    * bulk-writer ceiling (~300M rows per task on 160g executors) while only
    * rerouting partitions that are already far beyond Cassandra's own sizing
    * guidance. Costs one extra aggregation job over the partition-key columns
    * only -- a cheap column-pruned scan when the store is fed from a
    * computeCached parquet, as all large tables are.
    */
  private[storage] def oversizedPartitionKeys(
      df: DataFrame,
      table: String,
      partitionKeyColumns: Seq[String]
  ): Option[DataFrame] = {
    val maxRows = df.sparkSession.conf
      .get("spark.graphsense.sidecar.maxRowsPerPartition", "50000000")
      .toLong
    if (maxRows <= 0 || partitionKeyColumns.isEmpty) {
      None
    } else {
      val counts = df
        .groupBy(partitionKeyColumns.map(col): _*)
        .count()
        .filter(col("count") > maxRows)
        .persist()
      val oversized = counts.collect()
      if (oversized.isEmpty) {
        counts.unpersist()
        None
      } else {
        oversized.foreach { row =>
          val key = partitionKeyColumns.zipWithIndex
            .map { case (name, i) => s"${name}=${row.get(i)}" }
            .mkString(", ")
          val rows = row.getLong(partitionKeyColumns.length)
          println(
            s"Table ${table}: partition (${key}) holds ${rows} rows " +
              s"(> ${maxRows}), writing it via the CQL connector instead " +
              "of the sidecar bulk writer"
          )
        }
        // Returned with its `count` column: left_semi/left_anti joins only
        // emit left-side columns, and unpersist() must see the exact plan
        // that was persisted.
        Some(counts)
      }
    }
  }

  /** Upload tasks per ring token range for `table`, from
    * `spark.graphsense.sidecar.splits.<table>`, falling back to
    * `spark.graphsense.sidecar.splits.default` (1). Total upload tasks = splits
    * * number of ring token ranges.
    *
    * Splits must be sized per table, not globally: token ranges are unequal in
    * data, so large tables need several splits per range to keep the biggest
    * range's share small enough to sort in executor memory. But
    * cassandra-analytics 0.3.0 builds a sidecar client (a Vertx/netty
    * event-loop group) per upload task and never closes it on the executor, so
    * every task permanently costs the executor JVM file descriptors -- a
    * blanket high split count (or number_splits=-1, which derives splits from
    * spark.default.parallelism) exhausts the executor's fd limit.
    */
  private def numberSplits(df: DataFrame, table: String): Int = {
    val conf = df.sparkSession.conf
    val default = conf.get("spark.graphsense.sidecar.splits.default", "1")
    conf.get(s"spark.graphsense.sidecar.splits.${table}", default).toInt
  }
}

object SidecarBulkWriter {

  private def isSidecar(writer: String): Boolean =
    writer.equalsIgnoreCase("sidecar")

  /** A SparkConf with the cassandra-analytics bulk-writer setup applied (Kryo
    * registration and bulk write settings) when `--writer=sidecar` is selected.
    * Must be passed to the SparkSession builder before the session is created.
    */
  def sparkConf(writer: String): SparkConf = {
    val conf = new SparkConf()
    if (isSidecar(writer)) {
      BulkSparkConf.setupSparkConf(conf, true)
      KryoRegister.setup(conf)
    }
    conf
  }

  /** A [[SidecarBulkWriter]] when `--writer=sidecar`, or None for the default
    * Cassandra connector write path. Fails when a required sidecar option is
    * missing.
    */
  def forWriter(
      writer: String,
      contactPoints: Option[String],
      localDc: Option[String],
      consistencyLevel: String
  ): Option[SidecarBulkWriter] = {
    if (!isSidecar(writer)) {
      None
    } else {
      def required(value: Option[String], option: String): String =
        value.getOrElse(
          throw new IllegalArgumentException(
            s"$option is required when --writer=sidecar"
          )
        )
      Some(
        new SidecarBulkWriter(
          required(contactPoints, "--sidecar-contact-points"),
          required(localDc, "--sidecar-local-dc"),
          consistencyLevel
        )
      )
    }
  }

  /** Project a transformed DataFrame onto the target Cassandra table.
    *
    * GraphSense case classes use camelCase field names and, for several tables
    * (e.g. `address_incoming_relations`, `address_transactions`), carry more
    * fields than the table has columns -- the Spark Cassandra connector
    * silently wrote only the table's columns. cassandra-analytics instead
    * matches DataFrame columns to Cassandra columns by exact name, so this:
    *   - keeps only the columns present in the target table (dropping extras),
    *   - renames each to its exact Cassandra column name, matched by the
    *     connector's convention (equal once lowercased with underscores
    *     removed) -- this also covers irregular names such as `bech_32_prefix`,
    *   - renames nested UDT struct fields to snake_case,
    *   - casts varint columns to String (see [[castVarintsToString]]).
    *
    * Fails loudly if a table column has no matching DataFrame field.
    */
  def alignToSchema(df: DataFrame, tableColumns: Seq[String]): DataFrame =
    renameStructFields(
      castVarintsToString(projectToTableColumns(df, tableColumns))
    )

  /** The projection step of [[alignToSchema]]: keep only the table's columns,
    * each renamed to its exact Cassandra column name. Kept separate because it
    * is a pure Catalyst projection -- computations stacked on its result (the
    * oversized-partition detection in [[SidecarBulkWriter.write]]) still prune
    * columns at the source, which [[renameStructFields]]'s RDD roundtrip would
    * prevent.
    */
  private[storage] def projectToTableColumns(
      df: DataFrame,
      tableColumns: Seq[String]
  ): DataFrame = {
    def normalize(name: String): String =
      name.toLowerCase.replace("_", "")

    val fieldByNormalizedName =
      df.schema.fields.map(f => normalize(f.name) -> f.name).toMap

    val projection = tableColumns.map { column =>
      fieldByNormalizedName.get(normalize(column)) match {
        case Some(field) => col(field).as(column)
        case None =>
          throw new IllegalArgumentException(
            s"No DataFrame column matches Cassandra column '$column'"
          )
      }
    }
    df.select(projection: _*)
  }

  /** Recursively rename nested struct (UDT) fields to snake_case. Top-level
    * columns are already exact table column names; snake-casing an already
    * snake_case name is a no-op, so they are left unchanged.
    */
  private[storage] def renameStructFields(df: DataFrame): DataFrame = {
    def snake(name: String): String =
      name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase

    def renameType(dataType: DataType): DataType =
      dataType match {
        case st: StructType =>
          StructType(
            st.map(f =>
              f.copy(name = snake(f.name), dataType = renameType(f.dataType))
            )
          )
        case ArrayType(elementType, containsNull) =>
          ArrayType(renameType(elementType), containsNull)
        case MapType(keyType, valueType, valueContainsNull) =>
          MapType(keyType, renameType(valueType), valueContainsNull)
        case other => other
      }

    val renamedSchema =
      StructType(
        df.schema.map(f =>
          f.copy(name = snake(f.name), dataType = renameType(f.dataType))
        )
      )
    df.sparkSession.createDataFrame(df.rdd, renamedSchema)
  }

  /** Cast every varint column -- top-level or nested inside a UDT, Map, or
    * Array -- to String.
    *
    * cassandra-analytics 0.3.0's BigIntegerConverter rejects
    * java.math.BigDecimal, the JVM type Spark's DecimalType emits at row-read
    * time, so varint must reach the bulk writer as String; the converter then
    * parses it back to a BigInteger. No-op for DataFrames without varint.
    */
  def castVarintsToString(df: DataFrame): DataFrame = {
    val projection = df.schema.fields.map(f =>
      castVarintsExpr(df(f.name), f.dataType).as(f.name)
    )
    df.select(projection: _*)
  }

  // spark-cassandra-connector maps Cassandra `varint` to Spark
  // DecimalType(38, 0). Regular `decimal` columns carry their actual
  // precision/scale, so this isolates varint from decimal cleanly.
  private def isVarintType(dataType: DataType): Boolean =
    dataType match {
      case d: DecimalType => d.precision == 38 && d.scale == 0
      case _              => false
    }

  // Recursively rebuild `column` so any varint-shaped DecimalType becomes a
  // String, descending into structs, arrays, and maps.
  private def castVarintsExpr(column: Column, dataType: DataType): Column =
    dataType match {
      case d if isVarintType(d) =>
        column.cast(StringType)
      case st: StructType =>
        val fields = st.fields.map(f =>
          castVarintsExpr(column.getField(f.name), f.dataType).as(f.name)
        )
        when(column.isNull, lit(null)).otherwise(struct(fields: _*))
      case ArrayType(elementType, _) =>
        transform(column, x => castVarintsExpr(x, elementType))
      case MapType(_, valueType, _) =>
        transform_values(column, (_, v) => castVarintsExpr(v, valueType))
      case _ =>
        column
    }
}
