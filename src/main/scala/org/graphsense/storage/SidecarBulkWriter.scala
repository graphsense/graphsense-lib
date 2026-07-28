package org.graphsense.storage

import org.apache.cassandra.spark.KryoRegister
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{
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

  def write(
      keyspace: String,
      table: String,
      df: DataFrame,
      tableColumns: Seq[String]
  ): Unit = {
    SidecarBulkWriter
      .alignToSchema(df, tableColumns)
      .write
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
  def alignToSchema(df: DataFrame, tableColumns: Seq[String]): DataFrame = {
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
    castVarintsToString(renameStructFields(df.select(projection: _*)))
  }

  /** Recursively rename nested struct (UDT) fields to snake_case. Top-level
    * columns are already exact table column names; snake-casing an already
    * snake_case name is a no-op, so they are left unchanged.
    */
  private def renameStructFields(df: DataFrame): DataFrame = {
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
