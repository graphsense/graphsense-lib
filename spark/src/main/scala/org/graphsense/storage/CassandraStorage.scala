package org.graphsense.storage

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.graphsense.Util._
import scala.reflect.ClassTag
import com.datastax.spark.connector.cql.CassandraConnector

class CassandraStorage(
    spark: SparkSession,
    bulkWriter: Option[SidecarBulkWriter] = None
) {

  import spark.implicits._
  import com.datastax.spark.connector._

  def session(): SparkSession = {
    spark
  }

  def isTableEmpty(keyspace: String, tableName: String): Boolean = {
    CassandraConnector(spark.sparkContext).withSessionDo { session =>
      val test =
        session.execute(f"select * from ${keyspace}.${tableName} limit 1;")
      test.one() == null
    }
  }

  def tableExists(keyspace: String, tableName: String): Boolean = {
    CassandraConnector(spark.sparkContext).withSessionDo { session =>
      val result = session.execute(
        "select table_name from system_schema.tables " +
          s"where keyspace_name = '${keyspace}' " +
          s"and table_name = '${tableName}';"
      )
      result.one() != null
    }
  }

  def load[T <: Product: ClassTag: RowReaderFactory: ValidRDDType: Encoder](
      keyspace: String,
      tableName: String,
      columns: ColumnRef*
  ) = {
    spark.sparkContext.setJobDescription(s"Loading table ${tableName}")
    val table = spark.sparkContext.cassandraTable[T](keyspace, tableName)
    if (columns.isEmpty)
      table.toDS().as[T]
    else
      table.select(columns: _*).toDS().as[T]
  }

  /** Column names of a Cassandra table, read from system_schema. Used to
    * project a DataFrame onto the table columns before a bulk write.
    */
  def tableColumnNames(keyspace: String, tableName: String): Seq[String] = {
    import scala.collection.JavaConverters._
    CassandraConnector(spark.sparkContext).withSessionDo { session =>
      session
        .execute(
          "select column_name from system_schema.columns " +
            s"where keyspace_name = '${keyspace}' " +
            s"and table_name = '${tableName}';"
        )
        .all()
        .asScala
        .map(_.getString("column_name"))
        .toSeq
    }
  }

  /** Partition-key column names of a Cassandra table, in key order. The bulk
    * writer needs them to detect Cassandra partitions too large for its in-heap
    * partition accumulation.
    */
  def partitionKeyColumnNames(
      keyspace: String,
      tableName: String
  ): Seq[String] = {
    import scala.collection.JavaConverters._
    CassandraConnector(spark.sparkContext).withSessionDo { session =>
      session
        .execute(
          "select column_name, kind, position from system_schema.columns " +
            s"where keyspace_name = '${keyspace}' " +
            s"and table_name = '${tableName}';"
        )
        .all()
        .asScala
        .filter(_.getString("kind") == "partition_key")
        .sortBy(_.getInt("position"))
        .map(_.getString("column_name"))
        .toSeq
    }
  }

  def store[T <: Product: RowWriterFactory](
      keyspace: String,
      tableName: String,
      df: Dataset[T]
  ) = {
    val isEmpty = isTableEmpty(keyspace, tableName)
    spark.sparkContext.setJobDescription(
      s"Writing table ${tableName} - empty: ${isEmpty}"
    )
    time(s"Writing table ${tableName} - empty: ${isEmpty}") {
      bulkWriter match {
        case Some(writer) =>
          writer.write(
            keyspace,
            tableName,
            df.toDF,
            tableColumnNames(keyspace, tableName),
            partitionKeyColumnNames(keyspace, tableName)
          )
        case None => df.rdd.saveToCassandra(keyspace, tableName)
      }
    }
  }
}
