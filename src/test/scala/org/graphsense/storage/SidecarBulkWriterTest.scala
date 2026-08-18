package org.graphsense.storage

import org.graphsense.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class SidecarBulkWriterTest extends TestBase {

  import SidecarBulkWriter._

  // Currency UDT analog: value is varint (DecimalType(38, 0)),
  // fiat_values is list<float>.
  private val currencyType = StructType(
    Seq(
      StructField("value", DecimalType(38, 0), true),
      StructField("fiat_values", ArrayType(FloatType, true), true)
    )
  )

  private def bd(s: String): java.math.BigDecimal = new java.math.BigDecimal(s)

  test("castVarintsToString: top-level varint column becomes String") {
    val schema = StructType(
      Seq(
        StructField("id", LongType, false),
        StructField("amount", DecimalType(38, 0), true)
      )
    )
    val data = Seq(Row(1L, bd("1000000000000000000")))
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val out = castVarintsToString(df)
    assert(out.schema("amount").dataType == StringType)
    assert(out.schema("id").dataType == LongType)
    assert(out.collect()(0).getString(1) == "1000000000000000000")
  }

  test("castVarintsToString: varint inside a UDT struct becomes String") {
    val schema = StructType(
      Seq(
        StructField("id", LongType, false),
        StructField("total_received", currencyType, true)
      )
    )
    val data =
      Seq(Row(1L, Row(bd("123456789012345678901234567890"), Array(0.5f, 1.5f))))
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val out = castVarintsToString(df)
    val inner = out.schema("total_received").dataType.asInstanceOf[StructType]
    assert(inner("value").dataType == StringType)
    assert(inner("fiat_values").dataType == ArrayType(FloatType, true))
    val received = out.collect()(0).getStruct(1)
    assert(received.getString(0) == "123456789012345678901234567890")
    assert(received.getSeq[Float](1) == Seq(0.5f, 1.5f))
  }

  test("castVarintsToString: varint inside Map<text, UDT> values converted") {
    val schema = StructType(
      Seq(
        StructField(
          "total_tokens_received",
          MapType(StringType, currencyType, true),
          true
        )
      )
    )
    val data = Seq(
      Row(Map("USDT" -> Row(bd("100"), Array(1.0f, 2.0f))))
    )
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val out = castVarintsToString(df)
    val mapped =
      out.schema("total_tokens_received").dataType.asInstanceOf[MapType]
    val udt = mapped.valueType.asInstanceOf[StructType]
    assert(udt("value").dataType == StringType)
    val value =
      out.collect()(0).getMap[String, Row](0)("USDT").getString(0)
    assert(value == "100")
  }

  test("castVarintsToString: varint inside Array<UDT> elements converted") {
    val schema = StructType(
      Seq(StructField("history", ArrayType(currencyType, true), true))
    )
    val data = Seq(
      Row(Array(Row(bd("7"), Array(1.0f)), Row(bd("8"), Array(2.0f))))
    )
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val out = castVarintsToString(df)
    val arr = out.schema("history").dataType.asInstanceOf[ArrayType]
    val udt = arr.elementType.asInstanceOf[StructType]
    assert(udt("value").dataType == StringType)
    val elems = out.collect()(0).getSeq[Row](0)
    assert(elems.map(_.getString(0)) == Seq("7", "8"))
  }

  test("castVarintsToString: non-varint DecimalType left untouched") {
    // A genuine decimal(10, 2) is NOT varint — must pass through unchanged.
    val schema = StructType(
      Seq(StructField("price", DecimalType(10, 2), true))
    )
    val data = Seq(Row(bd("12.34")))
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val out = castVarintsToString(df)
    assert(out.schema("price").dataType == DecimalType(10, 2))
  }

  test("castVarintsToString: null UDT stays null, no NPE") {
    val schema = StructType(
      Seq(
        StructField("id", LongType, false),
        StructField("total_received", currencyType, true)
      )
    )
    val data = Seq(Row(1L, null))
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val out = castVarintsToString(df)
    assert(out.collect()(0).isNullAt(1))
  }

  test("castVarintsToString: DataFrame with no varint passes through") {
    import spark.implicits._
    val df = Seq((1, "a"), (2, "b")).toDF("id", "label")
    val out = castVarintsToString(df)
    assert(out.schema == df.schema)
    assert(
      out.collect().map(r => (r.getInt(0), r.getString(1))).toSet ==
        Set((1, "a"), (2, "b"))
    )
  }

  private val maxRowsConf = "spark.graphsense.sidecar.maxRowsPerPartition"

  private def writerForTest: SidecarBulkWriter =
    new SidecarBulkWriter("unused:9043", "dc1", "LOCAL_QUORUM")

  // (address_id_group, address_id_secondary_group)-shaped data: partition
  // (1, 10) holds 3 rows, (1, 20) and (2, 10) hold 1 each.
  private def partitionedDf = {
    import spark.implicits._
    Seq(
      (1, 10, 100L),
      (1, 10, 101L),
      (1, 10, 102L),
      (1, 20, 103L),
      (2, 10, 104L)
    ).toDF("address_id_group", "address_id_secondary_group", "transaction_id")
  }

  test("oversizedPartitionKeys: finds partitions above the threshold") {
    spark.conf.set(maxRowsConf, "2")
    try {
      val keys = writerForTest.oversizedPartitionKeys(
        partitionedDf,
        "address_transactions",
        Seq("address_id_group", "address_id_secondary_group")
      )
      assert(keys.isDefined)
      val rows = keys.get
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toSet
      assert(rows == Set((1, 10)))
      keys.get.unpersist()
    } finally {
      spark.conf.unset(maxRowsConf)
    }
  }

  test("oversizedPartitionKeys: None when all partitions fit") {
    spark.conf.set(maxRowsConf, "3")
    try {
      val keys = writerForTest.oversizedPartitionKeys(
        partitionedDf,
        "address_transactions",
        Seq("address_id_group", "address_id_secondary_group")
      )
      assert(keys.isEmpty)
    } finally {
      spark.conf.unset(maxRowsConf)
    }
  }

  test("oversizedPartitionKeys: threshold <= 0 disables the check") {
    spark.conf.set(maxRowsConf, "0")
    try {
      val keys = writerForTest.oversizedPartitionKeys(
        partitionedDf,
        "address_transactions",
        Seq("address_id_group", "address_id_secondary_group")
      )
      assert(keys.isEmpty)
    } finally {
      spark.conf.unset(maxRowsConf)
    }
  }

  test(
    "oversizedPartitionKeys: semi/anti joins split the rows exactly"
  ) {
    import org.apache.spark.sql.functions.broadcast
    spark.conf.set(maxRowsConf, "2")
    try {
      val df = partitionedDf
      val pk = Seq("address_id_group", "address_id_secondary_group")
      val keys = writerForTest
        .oversizedPartitionKeys(df, "address_transactions", pk)
        .get
      val bulkPart = df.join(broadcast(keys), pk, "left_anti")
      val cqlPart = df.join(broadcast(keys), pk, "left_semi")
      // schemas unchanged by the split (no `count` column leaks through)
      assert(bulkPart.schema == df.schema)
      assert(cqlPart.schema == df.schema)
      assert(
        cqlPart.collect().map(_.getLong(2)).toSet == Set(100L, 101L, 102L)
      )
      assert(bulkPart.collect().map(_.getLong(2)).toSet == Set(103L, 104L))
      keys.unpersist()
    } finally {
      spark.conf.unset(maxRowsConf)
    }
  }

  test(
    "alignToSchema: projects to table columns, snake_cases UDT fields, casts varint"
  ) {
    val currencyCamel = StructType(
      Seq(
        StructField("value", DecimalType(38, 0), true),
        StructField("fiatValues", ArrayType(FloatType, true), true)
      )
    )
    val schema = StructType(
      Seq(
        StructField("addressId", LongType, false),
        StructField("totalReceived", currencyCamel, true),
        StructField("extraField", StringType, true)
      )
    )
    val data = Seq(Row(1L, Row(bd("999"), Array(1.0f)), "drop me"))
    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val out = alignToSchema(df, Seq("address_id", "total_received"))

    // extra column dropped; top-level columns are exact table names
    assert(out.schema.fieldNames.toSeq == Seq("address_id", "total_received"))
    // nested UDT field snake_cased and varint -> String
    val udt = out.schema("total_received").dataType.asInstanceOf[StructType]
    assert(udt.fieldNames.toSeq == Seq("value", "fiat_values"))
    assert(udt("value").dataType == StringType)
    // data intact
    assert(out.collect()(0).getStruct(1).getString(0) == "999")
  }
}
