package org.graphsense.common
import org.graphsense.TestBase
import org.graphsense.Util._
import org.apache.spark.SparkException
import org.graphsense.TransformHelpers
import org.apache.spark.sql.functions.{floor, udf}

class TxIdTest extends TestBase {

  test("Monotonic txid test") {
    val blks = Range.inclusive(1250000000, 1250010000)
    val index = Range.inclusive(10000000, 10001000)

    var last = 0L;
    for (n <- blks) {
      for (i <- index) {
        val monotonictx = computeMonotonicTxId(n, i)
        assert(last < monotonictx)
        val (v1, v2) = decomposeMontonicTxId(monotonictx)
        assert(v1 == n)
        assert(v2 == i)
        last = monotonictx
      }
    }

  }

  test("safe int conversion") {
    assertThrows[ArithmeticException] {
      toIntSafe(Int.MaxValue.toLong + 1)
    }
  }

  test("Decimal(38,0) overflow behavior") {
    import spark.implicits._
    val data = Seq(("Java", BigInt(10).pow(37), BigInt(10).pow(37)))
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF("name", "a", "b")

    assertThrows[SparkException] {

      spark.conf.set("spark.sql.ansi.enabled", true)

      df.withColumn("c", $"a" * $"b").show()

    }

    spark.conf.set("spark.sql.ansi.enabled", false)

    df
  }

  test("test group id behavior") {
    import spark.implicits._
    val root_cause = 178442263217569973L
    val batch_size = 10000
    /* PYTHON computed values
  [(9223372036854775807, 1566804069),
   (178442263217569973, -1362793124),
   (178442263217569972, -1362793124),
   (178442263217569971, -1362793124),
   (178442263217569974, -1362793124),
   (178442263217579972, -1362793123),
   (178442263217579973, -1362793123),
   (178442263217579974, -1362793123),
   (178442263217559972, -1362793125),
   (178442263217559973, -1362793125),
   (178442263217559974, -1362793125),
   (2147483647, 214748),
   (2147483648, 214748),
   (2147483646, 214748),
   (4294967295, 429496),
   (4294967296, 429496),
   (4294967294, 429496),
   (21474836470000, 2147483647),
   (21474836470001, 2147483647),
   (21474836469999, 2147483646),
   (21474836470002, 2147483647),
   (21474836469998, 2147483646),
   (42949672950000, -1),
   (42949672960000, 0),
   (42949672940000, -2),
   (1, 0),
   (1000, 0),
   (100000, 10)]*/
    val data = Seq(
      (Long.MaxValue, 1566804069),
      (root_cause, -1362793124),
      (root_cause - 1, -1362793124),
      (root_cause - 2, -1362793124),
      (root_cause + 1, -1362793124),
      (root_cause + batch_size - 1, -1362793123),
      (root_cause + batch_size, -1362793123),
      (root_cause + batch_size + 1, -1362793123),
      (root_cause - batch_size - 1, -1362793125),
      (root_cause - batch_size, -1362793125),
      (root_cause - batch_size + 1, -1362793125),
      (Int.MaxValue.toLong, 214748),
      (Int.MaxValue.toLong + 1, 214748),
      (Int.MaxValue.toLong - 1, 214748),
      (4294967295L, 429496),
      (4294967296L, 429496),
      (4294967294L, 429496),
      (Int.MaxValue.toLong * batch_size, 2147483647),
      (Int.MaxValue.toLong * batch_size + 1, 2147483647),
      (Int.MaxValue.toLong * batch_size - 1, 2147483646),
      (Int.MaxValue.toLong * batch_size + 2, 2147483647),
      (Int.MaxValue.toLong * batch_size - 2, 2147483646),
      (4294967295L * batch_size, -1),
      (4294967296L * batch_size, 0),
      (4294967294L * batch_size, -2),
      (1L, 0),
      (1000L, 0),
      (100000L, 10)
    )
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF("original", "ref_py")

    val blockid = udf((x: Long) => (x / batch_size).toInt)
    val divScala = udf((x: Long) => (x.toDouble / batch_size))
    val blockid2 =
      udf((x: Long) => (x.toDouble / batch_size).floor.toLong.toInt)
    val blockid_floor = udf((x: Long) => (x / batch_size))

    val df2 = df
      .transform(
        TransformHelpers.withIdGroup("original", "original_group", batch_size)
      )
      .withColumn("ref_int", blockid($"original"))
      .withColumn("ref_double", blockid2($"original"))
      .withColumn("div_spark", $"original" / batch_size)
      .withColumn("div_scala", divScala($"original"))
      .withColumn("floor_spark", floor($"original" / batch_size))
      .withColumn("floor_scala_int", blockid_floor($"original"))
      .withColumn(
        "ok",
        $"original_group" === $"ref_double"
      )

    assert(df2.filter($"ok" === false).count() === 0)

    df
  }

}
