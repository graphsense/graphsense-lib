package org.graphsense.common

import org.apache.spark.sql.functions.col
import org.graphsense.TestBase
import org.graphsense.TransformHelpers

class SaturateToIntTest extends TestBase {

  import spark.implicits._

  test("saturateToInt caps at Int.MaxValue instead of wrapping") {
    // The middle two values are the ones a plain .cast(IntegerType) gets
    // wrong: Spark truncates two's complement in its default (non-ANSI)
    // mode, so they would land negative in an int column.
    // 3703869446 is the TRON USDT contract's real incoming-tx count; it was
    // stored as -591097850 (== 3703869446 - 2^32) before this fix.
    val counts = Seq(0L, 1L, 2147483646L, 2147483647L, 2147483648L, 3703869446L,
      4294967296L)

    val got = counts
      .toDF("c")
      .select(TransformHelpers.saturateToInt(col("c")).as("i"))
      .as[Int]
      .collect()
      .toSeq

    assert(
      got == Seq(0, 1, 2147483646, 2147483647, 2147483647, 2147483647,
        2147483647)
    )
    assert(got.forall(_ >= 0), s"saturateToInt produced a negative: $got")
  }

  test("saturateToInt still narrows to an int column") {
    val schema = Seq(5L)
      .toDF("c")
      .select(TransformHelpers.saturateToInt(col("c")).as("i"))
      .schema

    assert(schema("i").dataType.typeName == "integer")
  }

  test("a plain cast would wrap - guards the reason this helper exists") {
    import org.apache.spark.sql.types.IntegerType

    val wrapped = Seq(3703869446L)
      .toDF("c")
      .select(col("c").cast(IntegerType).as("i"))
      .as[Int]
      .head()

    assert(wrapped == -591097850)
  }
}
