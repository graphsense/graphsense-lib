package org.graphsense.common
import org.graphsense.TestBase
import org.graphsense.TransformHelpers
import org.graphsense.account.models.Currency
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.functions.expr

case class blub(name: String, value: Currency)

class TransfromHelpersTest extends TestBase {

  test("zeroValueIfNull helper tests") {
    import spark.implicits._
    val data = Seq(
      ("Java", Currency(1, Seq(1, 2))),
      ("Java", Currency(1, null)),
      ("Java", Currency(null, null)),
      (
        "Java",
        Currency(null, Seq(1, 2))
      )
      // (
      //   "Java",
      //   Currency(null, Seq(1, null))
      // )
    )
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF("name", "value")

    df.transform(
      TransformHelpers.zeroCurrencyValueIfNull(
        "value",
        2,
        castValueTo = DecimalType(38, 0)
      )
    )

    assert(
      df.filter(
        $"value.value".isNull || $"value.fiatValues".isNull || expr(
          "exists(value.fiatValues, x -> x is null)"
        )
      ).count() == 3
    )

    val dfnew = df
      .transform(
        TransformHelpers.zeroCurrencyValueIfNullSafe(
          "value",
          2,
          castValueTo = DecimalType(38, 0)
        )
      )
      .as[blub]

    assert(
      dfnew
        .filter(
          $"value.value".isNull || $"value.fiatValues".isNull || expr(
            "exists(value.fiatValues, x -> x is null)"
          )
        )
        .count() == 0
    )

  }

}
