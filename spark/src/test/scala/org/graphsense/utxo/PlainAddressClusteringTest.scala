package org.graphsense.utxo

import org.graphsense.TestBase

class PlainAddressClusteringTest extends TestBase {

  val bucketSize: Int = 2
  val t = new Transformator(spark, bucketSize)

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.setCheckpointDir("file:///tmp/spark-checkpoint")

  test(
    "single-input addresses that appear in only one transaction are not clustered"
  ) {
    import spark.implicits._
    val tx = Seq((666L, 1, false), (888L, 4, false))
      .toDF("txId", "addressId", "coinJoin")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true).sort("id")
    assert(clusters.isEmpty)
  }

  test(
    "each address is in only one transaction, but multiple-input addresses present"
  ) {
    import spark.implicits._
    val tx =
      Seq(
        (100L, 1, false), // lowest id, will become clusterId
        (100L, 2, false),
        (404L, 3, false)
      ) // addr 3 will not be clustered because it's not multiple-input
        .toDF("txId", "addressId", "coinJoin")

    val expected = Seq(
      (1, 1),
      (2, 1)
    ).toDF("id", "clusterId")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true).sort("id")
    assertDataFrameEquality(clusters, expected)
  }

  test("multiple tx for address 2, all addresses are multiple-input") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false), // addr 2 appears in most transactions
      (404L, 2, false),
      (404L, 3, false)
    ).toDF("txId", "addressId", "coinJoin")

    val expected = Seq(
      // addressId, clusterId
      (1, 2),
      (2, 2),
      (3, 2)
    ).toDF("id", "clusterId")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true).sort("id")
    assertDataFrameEquality(clusters, expected)
  }

  test("multiple tx for address 2 and 3, all addresses are multiple-input") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false),
      (100L, 3, false),
      (404L, 2, false),
      (404L, 3, false),
      (404L, 4, false)
    ).toDF("txId", "addressId", "coinJoin")

    val expected = Seq(
      // addressId, clusterId
      (1, 2),
      (2, 2),
      (3, 2),
      (4, 2)
    ).toDF("id", "clusterId")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true).sort("id")
    assertDataFrameEquality(clusters, expected)
  }

  test("three tx, two tx share address input, two clusters are found") {
    import spark.implicits._
    val tx = Seq(
      (
        100L,
        2,
        false
      ), // address 2 appears in most transactions in this cluster
      (100L, 3, false),
      (404L, 1, false),
      (404L, 2, false),
      (770L, 10, false), // separate cluster with id = min(10, 11)
      (770L, 11, false)
    ).toDF("txId", "addressId", "coinJoin")

    val expected = Seq(
      (1, 2),
      (2, 2),
      (3, 2),
      (10, 10),
      (11, 10)
    ).toDF("id", "clusterId")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true).sort("id")
    assertDataFrameEquality(clusters, expected)
  }

  test("addresses 2 and 3 in same # of tx, min ID is chosen as cluster ID") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false),
      (404L, 2, false),
      (404L, 3, false),
      (770L, 3, false),
      (770L, 4, false)
    ).toDF("txId", "addressId", "coinJoin")

    val expected = Seq(
      (1, 2),
      (2, 2),
      (3, 2),
      (4, 2)
    ).toDF("id", "clusterId")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true).sort("id")
    assertDataFrameEquality(clusters, expected)
  }

  test("addresses 1 and 2 are in multiple tx") {
    import spark.implicits._
    val tx = Seq(
      (100L, 1, false),
      (100L, 2, false),
      (404L, 2, false),
      (404L, 3, false),
      (770L, 1, false),
      (770L, 2, false),
      (770L, 4, false)
    ).toDF("txId", "addressId", "coinJoin")

    val expected = Seq(
      (1, 1),
      (2, 1),
      (3, 1),
      (4, 1)
    ).toDF("id", "clusterId")

    val clusters = t.plainAddressCluster(tx, removeCoinJoin = true).sort("id")
    assertDataFrameEquality(clusters, expected)
  }
}
