package org.graphsense.utxo

import org.apache.spark.sql.functions.col

import org.graphsense.utxo.{Fields => F}
import org.graphsense.utxo.models._
import org.graphsense.models._
import org.graphsense.TestBase

class TransformationTest extends TestBase {
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  spark.sparkContext.setCheckpointDir("file:///tmp/spark-checkpoint")

  val inputDir = "src/test/resources/utxo/"
  val refDir = inputDir + "reference/"

  val bucketSize: Int = 2
  val addressPrefixLength: Int = 5

  // transformation pipeline
  val t = new Transformation(spark, bucketSize, addressPrefixLength)

  // load ref data
  val blocks = readTestData[Block](inputDir + "test_blocks.json")
  val transactions = t.addCoinbaseAddress(
    readTestData[Transaction](inputDir + "test_txs.json")
  )
  val exchangeRatesRaw =
    readTestData[ExchangeRatesRaw](inputDir + "test_exchange_rates.json")

  // Compute data
  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .filter(col(F.blockId) === noBlocks - 1)
    .select(col(F.timestamp))
    .first()
    .getInt(0)
  val noTransactions = transactions.count()

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw)
      .persist()

  val regInputs = t.computeRegularInputs(transactions).persist()
  val regOutputs = t.computeRegularOutputs(transactions).persist()

  val addressIds = t.computeAddressIds(regOutputs)
  val addressByAddressPrefix = t.computeAddressByAddressPrefix(addressIds)

  test("addressByAddressPrefix") {
    val addressByAddressPrefixRef =
      readTestData[AddressByAddressPrefix](
        refDir + "address_by_address_prefix.json"
      )
    assertDataFrameEquality(addressByAddressPrefix, addressByAddressPrefixRef)
  }

  addressByAddressPrefix.unpersist()

  val addressTransactions =
    t.computeAddressTransactions(
      regInputs,
      regOutputs,
      addressIds
    ).sort(F.addressId, F.blockId, F.value)
      .persist()

  test("regularOutputs") {
    val regOutputsRef =
      readTestData[RegularOutput](refDir + "regular_outputs.json")
        .sort(F.txId, F.address)
    val sortedOutput = regOutputs.sort(F.txId, F.address)
    assertDataFrameEquality(sortedOutput, regOutputsRef)
  }
  regOutputs.unpersist()

  val (inputs, outputs) = t.splitTransactions(addressTransactions)
  inputs.persist()
  outputs.persist()

  val basicAddresses =
    t.computeBasicAddresses(
      addressTransactions,
      inputs,
      outputs,
      exchangeRates
    ).sort(F.addressId)
      .persist()

  val plainAddressRelations =
    t.computePlainAddressRelations(inputs, outputs, regInputs, transactions)

  val addressRelations =
    t.computeAddressRelations(plainAddressRelations, exchangeRates)
      .sort(F.dstAddressId, F.srcAddressId)
      .persist()
  val noAddressRelations = addressRelations.count()

  val addressCluster =
    t.computeAddressCluster(regInputs, addressIds, true)
      .sort(F.addressId)
      .persist()

  val addresses =
    t.computeAddresses(
      basicAddresses,
      addressCluster,
      addressRelations,
      addressIds
    ).sort(F.addressId)
      .persist()
  val noAddresses = addresses.count()

  test("basicAddresses") {
    val basicAddressesRef =
      readTestData[BasicAddress](refDir + "basic_addresses.json")
    assertDataFrameEquality(basicAddresses, basicAddressesRef)
  }
  basicAddresses.unpersist()

  val addressClusterCoinjoin =
    t.computeAddressCluster(regInputs, addressIds, false)
      .sort(F.addressId)
      .persist()

  test("regularInputs") {
    val regInputsRef =
      readTestData[RegularInput](refDir + "regular_inputs.json")
        .sort(F.txId, F.address)
    val sortedInputs = regInputs.sort(F.txId, F.address)
    assertDataFrameEquality(sortedInputs, regInputsRef)
  }
  regInputs.unpersist()

  test("addressIds") {
    val addressIdsRef =
      readTestData[AddressId](refDir + "address_ids.json")
    assertDataFrameEquality(addressIds, addressIdsRef)
  }
  addressIds.unpersist()

  val clusterAddresses =
    t.computeClusterAddresses(addressClusterCoinjoin)
      .sort(F.clusterId, F.addressId)
      .persist()

  val clusterTransactions =
    t.computeClusterTransactions(
      inputs,
      outputs,
      transactions,
      addressClusterCoinjoin
    ).sort(F.clusterId, F.blockId, F.value)
      .persist()

  val (clusterInputs, clusterOutputs) = t.splitTransactions(clusterTransactions)
  clusterInputs.persist()
  clusterOutputs.persist()

  val basicCluster =
    t.computeBasicCluster(
      clusterAddresses,
      clusterTransactions,
      clusterInputs,
      clusterOutputs,
      exchangeRates
    ).sort(F.clusterId)
      .persist()

  val plainClusterRelations =
    t.computePlainClusterRelations(plainAddressRelations, addressCluster)
      .persist()

  val clusterRelations =
    t.computeClusterRelations(plainClusterRelations, exchangeRates).persist()
  val noClusterRelations = clusterRelations.count()

  plainClusterRelations.unpersist()

  test("clusterInputs") {
    val clusterInputsRef =
      readTestData[ClusterTransaction](refDir + "cluster_inputs.json")
    assertDataFrameEquality(clusterInputs, clusterInputsRef)
  }
  clusterInputs.unpersist()

  test("clusterOutputs") {
    val clusterOutputsRef =
      readTestData[ClusterTransaction](refDir + "cluster_outputs.json")
    assertDataFrameEquality(clusterOutputs, clusterOutputsRef)
  }
  clusterOutputs.unpersist()

  test("addresses") {
    val addressesRef = readTestData[Address](refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }
  addresses.unpersist()

  val cluster =
    t.computeCluster(basicCluster, clusterRelations)
      .sort(F.clusterId)
      .persist()
  val noCluster = cluster.count()

  val summaryStatistics =
    t.summaryStatistics(
      lastBlockTimestamp,
      noBlocks,
      noTransactions,
      noAddresses,
      noAddressRelations,
      noCluster,
      noClusterRelations
    )

  // note("test address graph")

  test("addressTransactions") {
    val addressTransactionsRef =
      readTestData[AddressTransaction](refDir + "address_txs.json")
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }
  test("inputs") {
    val inputsRef =
      readTestData[AddressTransaction](refDir + "inputs.json")
    assertDataFrameEquality(inputs, inputsRef)
  }
  test("outputs") {
    val outputsRef =
      readTestData[AddressTransaction](refDir + "outputs.json")
    assertDataFrameEquality(outputs, outputsRef)
  }

  test("addressRelations") {
    val addressRelationsRef =
      readTestData[AddressRelation](refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }

  note("test cluster graph")

  test("addressCluster without coinjoin inputs") {
    val addressClusterRef =
      readTestData[AddressCluster](refDir + "address_cluster.json")
    assertDataFrameEquality(addressCluster, addressClusterRef)
  }
  test("addressCluster all inputs") {
    val addressClusterRef =
      readTestData[AddressCluster](
        refDir + "address_cluster_with_coinjoin.json"
      )
    assertDataFrameEquality(addressClusterCoinjoin, addressClusterRef)
  }
  test("clusterTransactions") {
    val clusterTransactionsRef =
      readTestData[ClusterTransaction](refDir + "cluster_txs.json")
    assertDataFrameEquality(clusterTransactions, clusterTransactionsRef)
  }

  test("basicCluster") {
    val basicClusterRef =
      readTestData[BasicCluster](refDir + "basic_cluster.json")
    assertDataFrameEquality(basicCluster, basicClusterRef)
  }
  test("plainClusterRelations") {
    val plainClusterRelationsRef =
      readTestData[PlainClusterRelation](
        refDir + "plain_cluster_relations.json"
      ).sort(F.txId, F.srcClusterId, F.dstClusterId)
    val sortedRelations =
      plainClusterRelations.sort(F.txId, F.srcClusterId, F.dstClusterId)
    assertDataFrameEquality(sortedRelations, plainClusterRelationsRef)
  }
  test("clusterRelations") {
    val clusterRelationsRef =
      readTestData[ClusterRelationAdj](refDir + "cluster_relations.json")
        .sort(F.srcClusterId, F.dstClusterId)
    val sortedRelations =
      clusterRelations.sort(F.srcClusterId, F.dstClusterId)
    assertDataFrameEquality(sortedRelations, clusterRelationsRef)
  }
  test("clusters") {
    val clusterRef = readTestData[Cluster](refDir + "cluster.json")
    assertDataFrameEquality(cluster, clusterRef)
  }
  test("clusterAdresses") {
    val clusterAddressesRef =
      readTestData[ClusterAddress](refDir + "cluster_addresses.json")
    assertDataFrameEquality(clusterAddresses, clusterAddressesRef)
  }

  note("summary statistics for address and cluster graph")

  test("summary statistics") {
    val summaryStatisticsRef =
      readTestData[SummaryStatistics](refDir + "summary_statistics.json")
    assertDataFrameEquality(summaryStatistics, summaryStatisticsRef)
  }
}
