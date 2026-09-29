package org.graphsense.config

import org.graphsense.account.config.AccountConfig
import org.graphsense.utxo.config.UtxoConf
import org.scalatest.funsuite.AnyFunSuite

class NetworkDefaultsTest extends AnyFunSuite {

  private def base(network: String) =
    Seq("--network", network, "--raw-keyspace", "r", "--target-keyspace", "t")

  test("utxo layout options default per network") {
    val btc = new UtxoConf(base("btc"))
    assert(btc.bucketSize() == 5000)
    assert(btc.addressPrefixLength() == 4)
    assert(btc.bech32Prefix() == "bc")
    assert(btc.coinjoinFilter())

    assert(new UtxoConf(base("LTC")).bech32Prefix() == "ltc1")
    assert(new UtxoConf(base("bch")).bech32Prefix() == "")
  }

  test("explicit utxo layout options win over the network defaults") {
    val conf = new UtxoConf(
      base("btc") ++ Seq(
        "--bucket-size",
        "25000",
        "--bech32-prefix",
        "",
        "--no-coinjoin-filtering"
      )
    )
    assert(conf.bucketSize() == 25000)
    assert(conf.bech32Prefix() == "")
    assert(!conf.coinjoinFilter())
  }

  test("account layout options default per network") {
    val trx = new AccountConfig(base("trx"))
    assert(trx.bucketSize() == 25000)
    assert(trx.addressPrefixLength() == 5)
    assert(trx.txPrefixLength() == 5)
    assert(trx.blockBucketSizeAddressTxs() == 50000)
    assert(trx.addressrelationsIdsNbuckets() == 100)

    assert(new AccountConfig(base("eth")).blockBucketSizeAddressTxs() == 150000)
  }

  test("explicit account layout options win over the network defaults") {
    val conf = new AccountConfig(
      base("trx") ++ Seq("--block-bucket-size-address-txs", "450000")
    )
    assert(conf.blockBucketSizeAddressTxs() == 450000)
  }

  test("a network without layout defaults fails loudly") {
    val conf = new UtxoConf(base("doge"))
    assertThrows[IllegalArgumentException](conf.bucketSize())
  }
}
