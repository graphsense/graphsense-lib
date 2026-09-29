package org.graphsense.config

/** Layout of a transformed UTXO keyspace. */
final case class UtxoLayout(
    bucketSize: Int,
    addressPrefixLength: Int,
    bech32Prefix: String,
    coinjoinFiltering: Boolean
)

/** Layout of a transformed account keyspace. */
final case class AccountLayout(
    bucketSize: Int,
    addressPrefixLength: Int,
    txPrefixLength: Int,
    blockBucketSizeAddressTxs: Int,
    addressrelationsIdsNbuckets: Int
)

/** Per-network defaults for the layout options of the job.
  *
  * These are the production layouts (the `configuration` rows of the prod
  * keyspaces). A single default per schema cannot be right for every network
  * (btc needs bech32 prefix `bc`, bch none; trx a block bucket size of 50000,
  * eth 150000), so they are looked up by `--network`.
  *
  * Must match graphsense-lib's `get_default_data_configuration`, which
  * `transformation raw-to-transformed` passes to the job explicitly; guarded
  * by `tests/transformation/test_spark_layout_defaults_parity.py`.
  */
object NetworkDefaults {

  val utxo: Map[String, UtxoLayout] = Map(
    "btc" -> UtxoLayout(
      bucketSize = 5000,
      addressPrefixLength = 4,
      bech32Prefix = "bc",
      coinjoinFiltering = true
    ),
    "ltc" -> UtxoLayout(
      bucketSize = 5000,
      addressPrefixLength = 4,
      bech32Prefix = "ltc1",
      coinjoinFiltering = true
    ),
    "bch" -> UtxoLayout(
      bucketSize = 5000,
      addressPrefixLength = 4,
      bech32Prefix = "",
      coinjoinFiltering = true
    ),
    "zec" -> UtxoLayout(
      bucketSize = 5000,
      addressPrefixLength = 4,
      bech32Prefix = "",
      coinjoinFiltering = true
    )
  )

  val account: Map[String, AccountLayout] = Map(
    "eth" -> AccountLayout(
      bucketSize = 25000,
      addressPrefixLength = 5,
      txPrefixLength = 5,
      blockBucketSizeAddressTxs = 150000,
      addressrelationsIdsNbuckets = 100
    ),
    "trx" -> AccountLayout(
      bucketSize = 25000,
      addressPrefixLength = 5,
      txPrefixLength = 5,
      blockBucketSizeAddressTxs = 50000,
      addressrelationsIdsNbuckets = 100
    )
  )

  def utxoFor(network: String): UtxoLayout =
    utxo.getOrElse(
      network.toLowerCase,
      throw new IllegalArgumentException(
        s"No UTXO layout defaults for network $network"
      )
    )

  def accountFor(network: String): AccountLayout =
    account.getOrElse(
      network.toLowerCase,
      throw new IllegalArgumentException(
        s"No account layout defaults for network $network"
      )
    )
}
