"""Per-keyspace constants.

Every value here ends up in the keyspace's ``configuration`` row, and both the
backfill and the DAL read it from there rather than hardcoding it. A key derived
from a constant is only stable as long as the constant is: change
``entity_buckets`` on a populated keyspace and every stats row moves.
"""

from __future__ import annotations

from dataclasses import dataclass

from graphsense_v3.schema.model import Family

#: Blocks per stats epoch (the doc calls this ``fold_batch_size``). One constant
#: serves both the ``epoch`` of a stats delta row and the ``block_batch`` of an
#: ``*_transactions_recent`` partition -- they batch the same blocks.
DEFAULT_EPOCH_SIZE = 1_000

#: Rows per ``*_transactions`` partition, assigned by ordinal at compaction.
DEFAULT_TX_PAGE_SIZE = 100_000

#: Hash modulus for the relations tables. v2's ``addressrelations_ids_nbuckets``
#: is 100, a *discovered* maximum; a /neighbors read scatters over all of them.
DEFAULT_RELATION_BUCKETS = 16

DEFAULT_ADDRESS_PREFIX_LENGTH = 4
DEFAULT_TX_PREFIX_LENGTH = 5

#: v3 schema version. Bump when a rendered schema changes shape.
SCHEMA_VERSION = 1


#: Spark schema of the ``configuration`` row, in ``as_row`` order.
CONFIGURATION_SCHEMA = (
    "keyspace_name STRING, entity_buckets INT, tx_page_size INT, "
    "relation_buckets INT, epoch_size INT, address_prefix_length INT, "
    "tx_prefix_length INT, block_bucket_size INT, tx_block_bucket_size INT, "
    "fiat_currencies ARRAY<STRING>, schema_version INT"
)


@dataclass(frozen=True)
class NetworkConfig:
    """The ``configuration`` row for one keyspace."""

    network: str
    family: Family
    entity_buckets: int
    block_bucket_size: int
    tx_block_bucket_size: int
    epoch_size: int = DEFAULT_EPOCH_SIZE
    tx_page_size: int = DEFAULT_TX_PAGE_SIZE
    relation_buckets: int = DEFAULT_RELATION_BUCKETS
    address_prefix_length: int = DEFAULT_ADDRESS_PREFIX_LENGTH
    tx_prefix_length: int = DEFAULT_TX_PREFIX_LENGTH
    fiat_currencies: tuple[str, ...] = ("EUR", "USD")
    schema_version: int = SCHEMA_VERSION

    def as_row(self, keyspace: str) -> tuple:
        """The ``configuration`` table row for ``keyspace``.

        A tuple, in :data:`CONFIGURATION_SCHEMA` order -- the two are read
        together, so keep them in step.
        """
        return (
            keyspace,
            self.entity_buckets,
            self.tx_page_size,
            self.relation_buckets,
            self.epoch_size,
            self.address_prefix_length,
            self.tx_prefix_length,
            self.block_bucket_size,
            self.tx_block_bucket_size,
            list(self.fiat_currencies),
            self.schema_version,
        )


def _utxo(
    network: str, entity_buckets: int, tx_block_bucket_size: int
) -> NetworkConfig:
    # UTXO `transaction`/`transaction_io` are partitioned by block_id itself,
    # not by a bucket, so block_bucket_size applies only to `block` and
    # `block_transactions`. There is no tx_bucket_size: tx_id is sparse.
    return NetworkConfig(
        network=network,
        family=Family.UTXO,
        entity_buckets=entity_buckets,
        block_bucket_size=100,
        tx_block_bucket_size=tx_block_bucket_size,
    )


def _account(network: str, entity_buckets: int) -> NetworkConfig:
    # Account block_bucket_size drops from v2's 1 000 to 100: at 1 000 the trace
    # and log partitions reach 47-400 MB and up to 2M rows at the chain head.
    return NetworkConfig(
        network=network,
        family=Family.ACCOUNT,
        entity_buckets=entity_buckets,
        block_bucket_size=100,
        # 4 blocks, not 16: an account transaction row carries `input`, and a
        # block's calldata is gas-bounded at ~2 MB, so a wider bucket has a fat
        # tail even though the typical partition is small.
        tx_block_bucket_size=4,
    )


#: ``entity_buckets`` is sized for ~5 000 rows per stats partition against the
#: address counts measured on 2026-08-31.
#: `tx_block_bucket_size` targets ~1 500-3 000 rows per transaction partition,
#: from the 2026-08-31 /stats counts: BTC ~1 480 transactions per block, BCH
#: ~430, LTC ~130, ZEC ~5, ETH ~143, TRX ~175. Sized from averages, so
#: `raw_utxo.preflight` reports the busiest block it actually sees.
CONFIGS: dict[str, NetworkConfig] = {
    "btc": _utxo("btc", entity_buckets=300_000, tx_block_bucket_size=1),
    # tx_block_bucket_size 1, like BTC and unlike LTC: BCH permits 32 MB
    # blocks, and the September 2018 stress test produced CONSECUTIVE blocks of
    # 100k+ transactions -- block 556045 alone holds 166,882. Bucketing 4 of
    # those together groups exactly the blocks that must not be grouped. The
    # average BCH block (~431 transactions) would amortise fine at 4; the tail
    # is what decides a partition bound.
    "bch": _utxo("bch", entity_buckets=100_000, tx_block_bucket_size=1),
    "ltc": _utxo("ltc", entity_buckets=100_000, tx_block_bucket_size=16),
    "zec": _utxo("zec", entity_buckets=5_000, tx_block_bucket_size=256),
    "eth": _account("eth", entity_buckets=100_000),
    "trx": _account("trx", entity_buckets=100_000),
}


def configuration_row(spark, config: "NetworkConfig", keyspace: str):
    """The single ``configuration`` row for ``keyspace``, as a DataFrame.

    Written into BOTH keyspaces of a run. The derived one is not a copy for
    tidiness: `address_bucket` and `rel_bucket` are `crc32(entity) % n`, so a
    reader of the derived keyspace cannot address a single partition without
    these constants -- and making it read them out of the RAW keyspace would
    mean knowing that keyspace's name, which is exactly the coupling the
    per-keyspace row removes.
    """
    return spark.createDataFrame([config.as_row(keyspace)], schema=CONFIGURATION_SCHEMA)


def config_for(network: str) -> NetworkConfig:
    net = network.lower()
    if net not in CONFIGS:
        raise KeyError(f"no v3 configuration for network {network!r}")
    return CONFIGS[net]
