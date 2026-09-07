"""Per-keyspace constants.

Every value here ends up in the keyspace's ``configuration`` row, and both the
backfill and the DAL read it from there rather than hardcoding it. A key derived
from a constant is only stable as long as the constant is: change
``entity_buckets`` on a populated keyspace and every stats row moves.
"""

from __future__ import annotations

from dataclasses import dataclass

from graphsense_v3.schema.model import Family

#: Blocks per stats epoch: how often ingest may PUBLISH, and therefore the
#: keyspace's staleness bound.
#:
#: A stats/relations row is keyed ``(entity, epoch)``, so there is exactly one
#: row per entity per epoch and a second write to it is an upsert rather than an
#: addend. Ingest must therefore emit an entity's row once, when the epoch's
#: blocks are done -- which means nothing from the current epoch is visible
#: until it closes. **The epoch IS the staleness.**
#:
#: Sized to roughly ONE MINUTE of chain time, floored at one block, which is as
#: fresh as a chain can be. That lands every network at ~1 440 epochs a day
#: (144 on the ten-minute chains), so an entity active every single minute
#: accumulates ~1 440 rows between daily compactions and a typical one
#: accumulates one or two.
#:
#: A single 1 000 for every network -- what this was -- meant a 200x spread in
#: what it bounded: ~6.9 days on BCH against ~50 minutes on TRON. Blocks are
#: not a unit of time, so one block count cannot mean one thing across chains.
EPOCH_SIZE: dict[str, int] = {
    "btc": 1,  # ~10 min
    "bch": 1,  # ~10 min
    "ltc": 1,  # ~2.5 min
    "zec": 1,  # ~75 s
    "eth": 5,  # ~60 s
    "trx": 20,  # ~60 s
}

#: Blocks per ``*_transactions_recent`` partition, and therefore the cadence the
#: tail drain runs at.
#:
#: SPLIT FROM `epoch_size`, which it used to share. The two pull in opposite
#: directions: `epoch` is a CLUSTERING column, so fine granularity costs only
#: rows and buys freshness; `block_batch` is a PARTITION KEY, so fine
#: granularity multiplies partitions and the reads that scan them -- a tail read
#: touches one partition per un-drained batch. Tying them together forced the
#: clustering column to accept the partition key's granularity, which is exactly
#: where the staleness came from.
#:
#: Sized to ~1 hour of chain time on the UTXO chains and ~15 minutes on the
#: account chains, where the hot addresses are: a tail partition holds one
#: entity's transactions for that span, so this bounds the partition a busy
#: address builds up before the drain clears it. Lower it, or drain more often,
#: if a chain grows an address hotter than this keeps bounded.
BLOCK_BATCH_SIZE: dict[str, int] = {
    "btc": 6,  # ~1 h
    "bch": 6,  # ~1 h
    "ltc": 24,  # ~1 h
    "zec": 48,  # ~1 h
    "eth": 75,  # ~15 min
    "trx": 300,  # ~15 min
}

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
    "relation_buckets INT, epoch_size INT, block_batch_size INT, "
    "address_prefix_length INT, tx_prefix_length INT, block_bucket_size INT, "
    "tx_block_bucket_size INT, fiat_currencies ARRAY<STRING>, schema_version INT"
)


@dataclass(frozen=True)
class NetworkConfig:
    """The ``configuration`` row for one keyspace."""

    network: str
    family: Family
    entity_buckets: int
    block_bucket_size: int
    tx_block_bucket_size: int
    epoch_size: int
    block_batch_size: int
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
            self.block_batch_size,
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
        epoch_size=EPOCH_SIZE[network],
        block_batch_size=BLOCK_BATCH_SIZE[network],
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
        epoch_size=EPOCH_SIZE[network],
        block_batch_size=BLOCK_BATCH_SIZE[network],
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
