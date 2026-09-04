"""Raw account keyspace -> v3 derived address tables (eth, trx).

Backfill only, and epoch 0 only, as with the UTXO transform.

The account spine differs from UTXO in three ways, and all three come from the
model rather than from us:

* **A transfer names both ends.** There is no apportioning: a trace or a log
  says who sent what to whom, so an edge is a leg, and the D7 netting problem
  has no account counterpart.
* **Transfers come from TRACES, not from the transaction row.** A trace covers
  the top-level transfer and every internal one alike, so reading traces gets
  both (`eth/Transformation.scala:506`). Only successful traces count.
* **Tokens.** An ERC-20/TRC-20 transfer is a `Transfer` log, not a trace, so a
  transaction can move several assets and touch one address several times. That
  is what `currency` and `tx_reference` are doing in the clustering keys.
"""

# NOTE: no `from __future__ import annotations` -- this module builds pandas
# UDFs through graphsense_v3.spark.columns.

from typing import TYPE_CHECKING, Optional

from graphsense_v3.config import NetworkConfig, config_for
from graphsense_v3.schema import Kind, schema_for
from graphsense_v3.schema.definitions import EPOCH_BASE
from graphsense_v3.spark import derived_common as common
from graphsense_v3.spark import writer
from graphsense_v3.spark.columns import bytes_to_varint_udf

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

#: keccak("Transfer(address,address,uint256)"). Both ERC-20 and TRC-20 use it.
TRANSFER_TOPIC0 = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

#: Native coin symbol and base-unit divisor per network.
NATIVE = {"eth": ("ETH", 10**18), "trx": ("TRX", 10**6)}

#: Trace kinds that bring a contract into existence. A contract deployed by
#: another contract shows up only as an INTERNAL create trace, which is what the
#: delta updater missed on TRON until 2026-06-30.
CREATE_TRACE_TYPES = ("create", "create2")
CREATE_TRACE_NOTES = ("create",)

TABLES = (
    "address_transactions",
    "address_tx_pages",
    "address_stats",
    "address_by_prefix",
    "balance_history",
    "address_outgoing_relations",
    "address_incoming_relations",
    "address_link_transactions",
    "balance",
)


def _topic_address(topics: "DataFrame", index: int):
    """The address in an indexed topic: 32 bytes, left-padded, address last."""
    from pyspark.sql import functions as F

    return F.expr(f"substring(topics[{index}], -20, 20)")


#: Call types that execute in the CALLER's context, so their `value` is
#: apparatus, not a movement: counting them invents transfers and inflates
#: balances. graphsense-spark excludes exactly these
#: (`eth/Transformation.scala:164`).
BORROWED_CONTEXT_CALLS = ("delegatecall", "callcode", "staticcall")


def _native_trace_filter(network: str):
    """Which traces are a native-coin movement on this chain.

    Both branches mirror graphsense-spark, and neither generalises:

    * **eth** -- a trace carries the top-level transfer AND every internal one,
      so the only exclusion is the borrowed-context call types above. A NULL
      ``call_type`` (a reward or suicide trace) is kept, as there.
    * **trx** -- ``isTrxTrace && isCallTrace``
      (`trx/Transformation.scala:50-51`): ``call_token_id IS NULL`` because a
      trace with one is a TRC-10 token movement, not TRX, and ``note = 'call'``
      because a `create` trace is a deployment. TRON traces are INTERNAL calls
      only, which is why :func:`top_level_transfers` exists.
    """
    from pyspark.sql import functions as F

    if network == "eth":
        return ~F.col("call_type").isin(list(BORROWED_CONTEXT_CALLS)) | F.isnull(
            F.col("call_type")
        )
    return F.isnull(F.col("call_token_id")) & (F.col("note") == "call")


def native_transfers(traces: "DataFrame", network: str) -> "DataFrame":
    """Native value transfers carried by traces.

    Only successful traces (`status == 1`). A failed trace moved nothing, so
    counting it would invent a transfer; on TRON `status` is derived from
    `rejected` (D14), which is why it is one column here.
    """
    from pyspark.sql import functions as F

    symbol, _ = NATIVE[network]
    return (
        traces.where(F.col("status") == 1)
        .where(_native_trace_filter(network))
        .where(~F.isnull(F.col("from_address")) & ~F.isnull(F.col("to_address")))
        .select(
            F.col("tx_id"),
            F.col("block_id"),
            F.col("from_address").alias("src_address"),
            F.col("to_address").alias("dst_address"),
            F.col("value").cast("decimal(38,0)").alias("value"),
            F.lit(symbol).alias("currency"),
            F.col("trace_index").cast("int").alias("trace_index"),
            F.lit(None).cast("int").alias("log_index"),
        )
    )


def token_transfers(logs: "DataFrame", token_config: "DataFrame") -> "DataFrame":
    """Transfers decoded from `Transfer` logs of configured tokens.

    Joined against ``token_configuration`` rather than decoding every log: an
    arbitrary contract can emit a `Transfer` with the same signature, and only
    configured assets have a ticker, decimals and a peg to price them with.
    """
    from pyspark.sql import functions as F

    varint = bytes_to_varint_udf()
    decoded = (
        logs.where(F.col("topic0") == F.lit(TRANSFER_TOPIC0))
        # from and to are indexed, so they are topics 1 and 2; the value is the
        # single non-indexed parameter and occupies the first word of `data`.
        .where(F.size(F.col("topics")) >= 3)
        .select(
            F.col("tx_id"),
            F.col("block_id"),
            F.col("address").alias("token_address"),
            _topic_address(logs, 1).alias("src_address"),
            _topic_address(logs, 2).alias("dst_address"),
            varint(F.expr("substring(data, 1, 32)")).alias("value"),
            F.col("log_index").cast("int").alias("log_index"),
        )
    )
    return decoded.join(
        token_config.select("token_address", "currency_ticker"),
        on="token_address",
        how="inner",
    ).select(
        F.col("tx_id"),
        F.col("block_id"),
        F.col("src_address"),
        F.col("dst_address"),
        F.col("value"),
        F.col("currency_ticker").alias("currency"),
        F.lit(None).cast("int").alias("trace_index"),
        F.col("log_index"),
    )


def _spendable_txs(transactions: "DataFrame") -> "DataFrame":
    """TRON transactions that moved TRX to a known recipient.

    ``onlySuccessfulTxs``, ``txContractCreationAsToAddress`` and
    ``removeUnknownRecipientTxs`` composed (`trx/Transformation.scala:155-157`).
    The second rewrites a creation tx's recipient to the deployed contract and
    the third then drops every row that HAS a ``receipt_contract_address``, so
    the pair composes to "successful, has a recipient, is not a deployment" --
    written out here rather than staged, because staged it reads as though
    creation transactions survive.
    """
    from pyspark.sql import functions as F

    return transactions.where(
        (F.col("receipt_status") == 1)
        & ~F.isnull(F.col("to_address"))
        & F.isnull(F.col("receipt_contract_address"))
    )


def top_level_transfers(transactions: "DataFrame", network: str) -> "DataFrame":
    """TRON's top-level TRX transfers, which its traces do not carry.

    A TRON trace is an INTERNAL call (`note = 'call'`), so the transfer the
    transaction itself performs appears nowhere in them -- which is why
    graphsense-spark sums traces AND transactions for both balances
    (`trx/Transformation.scala:174-183`) and address discovery (`:302-312`).
    On eth a trace already covers the top-level call, so this is empty there.
    """
    from pyspark.sql import functions as F

    symbol, _ = NATIVE[network]
    return _spendable_txs(transactions).select(
        F.col("tx_id"),
        F.col("block_id"),
        F.col("from_address").alias("src_address"),
        F.col("to_address").alias("dst_address"),
        F.col("value").cast("decimal(38,0)").alias("value"),
        F.lit(symbol).alias("currency"),
        F.lit(None).cast("int").alias("trace_index"),
        F.lit(None).cast("int").alias("log_index"),
    )


def transfers(
    traces: "DataFrame",
    logs: "DataFrame",
    token_config: "DataFrame",
    network: str,
    transactions: Optional["DataFrame"] = None,
) -> "DataFrame":
    """Every transfer, native and token, as one frame."""
    moves = native_transfers(traces, network)
    if network == "trx" and transactions is not None:
        moves = moves.unionByName(top_level_transfers(transactions, network))
    return moves.unionByName(token_transfers(logs, token_config))


def fee_events(
    transactions: "DataFrame",
    blocks: "DataFrame",
    fees: Optional["DataFrame"],
    network: str,
) -> "DataFrame":
    """Balance changes that are fees rather than transfers.

    The reason a balance cannot be summed from transfer legs alone, and the
    reason graphsense-spark's ``computeBalances`` has five terms where the
    transfer graph has two. Shape is ``derived_common.leg_events``'.

    * **eth** (`eth/Transformation.scala:189-216`) -- the sender pays
      ``receipt_gas_used * gas_price``; the miner receives it; and the EIP-1559
      base fee, ``base_fee_per_gas * gas_used``, is burnt out of the miner's
      credit, leaving the priority fee.
    * **trx** (`trx/Transformation.scala:196-203`) -- the sender pays ``fee``
      and no one receives it: TRON burns fees, so there is no miner side. The
      commented-out ``txFeeDebits`` there records that as a finding, not an
      omission.
    """
    from pyspark.sql import functions as F

    symbol, _ = NATIVE[network]

    def event(address, delta, source):
        return source.select(
            address.alias("address"),
            F.lit(symbol).alias("currency"),
            F.col("block_id"),
            F.col("tx_id"),
            delta.cast("decimal(38,0)").alias("delta"),
        )

    if network == "trx":
        if fees is None:
            raise ValueError("trx fee events need the raw `fee` table")
        paid = _spendable_txs(transactions).join(
            fees.select("tx_id", "fee"), on="tx_id", how="inner"
        )
        return event(F.col("from_address"), -F.col("fee"), paid)

    gas = F.col("receipt_gas_used") * F.col("gas_price")
    charged = transactions.select(
        F.col("tx_id"),
        F.col("block_id"),
        F.col("from_address"),
        gas.alias("gas_cost"),
    )
    sender = event(F.col("from_address"), -F.col("gas_cost"), charged)

    # The miner's two terms are per BLOCK, not per transaction, so they are
    # attributed to the block's last tx_id -- the balance is right from that
    # block onward either way, and a block with no transactions cannot have a
    # fee to attribute.
    earned = (
        charged.groupBy("block_id")
        .agg(F.sum("gas_cost").alias("gas_cost"), F.max("tx_id").alias("tx_id"))
        .join(
            blocks.select("block_id", "miner", "gas_used", "base_fee_per_gas"),
            "block_id",
        )
    )
    burnt = F.coalesce(F.col("base_fee_per_gas"), F.lit(0)) * F.col("gas_used")
    miner = event(F.col("miner"), F.col("gas_cost") - burnt, earned)
    return sender.unionByName(miner)


def priced(
    moves: "DataFrame",
    rates: "DataFrame",
    token_config: "DataFrame",
    network: str,
) -> "DataFrame":
    """Attach a fiat map to every transfer.

    Two cases, where graphsense-spark has three
    (`eth/Transformation.scala:374-410`):

    * **priced directly** -- the asset has a rate of its own for that block, so
      the fiat is the amount times the rate. That covers the native coin and an
      unpegged token alike, because one merged ``exchange_rates`` table holds
      both. An asset with no rate for a block gets no fiat at all, rather than a
      zero that would read as "worthless" instead of "unknown".
    * **pegged** -- the peg fixes one fiat currency exactly (a USD-pegged
      stablecoin is worth its face value in USD), and the others follow from the
      native coin's cross rate.
    """
    from pyspark.sql import functions as F

    symbol, divisor = NATIVE[network]
    config = token_config.select(
        F.col("currency_ticker").alias("currency"),
        F.col("decimal_divisor"),
        F.col("peg_currency"),
    )
    native_rates = rates.where(F.col("asset") == symbol).select(
        F.col("block_id"), F.col("fiat_values").alias("_native_rates")
    )
    joined = (
        moves.join(config, on="currency", how="left")
        .join(
            rates.select(
                F.col("asset").alias("currency"),
                F.col("block_id"),
                F.col("fiat_values").alias("_rates"),
            ),
            on=["currency", "block_id"],
            how="left",
        )
        .join(native_rates, on="block_id", how="left")
    )
    units = F.col("value") / F.coalesce(F.col("decimal_divisor"), F.lit(divisor))
    direct = F.transform_values(
        F.col("_rates"), lambda _, rate: F.round(units * rate, 2)
    )
    # The peg fixes its own currency; every other one is the cross rate against
    # it, which is why this needs the native rates and not just the peg.
    pegged = F.transform_values(
        F.col("_native_rates"),
        lambda code, rate: F.round(
            F.when(code == F.col("peg_currency"), units).otherwise(
                units
                * rate
                / F.element_at(F.col("_native_rates"), F.col("peg_currency"))
            ),
            2,
        ),
    )
    return joined.withColumn(
        "fiat_values",
        F.when(~F.isnull(F.col("peg_currency")), pegged).otherwise(direct),
    ).drop("_rates", "_native_rates", "peg_currency", "decimal_divisor")


def legs(moves: "DataFrame") -> "DataFrame":
    """One row per (transfer, address, direction).

    A transfer names both ends, so this is a plain explode -- no grouping, and
    no netting question: the direction is a property of the leg, not of a sum.
    """
    from pyspark.sql import functions as F

    # fiat_values travels with the leg: an entity's totals are the SUM of the
    # legs' fiat, each priced at its own block, never the total priced once.
    shared = [
        "tx_id",
        "block_id",
        "value",
        "currency",
        "fiat_values",
        "trace_index",
        "log_index",
    ]
    outgoing = moves.select(
        F.col("src_address").alias("address"), F.lit(True).alias("is_outgoing"), *shared
    )
    incoming = moves.select(
        F.col("dst_address").alias("address"),
        F.lit(False).alias("is_outgoing"),
        *shared,
    )
    return outgoing.unionByName(incoming).where(~F.isnull(F.col("address")))


def _tx_reference():
    from pyspark.sql import functions as F

    return F.struct(
        F.col("trace_index").alias("trace_index"), F.col("log_index").alias("log_index")
    )


def address_transactions(paged: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return paged.select(
        F.col("address"),
        F.col("is_outgoing"),
        F.col("is_zero_value"),
        F.col("tx_page"),
        F.col("tx_id"),
        _tx_reference().alias("tx_reference"),
        F.col("currency"),
        F.col("value").cast("decimal(38,0)").alias("value"),
        F.col("balance"),
    )


def address_link_transactions(moves: "DataFrame", config: NetworkConfig) -> "DataFrame":
    """Partition per EDGE, which is the account half of D10.

    An account chain has an order of magnitude fewer addresses than UTXO with
    more transactions per edge (TRX 2.27, ETH 1.92), so the repeated destination
    costs less than the partitions a per-source layout would save.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = Window.partitionBy("src_address", "dst_address").orderBy("tx_id")
    return (
        moves.withColumn("_ordinal", F.row_number().over(order) - 1)
        .withColumn(
            "tx_page", (F.col("_ordinal") / F.lit(config.tx_page_size)).cast("int")
        )
        .select(
            F.col("src_address"),
            F.col("dst_address"),
            F.col("tx_page"),
            F.col("tx_id"),
            _tx_reference().alias("tx_reference"),
            F.col("currency"),
            F.col("value").cast("decimal(38,0)").alias("value"),
        )
    )


def _relation_side(
    moves: "DataFrame", config: NetworkConfig, *, near: str, far: str, network: str
) -> "DataFrame":
    """One direction of the relations pair, aggregated to epoch 0.

    ``value`` holds the native total; ``token_values`` holds one entry per token
    the edge carried, which is the account-only column.
    """
    from pyspark.sql import functions as F

    symbol, _ = NATIVE[network]
    keys = [near, far]
    counts = moves.groupBy(*keys).agg(
        F.count("*").cast("bigint").alias("no_transactions")
    )

    def totals(rows: "DataFrame", extra_keys: list):
        amounts = rows.groupBy(*keys, *extra_keys).agg(
            F.sum("value").cast("decimal(38,0)").alias("_value")
        )
        fiat = common.sum_fiat(rows, keys + extra_keys, config.fiat_currencies)
        return amounts.join(fiat, on=keys + extra_keys, how="left")

    native = totals(moves.where(F.col("currency") == symbol), []).select(
        *keys, common.currency_struct(F.col("_value"), F.col("_fiat")).alias("value")
    )
    tokens = (
        totals(moves.where(F.col("currency") != symbol), ["currency"])
        .groupBy(*keys)
        .agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("currency"),
                        common.currency_struct(F.col("_value"), F.col("_fiat")).alias(
                            "value"
                        ),
                    )
                )
            ).alias("token_values")
        )
    )
    return (
        counts.join(native, on=keys, how="left")
        .join(tokens, on=keys, how="left")
        .select(
            F.col(near),
            common.entity_bucket(F.col(far), config).alias("rel_bucket"),
            F.col(far),
            F.lit(EPOCH_BASE).alias("epoch"),
            F.col("no_transactions"),
            F.col("value"),
            F.col("token_values"),
        )
    )


def degrees(moves: "DataFrame") -> "DataFrame":
    """Distinct-counterparty counts, per address and direction."""
    from pyspark.sql import functions as F

    def side(near: str, far: str, prefix: str) -> "DataFrame":
        return moves.groupBy(near).agg(
            F.countDistinct(far).cast("bigint").alias(f"{prefix}_degree"),
            F.countDistinct(F.when(F.col("value") == 0, F.col(far)))
            .cast("bigint")
            .alias(f"{prefix}_degree_zero_value"),
        )

    out = side("src_address", "dst_address", "out").withColumnRenamed(
        "src_address", "address"
    )
    incoming = side("dst_address", "src_address", "in").withColumnRenamed(
        "dst_address", "address"
    )
    return out.join(incoming, on="address", how="outer")


def contracts(traces: "DataFrame", network: str) -> "DataFrame":
    """Addresses a contract was deployed to.

    Any create trace counts, internal ones included: a contract deployed by a
    factory appears ONLY as an internal trace, which is what the delta updater
    missed on TRON until 2026-06-30.
    """
    from pyspark.sql import functions as F

    if network == "trx":
        created = F.col("note").isin(list(CREATE_TRACE_NOTES))
    else:
        created = F.col("trace_type").isin(list(CREATE_TRACE_TYPES))
    return (
        traces.where(created & ~F.isnull(F.col("to_address")))
        .select(F.col("to_address").alias("address"))
        .distinct()
        .withColumn("is_contract", F.lit(True))
    )


def address_stats(
    all_legs: "DataFrame",
    paged: "DataFrame",
    degree: "DataFrame",
    is_contract: "DataFrame",
    config: NetworkConfig,
    network: str,
) -> "DataFrame":
    """The epoch-0 base row for every address."""
    from pyspark.sql import functions as F

    symbol, _ = NATIVE[network]

    def side(outgoing: bool, prefix: str) -> "DataFrame":
        rows = all_legs.where(F.col("is_outgoing") == F.lit(outgoing))
        counts = rows.groupBy("address").agg(
            F.count("*").cast("bigint").alias(f"no_{prefix}_txs"),
            F.sum(F.when(F.col("value") == 0, 1).otherwise(0))
            .cast("bigint")
            .alias(f"no_{prefix}_txs_zero_value"),
        )
        native = rows.where(F.col("currency") == symbol)
        amounts = native.groupBy("address").agg(
            F.sum("value").cast("decimal(38,0)").alias("_value")
        )
        totals = amounts.join(
            common.sum_fiat(native, ["address"], config.fiat_currencies),
            on="address",
            how="left",
        ).select(
            "address",
            common.currency_struct(F.col("_value"), F.col("_fiat")).alias(
                "total_spent" if outgoing else "total_received"
            ),
        )
        token_rows = rows.where(F.col("currency") != symbol)
        token_amounts = token_rows.groupBy("address", "currency").agg(
            F.sum("value").cast("decimal(38,0)").alias("_value")
        )
        tokens = (
            token_amounts.join(
                common.sum_fiat(
                    token_rows, ["address", "currency"], config.fiat_currencies
                ),
                on=["address", "currency"],
                how="left",
            )
            .groupBy("address")
            .agg(
                F.map_from_entries(
                    F.collect_list(
                        F.struct(
                            F.col("currency"),
                            common.currency_struct(
                                F.col("_value"), F.col("_fiat")
                            ).alias("value"),
                        )
                    )
                ).alias("total_tokens_spent" if outgoing else "total_tokens_received")
            )
        )
        return counts.join(totals, on="address", how="left").join(
            tokens, on="address", how="left"
        )

    cursors = common.paging_cursors(paged)
    bounds = all_legs.groupBy("address").agg(
        F.min("tx_id").alias("first_tx_id"), F.max("tx_id").alias("last_tx_id")
    )
    joined = (
        bounds.join(side(False, "incoming"), on="address", how="left")
        .join(side(True, "outgoing"), on="address", how="left")
        .join(cursors, on="address", how="left")
        .join(degree, on="address", how="left")
        .join(is_contract, on="address", how="left")
    )
    zero = F.lit(0).cast("bigint")
    return joined.select(
        common.entity_bucket(F.col("address"), config).alias("address_bucket"),
        F.col("address"),
        F.lit(EPOCH_BASE).alias("epoch"),
        F.coalesce(F.col("no_incoming_txs"), zero).alias("no_incoming_txs"),
        F.coalesce(F.col("no_outgoing_txs"), zero).alias("no_outgoing_txs"),
        F.coalesce(F.col("no_incoming_txs_zero_value"), zero).alias(
            "no_incoming_txs_zero_value"
        ),
        F.coalesce(F.col("no_outgoing_txs_zero_value"), zero).alias(
            "no_outgoing_txs_zero_value"
        ),
        F.col("total_received"),
        F.col("total_spent"),
        F.col("total_tokens_received"),
        F.col("total_tokens_spent"),
        F.col("first_tx_id"),
        F.col("last_tx_id"),
        F.coalesce(F.col("in_degree"), zero).alias("in_degree"),
        F.coalesce(F.col("out_degree"), zero).alias("out_degree"),
        F.coalesce(F.col("in_degree_zero_value"), zero).alias("in_degree_zero_value"),
        F.coalesce(F.col("out_degree_zero_value"), zero).alias("out_degree_zero_value"),
        F.col("in_tx_page_max"),
        F.col("out_tx_page_max"),
        F.col("in_tx_ordinal_next"),
        F.col("out_tx_ordinal_next"),
        F.col("in_zero_tx_page_max"),
        F.col("out_zero_tx_page_max"),
        F.col("in_zero_tx_ordinal_next"),
        F.col("out_zero_tx_ordinal_next"),
        F.coalesce(F.col("is_contract"), F.lit(False)).alias("is_contract"),
    )


def balance_events(
    all_legs: "DataFrame",
    transactions: "DataFrame",
    blocks: "DataFrame",
    fees: Optional["DataFrame"],
    network: str,
) -> "DataFrame":
    """Every event that moves an account balance: transfers AND fees.

    ``balance``, ``balance_history`` and the per-transaction running balance all
    read this one stream, so the three cannot disagree -- which they would if
    each summed the legs and only some remembered the gas.
    """
    from pyspark.sql import functions as F

    return common.leg_events(all_legs, F.col("currency")).unionByName(
        fee_events(transactions, blocks, fees, network)
    )


def build(
    traces: "DataFrame",
    logs: "DataFrame",
    token_config: "DataFrame",
    blocks: "DataFrame",
    rates: "DataFrame",
    network: str,
    *,
    config: Optional[NetworkConfig] = None,
    transactions: Optional["DataFrame"] = None,
    fees: Optional["DataFrame"] = None,
) -> dict:
    """The derived address tables for an account network."""
    if transactions is None:
        raise ValueError(
            "the raw `transaction` frame is required: a fee moves value without "
            "being a transfer, so a balance summed from the legs alone is wrong "
            "-- and on TRON the transaction row carries transfers the traces do "
            "not have at all"
        )
    cfg = config or config_for(network)
    moves = priced(
        transfers(traces, logs, token_config, network, transactions),
        rates,
        token_config,
        network,
    ).cache()
    all_legs = legs(moves).cache()
    events = balance_events(all_legs, transactions, blocks, fees, network).cache()
    paged = common.with_ordinals(
        common.with_zero_flag(all_legs),
        ["address", "is_outgoing", "is_zero_value"],
        cfg,
    ).cache()
    return {
        "address_transactions": address_transactions(
            common.with_running_balance(paged, events, per_currency=True)
        ),
        "address_tx_pages": common.address_tx_pages(paged),
        "address_stats": address_stats(
            all_legs,
            paged,
            degrees(moves),
            contracts(traces, network),
            cfg,
            network,
        ),
        "address_by_prefix": common.address_by_prefix(all_legs, network, cfg),
        "address_outgoing_relations": _relation_side(
            moves, cfg, near="src_address", far="dst_address", network=network
        ),
        "address_incoming_relations": _relation_side(
            moves, cfg, near="dst_address", far="src_address", network=network
        ),
        "address_link_transactions": address_link_transactions(moves, cfg),
        "balance": common.balance(events, cfg),
        "balance_history": common.balance_history(events, blocks, cfg),
    }


def load(
    traces: "DataFrame",
    logs: "DataFrame",
    token_config: "DataFrame",
    blocks: "DataFrame",
    rates: "DataFrame",
    network: str,
    keyspace: str,
    *,
    tables: Optional[tuple] = None,
    config: Optional[NetworkConfig] = None,
    sidecar: Optional[dict] = None,
    transactions: Optional["DataFrame"] = None,
    fees: Optional["DataFrame"] = None,
) -> list:
    """Write the derived address tables into ``keyspace``."""
    schema = schema_for(network, Kind.DERIVED)
    frames = build(
        traces,
        logs,
        token_config,
        blocks,
        rates,
        network,
        config=config,
        transactions=transactions,
        fees=fees,
    )
    selected = tables or TABLES
    for name in selected:
        writer.check(frames[name], schema.table(name))
    written = []
    for name in selected:
        writer.write(frames[name], schema.table(name), keyspace, sidecar=sidecar)
        written.append(name)
    return written
