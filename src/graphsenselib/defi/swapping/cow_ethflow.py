"""CoW Protocol eth-flow: link an ETH sell order to the settlement filling it.

Selling native ETH on CoW takes two txs. In the first, the user calls
createOrder on an eth-flow contract with the ETH; the contract keeps the ETH
and emits OrderPlacement(sender, order, signature, data). Later a settlement
tx wraps the ETH, fills the order and pays the buy token to the order's
receiver. There the Trade event names the eth-flow contract as owner, so the
user only appears in the placement tx.

The two are linked by the order uid: the EIP-712 digest of the order under
the settlement contract's domain, followed by the owner (the eth-flow
contract) and validTo. It is computed from the OrderPlacement event, and the
settlement is looked up among the eth-flow contract's txs after the
placement: for each block in which it moved funds, the Trade logs of that
block are read (one topic-filtered query per block) until one carries the
uid. The result is a swap from the user's ETH payment in the placement tx to
the order's payout in the settlement tx.
"""

import asyncio
from typing import Any, Dict, List, Optional

from eth_abi import encode
from eth_utils import keccak

from graphsenselib.datatypes.abi import decode_logs_dict
from graphsenselib.defi.models import Trace
from graphsenselib.defi.swapping.models import COW_SETTLEMENT_ADDRESSES, ExternalSwap
from graphsenselib.utils.accountmodel import NATIVE_ASSET
from graphsenselib.utils.logging import logger

# CoWSwapEthFlow deployments on ethereum (the later one since block 21674096)
ETH_FLOW_ADDRESSES = frozenset(
    {
        "0x40a50cf069e992aa4536211b23f286ef88752187",
        "0xba3cb449bd2b4adddbc894d8697f5170800eadec",
    }
)
TRADE_TOPIC = bytes.fromhex(
    "a07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17"
)
# how far after the placement the settlement is looked for (eth-flow orders
# are usually filled within minutes; about a day of blocks)
SEARCH_MAX_BLOCKS = 7200
SEARCH_PAGE_SIZE = 100
SEARCH_MAX_PAGES = 5
# blocks whose Trade logs are fetched at the same time
SEARCH_BLOCK_BATCH = 10

ORDER_TYPE_HASH = keccak(
    text="Order(address sellToken,address buyToken,address receiver,"
    "uint256 sellAmount,uint256 buyAmount,uint32 validTo,bytes32 appData,"
    "uint256 feeAmount,string kind,bool partiallyFillable,"
    "string sellTokenBalance,string buyTokenBalance)"
)
DOMAIN_TYPE_HASH = keccak(
    text="EIP712Domain(string name,string version,uint256 chainId,"
    "address verifyingContract)"
)
CHAIN_IDS = {"eth": 1}
ORDER_FIELDS = (
    ("sellToken", "address"),
    ("buyToken", "address"),
    ("receiver", "address"),
    ("sellAmount", "uint256"),
    ("buyAmount", "uint256"),
    ("validTo", "uint32"),
    ("appData", "bytes32"),
    ("feeAmount", "uint256"),
    ("kind", "bytes32"),
    ("partiallyFillable", "bool"),
    ("sellTokenBalance", "bytes32"),
    ("buyTokenBalance", "bytes32"),
)


def _bytes(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return bytes.fromhex(str(value).removeprefix("0x"))


def domain_separator(chain_id: int, settlement: str) -> bytes:
    return keccak(
        encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [
                DOMAIN_TYPE_HASH,
                keccak(text="Gnosis Protocol"),
                keccak(text="v2"),
                chain_id,
                settlement,
            ],
        )
    )


def order_uid(order, owner: str, chain_id: int, settlement: str) -> bytes:
    """GPv2 order uid: EIP-712 digest ‖ owner ‖ validTo.

    order: the order struct as decoded from OrderPlacement (a tuple in
    ORDER_FIELDS order).
    """
    values = [
        _bytes(v) if t == "bytes32" else v for (_, t), v in zip(ORDER_FIELDS, order)
    ]
    struct_hash = keccak(
        encode(["bytes32", *(t for _, t in ORDER_FIELDS)], [ORDER_TYPE_HASH, *values])
    )
    digest = keccak(b"\x19\x01" + domain_separator(chain_id, settlement) + struct_hash)
    valid_to = dict(zip((n for n, _ in ORDER_FIELDS), order))["validTo"]
    return digest + _bytes(owner) + int(valid_to).to_bytes(4, "big")


def is_eth_flow_placement(dlog: Dict[str, Any]) -> bool:
    return (
        dlog["name"] == "OrderPlacement"
        and "eth-flow" in dlog["log_def"]["tags"]
        and str(dlog.get("address", "")).lower() in ETH_FLOW_ADDRESSES
    )


def _placement_payment(
    traces: List[Trace], eth_flow: str, amount: int
) -> Optional[Trace]:
    """The call paying the order's ETH to the eth-flow contract."""
    payments = [
        t
        for t in traces
        if t.is_call and t.value and str(t.to_address).lower() == eth_flow
    ]
    exact = [t for t in payments if t.value == amount]
    return (exact or payments or [None])[0]


async def find_settlement(
    network: str, db, eth_flow: str, uid: bytes, from_block: int
) -> Optional[Dict[str, Any]]:
    """The tx whose Trade log carries uid, among eth_flow's txs after from_block."""
    page = None
    for _ in range(SEARCH_MAX_PAGES):
        txs, page = await db.list_address_txs(
            network,
            _bytes(eth_flow),
            direction="out",
            min_height=from_block,
            max_height=from_block + SEARCH_MAX_BLOCKS,
            order="asc",
            page=page,
            pagesize=SEARCH_PAGE_SIZE,
        )
        blocks = list(dict.fromkeys(tx["height"] for tx in txs))
        for i in range(0, len(blocks), SEARCH_BLOCK_BATCH):
            batch = blocks[i : i + SEARCH_BLOCK_BATCH]
            results = await asyncio.gather(
                *(
                    db.get_logs_in_block_eth(network, block, topic=TRADE_TOPIC)
                    for block in batch
                )
            )
            for result in results:
                rows = getattr(result, "current_rows", result)
                for dlog, raw in decode_logs_dict(list(rows)):
                    if (
                        dlog["name"] == "Trade"
                        and _bytes(dlog["parameters"]["orderUid"]) == uid
                    ):
                        return raw
        if not page:
            break
    return None


async def get_eth_flow_swaps(
    network: str,
    db,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    traces: List[Trace],
) -> List[ExternalSwap]:
    """Swaps of the eth-flow orders placed in tx, each linked to its settlement.

    An order that was not (yet) settled within SEARCH_MAX_BLOCKS, e.g. one
    that expired and was refunded, yields no swap.
    """
    from graphsenselib.defi.swaps import cow_order_swaps, create_payment_identifier

    chain_id = CHAIN_IDS.get(network)
    if chain_id is None:
        return []
    (settlement,) = COW_SETTLEMENT_ADDRESSES
    tx_hash = _bytes(tx["tx_hash"]).hex()

    swaps = []
    for placement in filter(is_eth_flow_placement, dlogs):
        eth_flow = str(placement["address"]).lower()
        params = placement["parameters"]
        order = params["order"]
        fields = dict(zip((n for n, _ in ORDER_FIELDS), order))
        uid = order_uid(order, eth_flow, chain_id, settlement)
        paid = fields["sellAmount"] + fields["feeAmount"]
        payment = _placement_payment(traces, eth_flow, paid)
        if payment is None:
            logger.warning(f"eth-flow order without ETH payment, {tx_hash}")
            continue

        raw = await find_settlement(network, db, eth_flow, uid, tx["block_id"])
        if raw is None:
            logger.debug(f"eth-flow order 0x{uid.hex()} not settled, {tx_hash}")
            continue
        settlement_tx = await db.get_tx_by_hash(network, raw["tx_hash"])
        logs, trace_dicts = await asyncio.gather(
            db.fetch_transaction_logs(network, settlement_tx),
            db.fetch_transaction_traces(network, settlement_tx),
        )
        settlement_traces = Trace.dicts_to_normalized(
            network, trace_dicts, settlement_tx
        )
        decoded = decode_logs_dict(list(logs))
        if not decoded:
            continue
        s_dlogs, s_raw = (list(x) for x in zip(*decoded))
        filled = [
            swap
            for trade, swap in cow_order_swaps(s_dlogs, s_raw, settlement_traces)
            if _bytes(trade["parameters"]["orderUid"]) == uid
        ]
        if not filled:
            logger.warning(
                f"eth-flow order 0x{uid.hex()} settled in "
                f"{_bytes(raw['tx_hash']).hex()} but its payout was not found"
            )
            continue
        fill = filled[0]
        swaps.append(
            ExternalSwap(
                fromAddress=str(params["sender"]).lower(),
                toAddress=fill.toAddress,
                fromAsset=NATIVE_ASSET,
                toAsset=fill.toAsset,
                fromAmount=payment.value,
                toAmount=fill.toAmount,
                fromPayment=create_payment_identifier(
                    tx_hash, "trace", payment.trace_index
                ),
                toPayment=fill.toPayment,
            )
        )
    return swaps
