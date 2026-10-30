from enum import Enum
from dataclasses import dataclass, asdict


class SwapStrategy(Enum):
    """Enum for different swap detection strategies."""

    ORDER_RECORD = "order_record"
    COW_SETTLEMENT = "cow_settlement"
    IGNORE = "ignore"
    SWAP = "swap"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ExternalSwap:
    fromAddress: str
    toAddress: str
    fromAsset: str
    toAsset: str
    fromAmount: str
    toAmount: str
    fromPayment: str  # log {tx_hash}_L{log_index} or {tx_hash}_I{trace_index}
    toPayment: str

    def to_dict(self):
        return asdict(self)


# GPv2Settlement, deployed at the same address on every chain CoW supports
COW_SETTLEMENT_ADDRESSES = frozenset({"0x9008d19f58aabd9ed0d60971565aa8510560ab41"})


def is_cow_trade(dlog: dict) -> bool:
    """A Trade event of the CoW Protocol settlement contract."""
    return (
        dlog["name"] == "Trade"
        and "cow-protocol" in dlog["log_def"]["tags"]
        and str(dlog.get("address", "")).lower() in COW_SETTLEMENT_ADDRESSES
    )


def get_swap_strategy_from_decoded_logs(
    dlogs: list, parsed_input: dict | None = None, cow_protocol: bool = True
) -> SwapStrategy:
    """Determine the swap detection strategy from decoded logs.

    cow_protocol: detect the orders of CoW Protocol settlements (one
    conversion per order); False ignores settlements as before.
    """
    if not dlogs:
        return SwapStrategy.UNKNOWN

    # names = [dlog["name"] for dlog in dlogs]

    def get_tags(dlog):
        return dlog["log_def"]["tags"]

    tags = [tag for dlog in dlogs for tag in get_tags(dlog)]
    parsed_input_tags = (parsed_input or {}).get("function_def", {}).get("tags", [])
    final_log_tags = dlogs[-1]["log_def"]["tags"] if dlogs else []

    # just a guess but i think it should be last, so its not just routed through there?
    # Lets be conservative
    # if "OrderRecord" == names[-1]:
    #    return SwapStrategy.ORDER_RECORD
    if cow_protocol and "cross-chain" not in tags and any(map(is_cow_trade, dlogs)):
        # a batch of orders, possibly of several owners
        return SwapStrategy.COW_SETTLEMENT
    if (
        "settlement" in final_log_tags and "cow-protocol" in final_log_tags
    ) or "cross-chain" in tags:
        # e.g. https://etherscan.io/tx/0x8e7a3d044ed6873a5683ffe2f59b8cd68a3d786edaa64cdc4c05a9ae8ff97984
        # may settle multiple orders in one tx
        return SwapStrategy.IGNORE
    elif "swap" in tags or "swap" in parsed_input_tags:
        return SwapStrategy.SWAP
    else:
        return SwapStrategy.UNKNOWN
