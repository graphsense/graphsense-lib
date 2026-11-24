"""What the half bridges of every delegated bridge share (delegated.py): the
transfer a half bridge's missing side holds, and the reading of a tx's raw
logs (rows of the log table) into words, Transfers and legs."""

from typing import Any, Dict, List, Optional

from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)

PENDING = "pending"
UNKNOWN = "unknown"

_TRANSFER = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

Log = Dict[str, Any]


def _words(data: bytes, count: int) -> List[int]:
    """The first `count` 32-byte words of an event's data."""
    data = bytes(data)
    if len(data) < 32 * count:
        raise ValueError(f"{len(data)} bytes of data, {count} words expected")
    return [int.from_bytes(data[32 * i : 32 * (i + 1)], "big") for i in range(count)]


def _transfers(
    logs: List[Log],
    token: Optional[bytes] = None,
    sender: Optional[bytes] = None,
    receiver: Optional[bytes] = None,
    value: Optional[int] = None,
) -> List[Log]:
    """The ERC20 Transfers among logs, in log order, that match each of token,
    sender, receiver and value that is given (value exactly)."""
    return [
        log
        for log in logs
        if (token is None or bytes(log["address"]) == token)
        and len(log.get("topics") or ()) == 3
        and bytes(log["topics"][0]) == _TRANSFER
        and (sender is None or bytes(log["topics"][1])[-20:] == sender)
        and (receiver is None or bytes(log["topics"][2])[-20:] == receiver)
        and (value is None or int.from_bytes(bytes(log["data"]), "big") == value)
    ]


def _first(logs: List[Log], topic0: bytes, emitter: bytes) -> Optional[Log]:
    return next(
        (
            log
            for log in logs
            if log.get("topics")
            and bytes(log["topics"][0]) == topic0
            and bytes(log["address"]) == emitter
        ),
        None,
    )


def _leg(event: Log, transfers: List[Log]) -> str:
    """The asset transfer of an event's leg: its one Transfer, or, when several
    fit and cannot be told apart, the tx."""
    tx_hash = bytes(event["tx_hash"]).hex()
    if len(transfers) != 1:
        return tx_hash
    return SubTransactionIdentifier(
        tx_hash=tx_hash,
        tx_type=SubTransactionType.ERC20,
        sub_index=transfers[0]["log_index"],
    ).to_string()


def _pinned_leg(event: Log, transfers: List[Log]) -> str:
    """A bridge half's leg (halves/): `_leg`, except that several Transfers left
    after the token pin make it unknown, never a guess (the reader then names no
    asset or amount either), as the adapter's Across reader does."""
    return UNKNOWN if len(transfers) > 1 else _leg(event, transfers)


def _address(raw: bytes) -> str:
    return "0x" + raw.hex()
