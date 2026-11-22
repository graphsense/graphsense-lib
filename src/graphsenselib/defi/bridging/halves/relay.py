"""Relay (relay.link) half bridges (defi/bridging/delegated.py): the deposits.

A deposit is an event of Relay's Depository, counted only from that contract:

    RelayNativeDeposit(address from, uint256 amount, bytes32 id)
    RelayErc20Deposit(address from, address token, uint256 amount, bytes32 id)

nothing indexed. Neither names the chain it is filled on, so the destination
is unknown until the external backend pairs it (through Relay's own record of
the order). A refunded deposit, and one filled on its own chain, stays this
half: the backend answers no pair for it.

Legs, as the external backend adapter reads them (services/bridges/relay.py),
so its pairs name these legs: the one Transfer of the event's token and amount
into the Depository among the logs since the deposit before it, else the tx (a
native deposit always). A Transfer leg's fromAddress is its sender, set by
half_bridges (delegated._paid_by): a router often deposits for the user. A
bare-tx leg's fromAddress is the depositor the event states (`from`), never
the tx's sender, which is a bundler when the deposit comes through an
ERC-4337 EntryPoint (11 of 80 native deposits the adapter sampled).

A fill is no half bridge: it logs nothing of Relay's. All that tells it is its
calldata, a payment or a call to one of Relay's contracts with the order's id
after it, and who sent it, one of Relay's solver wallets. A refund, a solver's
other payments and anybody's transfer with 32 bytes appended have the same
calldata, and the wallets are Relay's list, which only the external backend
holds. So nothing of a fill is read here: `fill_shaped` says a tx MAY be one
(its calldata alone, the adapter's shapes in services/bridges/relay.py), the
conversions route then asks the backend and takes its row
(web/service/txs_service.py), and without its answer there is no row. Not
read either: a raw `ERC20.transfer` to the Depository, which logs no
Depository event.

Contract and topic0s: Relay's /chains answer (protocol.v2.depository) and the
adapter's recorded receipts (tests/data/bridges/relay/), 2026-11-22.

A deposit's transfer id is its `id`, Relay's orderId (0x hex)."""

from typing import List, Optional

from graphsenselib.defi.bridging.half_common import (
    PENDING,
    UNKNOWN,
    Log,
    _address,
    _leg,
    _transfers,
    _words,
)
from graphsenselib.defi.bridging.models import Bridge

PROTOCOL = "relay"
NATIVE = "native"

_NATIVE_DEPOSIT = bytes.fromhex(
    "8032066556caf3967d8fec4ad22a2d9e1e9576556b2903a0fcd5b1fd201e3477"
)
_ERC20_DEPOSIT = bytes.fromhex(
    "49fed1d0b752ce30eee63c7a81133f3363b532fec5d4d7dd1ccfd005de4555e1"
)
# network -> its Depository; a network not listed has no Relay half bridge
_DEPOSITORIES = {"eth": bytes.fromhex("4cd00e387622c35bddb9b4c962c136462338bc31")}

# A fill's calldata is a call plus one word, the order's id. Direct: the id
# alone on a plain value transfer, or after the token's transfer(address,
# uint256) / transferFrom(address,address,uint256).
_ID_BYTES = 32
_TRANSFER = bytes.fromhex("a9059cbb")
_TRANSFER_FROM = bytes.fromhex("23b872dd")
# Through one of Relay's contracts (its /chains `contracts`, one address per
# version on every network): contract -> the call a fill makes on it
_MULTICALL = bytes.fromhex("cd6e13f7")  # router
_PERMIT_MULTICALL = bytes.fromhex("0a2b8f36")  # ApprovalProxy
_EXECUTE = bytes.fromhex("2d9fb478")  # Depository
_FILL_CALLS = {
    bytes.fromhex("0729e2a4716d9f7dd17858d5d504426d228697f7"): _MULTICALL,
    bytes.fromhex("b92fe925dc43a0ecde6c8b1a2709c170ec4fff4f"): _MULTICALL,
    bytes.fromhex("5567954cd026cebfe486062cb75848e0f1c12e58"): _PERMIT_MULTICALL,
    bytes.fromhex("ccc88a9d1b4ed6b0eaba998850414b24f1c315be"): _PERMIT_MULTICALL,
}


def fill_shaped(network: str, to: Optional[bytes], data: Optional[bytes]) -> bool:
    """Whether a tx's calldata has a Relay fill's shape on network (module
    docstring). to and data: the tx's to_address and input."""
    depository = _DEPOSITORIES.get(network)
    if depository is None or not to or not data:
        return False
    to, data = bytes(to), bytes(data)
    selector = data[:4]
    if len(data) == _ID_BYTES:
        return True
    if selector == _TRANSFER:
        return len(data) == 4 + 2 * 32 + _ID_BYTES
    if selector == _TRANSFER_FROM:
        return len(data) == 4 + 3 * 32 + _ID_BYTES
    if to == depository:
        return selector == _EXECUTE
    return _FILL_CALLS.get(to) == selector


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's Relay deposits on network, in log order."""
    depository = _DEPOSITORIES.get(network)
    bridges = []
    since = 0  # the position after the last deposit
    for position, log in enumerate(logs):
        topics = log.get("topics")
        if not topics or bytes(log["address"]) != depository:
            continue
        topic0 = bytes(topics[0])
        if topic0 == _NATIVE_DEPOSIT:
            depositor, amount, order = _words(log["data"], 3)
            asset, payment = NATIVE, _leg(log, [])
        elif topic0 == _ERC20_DEPOSIT:
            depositor, token, amount, order = _words(log["data"], 4)
            token = token.to_bytes(32, "big")[-20:]
            asset = _address(token)
            payment = _leg(
                log,
                _transfers(
                    logs[since:position],
                    token=token,
                    receiver=depository,
                    value=amount,
                ),
            )
        else:
            continue
        since = position + 1
        bridges.append(
            Bridge(
                # the event's depositor; half_bridges puts a Transfer leg's
                # sender in its place
                fromAddress=_address(depositor.to_bytes(32, "big")[-20:]),
                fromAsset=asset,
                fromAmount=amount,
                fromNetwork=network,
                fromPayment=payment,
                toAddress=UNKNOWN,
                toAsset=UNKNOWN,
                toAmount=0,
                toNetwork=UNKNOWN,  # a deposit does not say where it is filled
                toPayment=PENDING,
                protocol=PROTOCOL,
                transferId="0x" + order.to_bytes(32, "big").hex(),
            )
        )
    return bridges
