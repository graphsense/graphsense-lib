"""Every bridge row graphsense-lib builds names the protocol whose contracts
prove it, the app on top where a verified table names one, and the protocol's
own id of the transfer where the reader decodes one. The dashboard's bridge
panel names the bridge and links its explorer from these (`protocol`,
`protocol_app`, `transfer_id`; adapter DASHBOARD_CHANGES D-38).

Expected ids are read off the recorded chain data, not off our decoders: the
CCTP nonce topics (V1 DepositForBurn topic 1, MessageReceived topic 2), the
LayerZero guid topic, Across's indexed depositId, the deBridge orderId (data
word 1), Relay's id (the deposit's last data word) and the Wormhole VAA id
(emitter chain / emitter / sequence) of the LogMessagePublished or
TransferRedeemed.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from graphsenselib.defi.bridging import symbiosis
from graphsenselib.defi.bridging.delegated import half_bridges
from graphsenselib.defi.bridging.halves import (
    across,
    debridge,
    layerzero,
    relay,
    wormhole,
)
from graphsenselib.defi.bridging.models import (
    BridgeReceiveTransfer,
    BridgeSendTransfer,
)
from graphsenselib.defi.bridging.thorchain import combine_bridge_transfers

DATA = Path(__file__).parent


def unhex(s):
    return bytes.fromhex(s.removeprefix("0x"))


def db_rows(fixture, part):
    """Logs of one fixture tx as rows of the raw log table."""
    tx = json.loads((DATA / fixture).read_text())[part]
    return [
        {
            "tx_hash": unhex(tx["tx_hash"]),
            "block_id": tx.get("block_id"),
            "log_index": log["log_index"],
            "address": unhex(log["address"]),
            "topics": [unhex(t) for t in log["topics"]],
            "topic0": unhex(log["topics"][0]),
            "data": unhex(log["data"]),
        }
        for log in tx["logs"]
    ]


def named(bridges):
    return [(b.protocol, b.protocolApp, b.transferId) for b in bridges]


def cctp(network, logs):
    return list(half_bridges(network, logs, ("cctp",)))


# the eth Token Bridge as a Wormhole emitter: 32 bytes, no 0x
ETH_TOKEN_BRIDGE = "0" * 24 + "3ee18b2214aff97000d974cf647e7c347e8fa585"
BASE_TOKEN_BRIDGE = "0" * 24 + "8d2de8d2f73f1f4cab472ac9a881c9b123c79627"

CASES = [
    # CCTP V1: the uint64 nonce in decimal (DepositForBurn topic 1 0x…07ba02;
    # MessageReceived topic 2 0x…0e075b)
    (cctp, "cctp_eth_logs.json", "v1_burn", "eth", [("cctp", None, "506370")]),
    (cctp, "cctp_eth_logs.json", "v1_mint", "eth", [("cctp", None, "919387")]),
    # CCTP V2: a burn's MessageSent carries an empty nonce (Iris assigns it),
    # a mint's MessageReceived the bytes32 nonce
    (cctp, "cctp_eth_logs.json", "v2_burn", "eth", [("cctp", None, None)]),
    (
        cctp,
        "cctp_eth_logs.json",
        "v2_mint",
        "eth",
        [
            (
                "cctp",
                None,
                "0x3bfde92aaa92d71d8fa947c466bfe7433345e56b302a2b45e53a5f598a7760cb",
            )
        ],
    ),
    # LayerZero: the guid; a Stargate pool is the stargate app, the eth USDT0
    # lockbox (0x6c96de32) the usdt0 app; a bus ride (guid 0) has no id
    (
        layerzero.read,
        "data/bridges/layerzero/eth_logs.json",
        "eth_arb_usdc_lifi",
        "eth",
        [
            (
                "layerzero",
                "stargate",
                "0x53232cde0d501c67ba7636b541a1a50730f6532c68f4ce7bbc3cc94b8f239ff8",
            )
        ],
    ),
    (
        layerzero.read,
        "data/bridges/layerzero/eth_logs.json",
        "arb_eth_usdc",
        "eth",
        [
            (
                "layerzero",
                "stargate",
                "0x25232a057570c02289df4b6868dff8716f7a6737a9569a1a78f704071c762ffe",
            )
        ],
    ),
    (
        layerzero.read,
        "data/bridges/layerzero/eth_logs.json",
        "arb_eth_usdt0",
        "eth",
        [
            (
                "layerzero",
                "usdt0",
                "0x91389304683b71c924031545755f1da8edffb74947ef8928776a2706d307a07f",
            )
        ],
    ),
    (
        layerzero.read,
        "data/bridges/layerzero/eth_logs.json",
        "eth_linea_native_bus",
        "eth",
        [("layerzero", "stargate", None)],
    ),
    # Across: the depositId in decimal (unique per origin chain only)
    (
        across.read,
        "data/bridges/across/eth_logs.json",
        "eth_base_native",
        "eth",
        [("across", None, "3487163")],
    ),
    (
        across.read,
        "data/bridges/across/eth_logs.json",
        "base_eth_weth_fill",
        "eth",
        [("across", None, "5154354")],
    ),
    (
        across.read,
        "data/bridges/across/eth_logs.json",
        "pol_eth_usdc_fill",
        "eth",
        [("across", None, "2014742")],
    ),
    (
        across.read,
        "data/bridges/across/eth_logs.json",
        "eth_pol_v3_legacy",
        "eth",
        [("across", None, "1642578")],
    ),
    # deBridge: the orderId
    (
        debridge.read,
        "data/bridges/debridge/eth_logs.json",
        "eth_arb_native",
        "eth",
        [
            (
                "debridge",
                None,
                "0xa73bbde6860120ce7dfa6a8767b8314f0dda1e4a903c48ee437d057c22546f5a",
            )
        ],
    ),
    (
        debridge.read,
        "data/bridges/debridge/eth_logs.json",
        "avax_eth_old_fill",
        "eth",
        [
            (
                "debridge",
                None,
                "0x45e24834b5f73a7e50cca0c37358c68948251994522756bc9abd9c108f13b9a8",
            )
        ],
    ),
    (
        debridge.read,
        "data/bridges/debridge/eth_logs.json",
        "bnb_eth_batched_fill",
        "eth",
        [
            (
                "debridge",
                None,
                "0xe568758c8490ec89da01036343920ee243b56e227c13adb9887d02cdb5df6128",
            ),
            (
                "debridge",
                None,
                "0x7016cb63f3153c745e5937885104c7f9dc3f1b9d0c91642bb0fc53360c959410",
            ),
        ],
    ),
    # Relay: the deposit's id, Relay's orderId
    (
        relay.read,
        "data/bridges/relay/eth_logs.json",
        "eth_native_deposit",
        "eth",
        [
            (
                "relay",
                None,
                "0xb278138a886ce306942ff60db4232a4c1193bcf8e7def91091e1a8eaf75ad16a",
            )
        ],
    ),
    (
        relay.read,
        "data/bridges/relay/eth_logs.json",
        "eth_erc20_deposit",
        "eth",
        [
            (
                "relay",
                None,
                "0x96735c7bb1920b796cd2bb66933982d887db39e72ba0ab3823cac43ba95c85d2",
            )
        ],
    ),
    # Wormhole: the VAA id; a send's emitter is its network's Token Bridge, a
    # redeem names the source's
    (
        wormhole.read,
        "data/bridges/wormhole/eth_logs.json",
        "eth_arb",
        "eth",
        [("wormhole", None, f"2/{ETH_TOKEN_BRIDGE}/704561")],
    ),
    (
        wormhole.read,
        "data/bridges/wormhole/eth_logs.json",
        "eth_base_with_payload",
        "eth",
        [("wormhole", None, f"2/{ETH_TOKEN_BRIDGE}/704559")],
    ),
    (
        wormhole.read,
        "data/bridges/wormhole/eth_logs.json",
        "base_eth",
        "eth",
        [("wormhole", None, f"30/{BASE_TOKEN_BRIDGE}/285063")],
    ),
]


@pytest.mark.parametrize(
    "read, fixture, part, network, expected",
    CASES,
    ids=[f"{c[1].split('/')[-2] if '/' in c[1] else 'cctp'}-{c[2]}" for c in CASES],
)
def test_a_half_bridge_names_its_protocol_app_and_transfer_id(
    read, fixture, part, network, expected
):
    assert named(read(network, db_rows(fixture, part))) == expected


def test_a_debridge_order_has_one_id_from_either_end():
    """The bnb order and the eth fill of the same order name the same id."""
    [created] = debridge.read(
        "bnb",
        db_rows("data/bridges/debridge/bnb_logs.json", "bnb_order_via_forwarder"),
    )
    fills = debridge.read(
        "eth", db_rows("data/bridges/debridge/eth_logs.json", "bnb_eth_batched_fill")
    )
    assert created.protocol == "debridge"
    assert created.transferId == fills[0].transferId


def test_a_thorchain_bridge_names_thorchain_and_no_id():
    """THORChain's explorer finds a swap by its inbound tx, the source leg: no
    separate transfer id (as the adapter's THORChain rows)."""
    bridge = combine_bridge_transfers(
        BridgeSendTransfer(
            fromAddress="0x" + "11" * 20,
            fromAsset="native",
            fromAmount=1,
            fromNetwork="eth",
            fromPayment="ab" * 32,
        ),
        BridgeReceiveTransfer(
            toAddress="bc1qexample",
            toAsset="native",
            toAmount=1,
            toNetwork="btc",
            toPayment="cd" * 32,
        ),
    )
    assert named([bridge]) == [("thorchain", None, None)]


async def test_a_symbiosis_bridge_names_symbiosis():
    record = {
        "from_route": [{"token": {"address": ""}, "amount": "5"}],
        "to_route": [{"token": {"address": ""}, "amount": "4"}],
        "from_chain_id": 1,
        "to_chain_id": 728126428,  # trx
        "from_tx_hash": "0x" + "ab" * 32,
        "to_tx_hash": "0x" + "cd" * 32,
        "from_address": "0x" + "11" * 20,
        "to_address": "0x" + "22" * 20,
    }
    with (
        patch.object(
            symbiosis, "_search_symbiosis_api", AsyncMock(return_value=record)
        ),
        patch.object(symbiosis, "_create_payment_identifier", return_value="ab" * 32),
        patch.object(
            symbiosis,
            "_create_payment_identifier_for_receive",
            AsyncMock(return_value="cd" * 32),
        ),
    ):
        bridges = await symbiosis.get_bridges_from_symbiosis_decoded_logs(
            "eth", None, {"tx_hash": "ab" * 32}, [], [], []
        )
    assert named(bridges) == [("symbiosis", None, None)]
