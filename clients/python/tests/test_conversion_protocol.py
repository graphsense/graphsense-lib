"""A conversions row may name a bridge protocol this client does not know."""

from __future__ import annotations

import json

from graphsense.api_client import ApiClient


def bridge_row(protocol: str, protocol_app: str) -> dict:
    return {
        "conversion_type": "bridge_tx",
        "from_address": "0x" + "11" * 20,
        "to_address": "0x" + "22" * 20,
        "from_asset": "native",
        "to_asset": "unknown",
        "from_amount": "0x1",
        "to_amount": "0x0",
        "from_asset_transfer": "ab" * 32,
        "to_asset_transfer": "pending",
        "from_network": "eth",
        "to_network": "arb",
        "from_is_supported_asset": True,
        "to_is_supported_asset": False,
        "protocol": protocol,
        "protocol_app": protocol_app,
    }


def test_an_unknown_protocol_keeps_the_conversions():
    """A newer server adds protocols: their rows must not fail the list."""
    text = json.dumps(
        [bridge_row("layerzero", "stargate"), bridge_row("somethingnew", "someapp")]
    )
    rows = ApiClient().deserialize(
        text, "List[ExternalConversion]", "application/json"
    )
    assert [(r.protocol, r.protocol_app) for r in rows] == [
        ("layerzero", "stargate"),
        ("somethingnew", "someapp"),
    ]
