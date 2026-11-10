"""Pair only the local halves named unambiguously by the backend."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from graphsenselib.db.asynchronous.services.models import ExternalConversion
from graphsenselib.db.asynchronous.services.txs_service import TxConversions, TxsService
from graphsenselib.web.service.txs_service import get_tx_conversions

from tests.defi.test_delegated_bridges import db_rows
from tests.web.test_delegated_conversions import (
    BRIDGE_ROW,
    CCTP_BURN,
    HALF_ROW,
    LOCAL_ROW,
    fake_db,
)


def context(result):
    return SimpleNamespace(
        services=SimpleNamespace(
            txs_service=SimpleNamespace(get_conversions=AsyncMock(return_value=result))
        ),
        config=SimpleNamespace(included_bridges=("cctp",), cow_protocol_swaps=False),
    )


async def merge(halves, paired, rows=()):
    result = TxConversions(
        tuple(ExternalConversion(**{**r, "conversion_type": "swap"}) for r in rows),
        tuple(ExternalConversion(**{**r, "conversion_type": "bridge"}) for r in halves),
    )
    pair = AsyncMock(return_value=[ExternalConversion(**r) for r in paired])
    answer = await get_tx_conversions(context(result), "eth", CCTP_BURN, pair)
    pair.assert_awaited_once_with()
    return [r.to_dict() for r in answer]


@pytest.mark.asyncio
async def test_unrequested_stargate_pair_keeps_cctp_half():
    stargate = {**BRIDGE_ROW, "from_asset_transfer": f"{CCTP_BURN}_T700"}
    assert await merge([HALF_ROW], [stargate], [LOCAL_ROW]) == [LOCAL_ROW, HALF_ROW]


@pytest.mark.asyncio
async def test_only_one_of_two_burns_is_replaced():
    other = {**HALF_ROW, "from_asset_transfer": f"{CCTP_BURN}_T794"}
    assert await merge([HALF_ROW, other], [BRIDGE_ROW]) == [other, BRIDGE_ROW]


@pytest.mark.asyncio
@pytest.mark.parametrize("side", ["from", "to"])
@pytest.mark.parametrize("suffix", ["_T694", "_I12", ""])
async def test_exact_normalized_leg_replaces_half(side, suffix):
    pair = dict(BRIDGE_ROW)
    if side == "to":
        for key in BRIDGE_ROW:
            if key.startswith("from_"):
                other = key.replace("from_", "to_", 1)
                pair[key], pair[other] = BRIDGE_ROW[other], BRIDGE_ROW[key]
    pair[f"{side}_asset_transfer"] = CCTP_BURN[2:] + suffix
    half = {**pair, f"{side}_asset_transfer": "0X" + CCTP_BURN[2:].upper() + suffix}
    half[f"{'to' if side == 'from' else 'from'}_asset_transfer"] = "pending"
    assert await merge([half], [pair]) == [pair]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates",
    [
        {"from_network": "arb"},
        {"from_asset_transfer": CCTP_BURN},
        {"from_asset_transfer": f"{CCTP_BURN}_I694"},
        {
            "from_asset_transfer": "pending",
            "to_asset_transfer": HALF_ROW["from_asset_transfer"],
        },
    ],
)
async def test_wrong_network_identifier_or_side_does_not_match(updates):
    assert await merge([HALF_ROW], [{**BRIDGE_ROW, **updates}]) == [HALF_ROW]


@pytest.mark.asyncio
@pytest.mark.parametrize("half_count,pair_count", [(2, 1), (1, 2), (2, 2)])
async def test_ambiguous_matches_keep_halves(half_count, pair_count):
    halves = [HALF_ROW] * half_count
    assert await merge(halves, [BRIDGE_ROW] * pair_count) == halves


@pytest.mark.asyncio
async def test_accepted_pair_drops_local_row_naming_its_leg():
    local = {
        **LOCAL_ROW,
        "to_asset_transfer": "0X" + HALF_ROW["from_asset_transfer"].upper(),
    }
    assert await merge([HALF_ROW], [BRIDGE_ROW], [local]) == [BRIDGE_ROW]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix,traces,whole_tx,kwargs,has_half",
    [
        ("", [], True, {}, True),
        (
            "_I7",
            [{"trace_index": 7, "trace_address": ""}],
            True,
            {"tx_identifier": CCTP_BURN},
            True,
        ),
        (
            "_I7",
            [{"trace_index": 7, "trace_address": None}],
            True,
            {"tx_identifier": CCTP_BURN},
            True,
        ),
        ("_I0", [{"trace_index": 0, "trace_address": "0"}], False, {}, False),
        ("_I7", [], False, {}, False),
        ("_T694", [], False, {}, True),
    ],
)
async def test_db_whole_tx_controls_pairing_identifier(
    suffix, traces, whole_tx, kwargs, has_half
):
    db = fake_db(db_rows("v2_burn"), (CCTP_BURN,))
    db.fetch_transaction_traces = AsyncMock(
        return_value=[
            {
                "call_type": "call",
                "from_address": bytes(20),
                "to_address": bytes(20),
                "value": 0,
                **trace,
            }
            for trace in traces
        ]
    )
    service = TxsService(db=db, rates_service=MagicMock(), logger=MagicMock())
    identifier = CCTP_BURN + suffix
    result = await service.get_conversions(
        "eth", identifier, included_bridges=("cctp",), cow_protocol_swaps=False
    )
    assert result.whole_tx is whole_tx
    assert bool(result.half_bridges) is has_half
    pair = AsyncMock(return_value=[ExternalConversion(**BRIDGE_ROW)])
    await get_tx_conversions(context(result), "eth", identifier, pair)
    if has_half:
        pair.assert_awaited_once_with(**kwargs)
    else:
        pair.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_root_internal_half_calls_pair_without_override():
    half = {**HALF_ROW, "from_asset_transfer": f"{CCTP_BURN}_I7"}
    result = TxConversions(half_bridges=(ExternalConversion(**half),))
    pair = AsyncMock(return_value=[])
    await get_tx_conversions(context(result), "eth", f"{CCTP_BURN}_I7", pair)
    pair.assert_awaited_once_with()
