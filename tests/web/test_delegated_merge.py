"""Which rows of the backend's pairing (middleware rule 6) replace a local
half bridge (web/service/txs_service.py): only a complete pair, matched to
exactly one half. The backend also answers half rows (a side "pending" or
"unknown") for the legs it cannot pair; those are dropped, and the local
half is the answer.

Set up as tests/web/test_delegated_conversions.py: the real txs route and
TxsService over a fake database, the middleware in front, an httpx
MockTransport for the backend. No containers.
"""

import httpx
import pytest

from tests.web.test_bridge_pairing_logging import lines_of, the_pairing_line
from tests.web.test_delegated_conversions import (
    ARB_MINT,
    BRIDGE_ROW,
    CCTP_BURN,
    CONVERSIONS,
    HALF_ROW,
    LOCAL_ROW,
    SWAP_ROW,
    local_swap,  # noqa: F401 -- the autouse fixture of that module
    make_client,
    records_of_the_middleware,
)
from tests.web.test_pairing_merge_fixes import merge


def the_merge_fields(records):
    ((_, fields),) = lines_of(records, "bridge_merge")
    return fields


# the backend's half of the burn: it found the eth leg (the one HALF_ROW
# names) but not the arb mint, so it fills that side as graphsense-lib does
ADAPTER_HALF = {
    **BRIDGE_ROW,
    "to_address": "unknown",
    "to_asset": "unknown",
    "to_amount": "0x0",
    "to_asset_transfer": "pending",
    "to_network": "unknown",
    "to_is_supported_asset": False,
}


@pytest.mark.parametrize(
    "half",
    [
        ADAPTER_HALF,
        # the source side unknown: a mint on eth whose burn it did not find
        {
            **BRIDGE_ROW,
            "from_asset_transfer": "unknown",
            "from_network": "unknown",
            "to_asset_transfer": HALF_ROW["from_asset_transfer"],
            "to_network": "eth",
        },
        # a placeholder in another case still names no transfer
        {**ADAPTER_HALF, "to_asset_transfer": "Pending"},
        # a half naming the local swap's leg removes no local row
        {**ADAPTER_HALF, "from_asset_transfer": f"{CCTP_BURN[2:]}_T690"},
    ],
)
def test_adapter_half_row_does_not_replace_local_half(half):
    client, _, _ = make_client(backend_answer=httpx.Response(200, json=[half]))
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.status_code == 200
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    _, fields = the_pairing_line(records)
    assert fields["reason"] == "half"
    assert fields["rows"] == "0"
    assert fields["halves"] == "1"
    merge = the_merge_fields(records)
    assert merge["paired_rows"] == "0"
    assert merge["kept_halves"] == "1"


def test_a_half_row_next_to_a_pair_is_dropped_and_the_pair_taken():
    other_half = {**ADAPTER_HALF, "from_asset_transfer": f"{CCTP_BURN[2:]}_T700"}
    client, _, _ = make_client(
        backend_answer=httpx.Response(200, json=[other_half, BRIDGE_ROW])
    )
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]
    _, fields = the_pairing_line(records)
    assert fields["reason"] == "paired"
    assert fields["rows"] == "1"
    assert fields["halves"] == "1"


def test_two_rows_naming_one_half_keep_the_half():
    other = {**BRIDGE_ROW, "to_asset_transfer": f"{ARB_MINT[2:]}_T9"}
    client, _, _ = make_client(
        backend_answer=httpx.Response(200, json=[BRIDGE_ROW, other])
    )
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, HALF_ROW]
    merge = the_merge_fields(records)
    assert merge["paired_rows"] == "2"
    assert merge["accepted"] == "0"
    assert merge["ambiguous"] == "2"
    assert merge["kept_halves"] == "1"


def test_adjacent_swap_survives_a_pair():
    """The local swap into USDC (_I0 -> _T690) shares no transfer with the
    pair (_T694 -> the arb mint): it stays. The backend's own swap row is
    not taken: the local detection answers swaps."""
    client, _, _ = make_client(
        backend_answer=httpx.Response(200, json=[SWAP_ROW, BRIDGE_ROW])
    )
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]
    merge = the_merge_fields(records)
    assert merge["accepted"] == "1"
    assert merge["local_dropped"] == "0"


def test_pair_replaces_its_half():
    client, _, _ = make_client(backend_answer=httpx.Response(200, json=[BRIDGE_ROW]))
    with records_of_the_middleware() as records:
        response = client.get(CONVERSIONS)
    assert response.json() == [LOCAL_ROW, BRIDGE_ROW]
    _, fields = the_pairing_line(records)
    assert fields["reason"] == "paired"
    assert "halves" not in fields
    merge = the_merge_fields(records)
    assert merge["accepted"] == "1"
    assert merge["kept_halves"] == "0"


@pytest.mark.asyncio
async def test_the_merge_itself_takes_no_half_row():
    """The middleware drops half rows; the merge does not rely on it."""
    assert await merge([HALF_ROW], [ADAPTER_HALF]) == [HALF_ROW]
