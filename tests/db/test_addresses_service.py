from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from graphsenselib.db.asynchronous.services.addresses_service import AddressesService
from graphsenselib.db.asynchronous.services.models import AddressTagResult


INVALID_TRX_ADDRESS = "tm1zxxmzpnbpms2mv1t7nu2b8sqv5bjcf3"


@pytest.mark.asyncio
async def test_get_cross_chain_pubkey_related_addresses_invalid_trx_does_not_fail():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    logger = MagicMock()

    service = AddressesService(
        db=db,
        tags_service=None,
        entities_service=None,
        blocks_service=None,
        rates_service=None,
        logger=logger,
    )

    with patch(
        "graphsenselib.db.asynchronous.services.addresses_service.tron_address_to_evm_string",
        side_effect=ValueError("Invalid checksum"),
    ):
        result = await service.get_cross_chain_pubkey_related_addresses(
            INVALID_TRX_ADDRESS,
            network="trx",
        )

    assert result.addresses == []
    assert result.next_page is None
    db.get_cross_chain_pubkey_related_addresses.assert_awaited_once_with(
        INVALID_TRX_ADDRESS, "trx"
    )
    logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_list_tags_by_address_invalid_trx_pubkey_lookup_does_not_fail():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    db.get_address_entity_id = AsyncMock(return_value=None)

    tags_service = MagicMock()
    tags_service.tagstore = None
    tags_service.list_tags_by_addresses_raw = AsyncMock(return_value=([], True))
    tags_service._get_address_tag_result = MagicMock(
        return_value=AddressTagResult(address_tags=[], next_page=None)
    )

    service = AddressesService(
        db=db,
        tags_service=tags_service,
        entities_service=None,
        blocks_service=None,
        rates_service=None,
        logger=MagicMock(),
    )

    with patch(
        "graphsenselib.db.asynchronous.services.addresses_service.tron_address_to_evm_string",
        side_effect=ValueError("Invalid checksum"),
    ):
        result = await service.list_tags_by_address(
            currency="trx",
            address=INVALID_TRX_ADDRESS,
            tagstore_groups=[],
            cache={},
            include_pubkey_derived_tags=True,
        )

    assert result.address_tags == []
    tags_service.list_tags_by_addresses_raw.assert_awaited_once()
    query_inputs = tags_service.list_tags_by_addresses_raw.call_args.args[0]
    assert len(query_inputs) == 1
    assert query_inputs[0].network == "trx"
    assert query_inputs[0].address == INVALID_TRX_ADDRESS
