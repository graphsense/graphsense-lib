from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from graphsenselib.db.asynchronous.services.addresses_service import AddressesService
from graphsenselib.db.asynchronous.services.models import AddressTagResult
from graphsenselib.errors import AddressNotFoundException


INVALID_TRX_ADDRESS = "tm1zxxmzpnbpms2mv1t7nu2b8sqv5bjcf3"

BTC_LEGACY_ADDRESS = "13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94"
BTC_BECH32_ADDRESS = "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"
TRX_ADDRESS = "TLa2f6VPqDgRE67v1736s7bJ8Ray5wYjU7"
TRX_ADDRESS_AS_EVM = "0x74472e7d35395a6b5add427eecb7f4b62ad2b071"
ETH_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"


def _make_service(db=None, tags_service=None):
    return AddressesService(
        db=db if db is not None else MagicMock(),
        tags_service=tags_service,
        entities_service=None,
        blocks_service=None,
        rates_service=None,
        logger=MagicMock(),
    )


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
    assert logger.warning.call_count >= 1


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


@pytest.mark.asyncio
async def test_trivial_fork_btc_to_bch_no_pubkey_entry():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    service = _make_service(db=db)

    async def fake_get_address(currency, address, **_kwargs):
        if currency == "bch" and address == BTC_LEGACY_ADDRESS:
            return SimpleNamespace(currency="bch", address=BTC_LEGACY_ADDRESS)
        raise AddressNotFoundException(currency, address)

    with patch.object(service, "get_address", side_effect=fake_get_address):
        result = await service.get_cross_chain_pubkey_related_addresses(
            BTC_LEGACY_ADDRESS, network="btc"
        )

    assert len(result.addresses) == 1
    entry = result.addresses[0]
    assert entry.network == "bch"
    assert entry.address == BTC_LEGACY_ADDRESS
    assert entry.type == "trivial_fork"


@pytest.mark.asyncio
async def test_trivial_fork_bch_cashaddr_to_btc():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    service = _make_service(db=db)

    cashaddr = "bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a"
    legacy_equivalent = "1BpEi6DfDAUFd7GtittLSdBeYJvcoaVggu"

    async def fake_get_address(currency, address, **_kwargs):
        if currency == "btc" and address == legacy_equivalent:
            return SimpleNamespace(currency="btc", address=legacy_equivalent)
        raise AddressNotFoundException(currency, address)

    with patch.object(service, "get_address", side_effect=fake_get_address):
        result = await service.get_cross_chain_pubkey_related_addresses(
            cashaddr, network="bch"
        )

    assert len(result.addresses) == 1
    entry = result.addresses[0]
    assert entry.network == "btc"
    assert entry.address == legacy_equivalent
    assert entry.type == "trivial_fork"


@pytest.mark.asyncio
async def test_trivial_fork_skips_bech32():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    service = _make_service(db=db)

    get_address_mock = AsyncMock()
    with patch.object(service, "get_address", get_address_mock):
        result = await service.get_cross_chain_pubkey_related_addresses(
            BTC_BECH32_ADDRESS, network="btc"
        )

    assert result.addresses == []
    get_address_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_trivial_fork_skipped_when_other_chain_missing():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    service = _make_service(db=db)

    async def fake_get_address(currency, address, **_kwargs):
        raise AddressNotFoundException(currency, address)

    with patch.object(service, "get_address", side_effect=fake_get_address):
        result = await service.get_cross_chain_pubkey_related_addresses(
            BTC_LEGACY_ADDRESS, network="btc"
        )

    assert result.addresses == []


@pytest.mark.asyncio
async def test_trivial_evm_trx_to_eth():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    service = _make_service(db=db)

    async def fake_get_address(currency, address, **_kwargs):
        if currency == "eth" and address.lower() == TRX_ADDRESS_AS_EVM:
            return SimpleNamespace(currency="eth", address=TRX_ADDRESS_AS_EVM)
        raise AddressNotFoundException(currency, address)

    with patch.object(service, "get_address", side_effect=fake_get_address):
        result = await service.get_cross_chain_pubkey_related_addresses(
            TRX_ADDRESS, network="trx"
        )

    assert len(result.addresses) == 1
    entry = result.addresses[0]
    assert entry.network == "eth"
    assert entry.type == "trivial_evm"


@pytest.mark.asyncio
async def test_trivial_evm_eth_to_trx():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    service = _make_service(db=db)

    captured = {}

    async def fake_get_address(currency, address, **_kwargs):
        if currency == "trx":
            captured["address"] = address
            return SimpleNamespace(currency="trx", address=address)
        raise AddressNotFoundException(currency, address)

    with patch.object(service, "get_address", side_effect=fake_get_address):
        result = await service.get_cross_chain_pubkey_related_addresses(
            ETH_ADDRESS, network="eth"
        )

    assert len(result.addresses) == 1
    entry = result.addresses[0]
    assert entry.network == "trx"
    assert entry.type == "trivial_evm"
    # Sanity: TRX addresses are Base58 starting with 'T'.
    assert captured["address"].startswith("T")


@pytest.mark.asyncio
async def test_trivial_overlap_dedupes_against_pubkey_results():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(
        return_value=[
            {
                "currency": "bch",
                "address": BTC_LEGACY_ADDRESS,
                "type": "p2pkh",
                "pubkey": b"\x00" * 33,
            }
        ]
    )
    service = _make_service(db=db)

    async def fake_get_address(currency, address, **_kwargs):
        if currency == "bch" and address == BTC_LEGACY_ADDRESS:
            return SimpleNamespace(currency="bch", address=BTC_LEGACY_ADDRESS)
        raise AddressNotFoundException(currency, address)

    with patch.object(service, "get_address", side_effect=fake_get_address):
        result = await service.get_cross_chain_pubkey_related_addresses(
            BTC_LEGACY_ADDRESS, network="btc"
        )

    bch_entries = [a for a in result.addresses if a.network == "bch"]
    assert len(bch_entries) == 1
    assert bch_entries[0].type == "p2pkh"


@pytest.mark.asyncio
async def test_trivial_evm_invalid_trx_does_not_fail():
    db = MagicMock()
    db.get_cross_chain_pubkey_related_addresses = AsyncMock(return_value=[])
    service = _make_service(db=db)

    get_address_mock = AsyncMock()
    with patch.object(service, "get_address", get_address_mock):
        result = await service.get_cross_chain_pubkey_related_addresses(
            INVALID_TRX_ADDRESS, network="trx"
        )

    assert result.addresses == []
    get_address_mock.assert_not_awaited()
