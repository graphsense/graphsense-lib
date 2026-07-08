# flake8: noqa
import pickle
from io import StringIO
import pytest
import pandas as pd

from graphsenselib.deltaupdate.update.account.createchanges import (
    prepare_token_exchange_rates_for_ingest,
)
from graphsenselib.deltaupdate.update.account.createdeltas import (
    get_sorted_unique_addresses,
    get_prices,
)
from graphsenselib.deltaupdate.update.account.modelsraw import (
    AccountBlockAdapter,
    AccountLogAdapter,
    AccountTransactionAdapter,
    EthTraceAdapter,
    TrxTraceAdapter,
    TrxTransactionAdapter,
)
from graphsenselib.deltaupdate.update.account.tokens import ERC20Decoder, TokenTransfer

data_eth = """
currency_ticker,assettype,decimals,token_address,peg_currency
USDT,ERC20,6,0xdac17f958d2ee523a2206206994597c13d831ec7,USD
USDC,ERC20,6,0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,USD
WETH,ERC20,18,0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,ETH
DEUR,ERC20,6,0xba3f535bbcccca2a154b573ca6c5a49baae0a3ea,EUR
"""

data_trx = """currency_ticker,assettype,decimals,token_address,coin_equivalent,peg_currency
USDT,TRC20,6,0xa614f803b6fd780986a42c78ec9c7f77e6ded13c,0,USD
USDC,TRC20,6,0x3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe,0,USD
WTRX,TRC20,6,0x891cdb91d149f23b1a45d9c5ca78a88d0cb44c18,1,ETH
"""


currencies = ["trx", "eth"]
folder = "tests/deltaupdate/resources/account"
filetypes = ["transactions", "traces", "logs", "blocks"]

tx_schema = {
    "transaction_index": int,
    "tx_hash": bytes,
    "from_address": bytes,
    "to_address": bytes,
    "value": int,
    "gas_price": int,
    "transaction_type": int,
    "receipt_gas_used": int,
    "receipt_status": int,
    "block_id": int,
}

log_schema = {
    "block_id": int,
    "tx_hash": bytes,
    "log_index": int,
    "address": bytes,
    "topics": list,
    "data": bytes,
}
block_schema = {
    "block_id": int,
    "miner": bytes,
    "base_fee_per_gas": int,
    "gas_used": int,
}

trace_schema = {
    "block_id": int,
    "tx_hash": bytes,
    "trace_index": bytes,
    "from_address": bytes,
    "to_address": bytes,
    "value": int,
    "call_type": str,
    "status": int,
}


def load_data():
    # load txs, traces, logs, and blocks
    # read the jsons
    def loadFile(f, c, ft):
        with open(f"{f}/{c}/{ft}.pkl", "rb") as f:
            return pickle.load(f)

    data = {
        currency: {
            filetype: loadFile(folder, currency, filetype) for filetype in filetypes
        }
        for currency in currencies
    }
    return data


def load_reference_data():
    lengths = {
        "trx": {
            "transactions": 293,
            "traces": 2,
            "logs": 76,
            "blocks": 1,
        },
        "eth": {
            "transactions": 117,
            "traces": 671,
            "logs": 300,
            "blocks": 1,
        },
    }

    blocks = {"trx": 50000011, "eth": 18000011}

    return lengths, blocks


def test_adapters_regression():
    data = load_data()
    lengths_ref, blocks_ref = load_reference_data()
    for currency in currencies:
        data_currency = data[currency]
        transactions, traces, logs, blocks = (
            data_currency["transactions"],
            data_currency["traces"],
            data_currency["logs"],
            data_currency["blocks"],
        )

        if currency == "trx":
            trace_adapter = TrxTraceAdapter()
            transaction_adapter = TrxTransactionAdapter()

        elif currency == "eth":
            trace_adapter = EthTraceAdapter()
            transaction_adapter = AccountTransactionAdapter()

        # convert dictionaries to dataclasses and unify naming
        log_adapter = AccountLogAdapter()
        block_adapter = AccountBlockAdapter()
        traces = trace_adapter.dicts_to_renamed_dataclasses(traces)
        traces = trace_adapter.process_fields_in_list(traces)
        transactions = transaction_adapter.dicts_to_dataclasses(transactions)
        logs = log_adapter.dicts_to_dataclasses(logs)
        blocks = block_adapter.dicts_to_dataclasses(blocks)

        length_ref = lengths_ref[currency]
        assert len(transactions) == length_ref["transactions"]
        assert len(traces) == length_ref["traces"]
        assert len(logs) == length_ref["logs"]
        assert len(blocks) == length_ref["blocks"]

        assert traces[0].block_id == blocks_ref[currency]

        # check that the files have the correct schema
        for transaction in transactions:
            assert isinstance(transaction, transaction_adapter.datamodel)
            assert isinstance(transaction.tx_hash, bytes)
            assert (
                isinstance(transaction.from_address, bytes)
                or transaction.from_address is None
            )
            assert isinstance(transaction.to_address, bytes)
            assert isinstance(transaction.value, int)
            assert isinstance(transaction.gas_price, int)
            assert isinstance(transaction.transaction_type, int)
            assert isinstance(transaction.receipt_gas_used, int)
            assert isinstance(transaction.receipt_status, int)
            assert isinstance(transaction.block_id, int)

        for trace in traces:
            assert isinstance(trace, trace_adapter.datamodel)
            assert isinstance(trace.tx_hash, bytes) or trace.tx_hash is None
            assert isinstance(trace.from_address, bytes) or trace.from_address is None
            assert isinstance(trace.to_address, bytes)
            assert isinstance(trace.value, int)
            assert isinstance(trace.call_type, str) or trace.call_type is None
            assert isinstance(trace.status, int)
            assert isinstance(trace.block_id, int)

        for log in logs:
            assert isinstance(log, log_adapter.datamodel)
            assert isinstance(log.block_id, int)
            assert isinstance(log.tx_hash, bytes)
            assert isinstance(log.log_index, int)
            assert isinstance(log.address, bytes)
            assert isinstance(log.topics, list)
            assert isinstance(log.data, bytes)

        for block in blocks:
            assert isinstance(block, block_adapter.datamodel)
            assert isinstance(block.block_id, int)
            assert isinstance(block.miner, bytes)
            assert isinstance(block.base_fee_per_gas, int)
            assert isinstance(block.gas_used, int)


def test_tokens_detected():
    SUPPORTED_TOKENS = pd.read_csv(StringIO(data_eth))

    pytest.importorskip("web3")

    tokendecoder = ERC20Decoder("eth", SUPPORTED_TOKENS)
    data = load_data()
    eth_data = data["eth"]
    logs = eth_data["logs"]
    log_adapter = AccountLogAdapter()
    logs = log_adapter.dicts_to_dataclasses(logs)
    token_transfers = [tokendecoder.log_to_transfer(log) for log in logs]
    token_transfers = [x for x in token_transfers if x is not None]

    assert len(token_transfers) == 51


def test_token_decoding():
    adapter = AccountLogAdapter()

    pytest.importorskip("web3")

    example_log = {
        "log_index": 0,
        "transaction_index": 1,
        "block_hash": b"\x00\x00\x00\x00\x02\xfa\xf0\xe5A\xeab\x1d\xed\xc7%\x00\x074^"
        b"\x10\xaa5\xe7\xbd\xb7\xa9\x1c\xee\x99\x0f96",
        "address": b"\xa6\x14\xf8\x03\xb6\xfdx\t\x86\xa4,x\xec\x9c\x7fw\xe6\xde\xd1<",
        "data": b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xba\x81@",
        "topics": [
            b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h\xfc7\x8d\xaa\x95+\xa7\xf1c"
            b"\xc4\xa1\x16(\xf5ZM\xf5#\xb3\xef",
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb3\xa8"da\xf0\xe6'
            b"\xa9\xa1\x06?\xeb\xea\x88\xc6\xf6\xa5\xa0\x85~",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0LT\xf6\xb6\xa2"
            b'\x9a\xf0ZT\x95\x8cIt\xc3\x83\xb4\xd9"\xac',
        ],
        "tx_hash": b"\xe0}\xe10\xe5\xc2\xb1\xde\x13\xcd\x88\xee!\xfa\x1e\xca]e\xba\xbb"
        b"\xecVG\xd3\x1c\xb7\x90\x1f\xc92wo",
        "block_id": 50000101,
        "block_id_group": 50000,
        "partition": 500,
        "topic0": b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h\xfc7\x8d\xaa\x95+\xa7\xf1c"
        b"\xc4\xa1\x16(\xf5ZM\xf5#\xb3\xef",
    }

    example_log = adapter.dict_to_dataclass(example_log)
    SUPPORTED_TOKENS = pd.read_csv(StringIO(data_eth))

    decoder = ERC20Decoder("eth", SUPPORTED_TOKENS)
    decoded_transfer = decoder.log_to_transfer(example_log)
    assert decoded_transfer is None

    example_log = {
        "log_index": 0,
        "transaction_index": 1,
        "block_hash": b"\x00\x00\x00\x00\x02\xfa\xf0\xe5A\xeab\x1d\xed\xc7%\x00\x074^"
        b"\x10\xaa5\xe7\xbd\xb7\xa9\x1c\xee\x99\x0f96",
        "address": bytes.fromhex("dac17f958d2ee523a2206206994597c13d831ec7"),
        "data": b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xba\x81@",
        "topics": [
            b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h"
            b"\xfc7\x8d\xaa\x95+\xa7\xf1c\xc4\xa1"
            b"\x16(\xf5ZM\xf5#\xb3\xef",
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb3\xa8"da\xf0\xe6\xa9'
            b"\xa1\x06?\xeb\xea\x88\xc6\xf6\xa5\xa0\x85~",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0LT\xf6\xb6\xa2\x9a"
            b'\xf0ZT\x95\x8cIt\xc3\x83\xb4\xd9"\xac',
        ],
        "tx_hash": b"\xe0}\xe10\xe5\xc2\xb1\xde\x13\xcd\x88\xee!\xfa\x1e\xca]e\xba\xbb"
        b"\xecVG\xd3\x1c\xb7\x90\x1f\xc92wo",
        "block_id": 50000101,
        "block_id_group": 50000,
        "partition": 500,
        "topic0": b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h\xfc7\x8d\xaa\x95+\xa7\xf1c"
        b"\xc4\xa1\x16(\xf5ZM\xf5#\xb3\xef",
    }

    example_log = adapter.dict_to_dataclass(example_log)
    decoder = ERC20Decoder("eth", SUPPORTED_TOKENS)
    decoded_transfer = decoder.log_to_transfer(example_log)
    check = TokenTransfer(
        from_address=bytes.fromhex("B3a8226461F0e6A9a1063fEBeA88C6f6A5a0857E"),
        to_address=bytes.fromhex("F04C54F6b6A29aF05A54958c4974C383B4D922ac"),
        value=29000000,
        asset="USDT",
        coin_equivalent=0,
        usd_equivalent=1,
        eur_equivalent=0,
        block_id=50000101,
        tx_hash=b"\xe0}\xe10\xe5\xc2\xb1\xde\x13\xcd\x88\xee!\xfa\x1e\xca]e\xba\xbb"
        b"\xecVG\xd3\x1c\xb7\x90\x1f\xc92wo",
        log_index=0,
        decimals=6,
    )

    euro_value, dollar_value = get_prices(
        decoded_transfer.value,
        decoded_transfer.decimals,
        [1, 2],
        decoded_transfer.usd_equivalent,
        decoded_transfer.eur_equivalent,
        decoded_transfer.coin_equivalent,
    )
    assert float(dollar_value) == 29.0
    assert float(euro_value) == 14.5

    assert decoded_transfer == check

    SUPPORTED_TOKENS = pd.read_csv(StringIO(data_trx))
    decoder = ERC20Decoder("trx", SUPPORTED_TOKENS)
    decoded_transfer = decoder.log_to_transfer(example_log)
    assert decoded_transfer is None


def test_address_sorting():
    class SortableAssetTransfer:
        def __init__(
            self,
            from_address=None,
            to_address=None,
            block_id=None,
            log_index=None,
            trace_index=None,
            transaction_index=None,
        ):
            self.from_address = from_address
            self.to_address = to_address
            self.block_id = block_id
            self.log_index = log_index
            self.trace_index = trace_index
            self.transaction_index = transaction_index

    traces_s = [
        SortableAssetTransfer(
            from_address="0x1", to_address="0x2", block_id=1, trace_index=2
        ),
        SortableAssetTransfer(
            from_address="0x3", to_address="0x4", block_id=2, trace_index=1
        ),
    ]
    reward_traces = [SortableAssetTransfer(to_address="0x5", block_id=3, trace_index=0)]
    token_transfers = [
        SortableAssetTransfer(
            from_address="0x2", to_address="0x3", block_id=1, log_index=1
        ),
        SortableAssetTransfer(
            from_address="0x4", to_address="0x1", block_id=2, log_index=2
        ),
    ]
    transactions = [
        SortableAssetTransfer(
            from_address="0x1", to_address="0x0", block_id=1, transaction_index=1000001
        ),
        SortableAssetTransfer(
            from_address="0x2", to_address="0x4", block_id=2, transaction_index=1000002
        ),
    ]

    expected_addresses = ["0x0", "0x1", "0x2", "0x3", "0x4", "0x5"]

    result_addresses = list(
        get_sorted_unique_addresses(
            traces_s, reward_traces, token_transfers, transactions, []
        )
    )

    assert result_addresses == expected_addresses


def test_token_with_eur_peg():
    pytest.importorskip("web3")
    adapter = AccountLogAdapter()
    SUPPORTED_TOKENS = pd.read_csv(StringIO(data_eth))
    example_log = {
        "log_index": 0,
        "transaction_index": 1,
        "block_hash": b"\x00\x00\x00\x00\x02\xfa\xf0\xe5A\xeab\x1d\xed\xc7%\x00\x074^"
        b"\x10\xaa5\xe7\xbd\xb7\xa9\x1c\xee\x99\x0f96",
        "address": bytes.fromhex("bA3f535bbCcCcA2A154b573Ca6c5A49BAAE0a3ea"),
        "data": b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xba\x81@",
        "topics": [
            b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h"
            b"\xfc7\x8d\xaa\x95+\xa7\xf1c\xc4\xa1"
            b"\x16(\xf5ZM\xf5#\xb3\xef",
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb3\xa8"da\xf0\xe6\xa9'
            b"\xa1\x06?\xeb\xea\x88\xc6\xf6\xa5\xa0\x85~",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0LT\xf6\xb6\xa2\x9a"
            b'\xf0ZT\x95\x8cIt\xc3\x83\xb4\xd9"\xac',
        ],
        "tx_hash": b"\xe0}\xe10\xe5\xc2\xb1\xde\x13\xcd\x88\xee!\xfa\x1e\xca]e\xba\xbb"
        b"\xecVG\xd3\x1c\xb7\x90\x1f\xc92wo",
        "block_id": 50000101,
        "block_id_group": 50000,
        "partition": 500,
        "topic0": b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h\xfc7\x8d\xaa\x95+\xa7\xf1c"
        b"\xc4\xa1\x16(\xf5ZM\xf5#\xb3\xef",
    }

    example_log = adapter.dict_to_dataclass(example_log)
    decoder = ERC20Decoder("eth", SUPPORTED_TOKENS)

    decoded_transfer = decoder.log_to_transfer(example_log)
    check = TokenTransfer(
        from_address=bytes.fromhex("B3a8226461F0e6A9a1063fEBeA88C6f6A5a0857E"),
        to_address=bytes.fromhex("F04C54F6b6A29aF05A54958c4974C383B4D922ac"),
        value=29000000,
        asset="DEUR",
        coin_equivalent=0,
        usd_equivalent=0,
        eur_equivalent=1,
        block_id=50000101,
        tx_hash=b"\xe0}\xe10\xe5\xc2\xb1\xde\x13\xcd\x88\xee!\xfa\x1e\xca]e\xba\xbb"
        b"\xecVG\xd3\x1c\xb7\x90\x1f\xc92wo",
        log_index=0,
        decimals=6,
    )

    euro_value, dollar_value = get_prices(
        decoded_transfer.value,
        decoded_transfer.decimals,
        [1, 2],
        decoded_transfer.usd_equivalent,
        decoded_transfer.eur_equivalent,
        decoded_transfer.coin_equivalent,
    )

    assert float(dollar_value) == 14.5
    assert float(euro_value) == 29.0

    assert decoded_transfer == check


def test_get_prices_unpegged_with_token_rate():
    # token_rate is positional [eur_per_token, usd_per_token]; value in smallest
    # unit with 6 decimals -> 2.5 whole tokens.
    euro, dollar = get_prices(
        2_500_000, 6, [1.0, 1.2], False, False, False, token_rate=[2.0, 2.5]
    )
    assert euro == 5.0
    assert dollar == 6.25


def test_get_prices_unpegged_without_rate_is_zero():
    assert get_prices(2_500_000, 6, [1.0, 1.2], False, False, False) == [0, 0]
    # a partial (None) rate also falls back to zero
    assert get_prices(
        2_500_000, 6, [1.0, 1.2], False, False, False, token_rate=[None, 2.5]
    ) == [0, 0]


def test_prepare_token_exchange_rates_for_ingest():
    token_rates = {("UNI", 100): [2.0, 2.5], ("FOO", 101): [0.1, 0.12]}
    changes = prepare_token_exchange_rates_for_ingest(token_rates)
    assert len(changes) == 2
    assert all(c.table == "token_exchange_rates" for c in changes)
    by_asset = {c.data["asset"]: c.data for c in changes}
    assert by_asset["UNI"] == {
        "asset": "UNI",
        "block_id": 100,
        "fiat_values": [2.0, 2.5],
    }
    assert by_asset["FOO"]["block_id"] == 101


def test_token_with_no_peg_decodes_without_fiat():
    pytest.importorskip("web3")
    adapter = AccountLogAdapter()
    # Same DEUR contract, but configured with an empty (unpegged) peg_currency.
    unpegged_csv = (
        "currency_ticker,assettype,decimals,token_address,peg_currency\n"
        "DEUR,ERC20,6,0xba3f535bbcccca2a154b573ca6c5a49baae0a3ea,\n"
    )
    SUPPORTED_TOKENS = pd.read_csv(StringIO(unpegged_csv))
    example_log = {
        "log_index": 0,
        "transaction_index": 1,
        "block_hash": b"\x00\x00\x00\x00\x02\xfa\xf0\xe5A\xeab\x1d\xed\xc7%\x00\x074^"
        b"\x10\xaa5\xe7\xbd\xb7\xa9\x1c\xee\x99\x0f96",
        "address": bytes.fromhex("bA3f535bbCcCcA2A154b573Ca6c5A49BAAE0a3ea"),
        "data": b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\xba\x81@",
        "topics": [
            b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h"
            b"\xfc7\x8d\xaa\x95+\xa7\xf1c\xc4\xa1"
            b"\x16(\xf5ZM\xf5#\xb3\xef",
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb3\xa8"da\xf0\xe6\xa9'
            b"\xa1\x06?\xeb\xea\x88\xc6\xf6\xa5\xa0\x85~",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0LT\xf6\xb6\xa2\x9a"
            b'\xf0ZT\x95\x8cIt\xc3\x83\xb4\xd9"\xac',
        ],
        "tx_hash": b"\xe0}\xe10\xe5\xc2\xb1\xde\x13\xcd\x88\xee!\xfa\x1e\xca]e\xba\xbb"
        b"\xecVG\xd3\x1c\xb7\x90\x1f\xc92wo",
        "block_id": 50000101,
        "block_id_group": 50000,
        "partition": 500,
        "topic0": b"\xdd\xf2R\xad\x1b\xe2\xc8\x9bi\xc2\xb0h\xfc7\x8d\xaa\x95+\xa7\xf1c"
        b"\xc4\xa1\x16(\xf5ZM\xf5#\xb3\xef",
    }

    example_log = adapter.dict_to_dataclass(example_log)
    decoder = ERC20Decoder("eth", SUPPORTED_TOKENS)

    decoded_transfer = decoder.log_to_transfer(example_log)

    # decodes fine, but carries no fiat equivalent
    assert decoded_transfer is not None
    assert decoded_transfer.asset == "DEUR"
    assert decoded_transfer.value == 29000000
    assert decoded_transfer.coin_equivalent == 0
    assert decoded_transfer.usd_equivalent == 0
    assert decoded_transfer.eur_equivalent == 0

    euro_value, dollar_value = get_prices(
        decoded_transfer.value,
        decoded_transfer.decimals,
        [1, 2],
        decoded_transfer.usd_equivalent,
        decoded_transfer.eur_equivalent,
        decoded_transfer.coin_equivalent,
    )
    assert float(dollar_value) == 0.0
    assert float(euro_value) == 0.0


def test_trx_factory_contract_flagged_from_create_trace():
    """A TRON contract deployed by a factory only shows up as a 'create' trace
    (top-level tx is a TriggerSmartContract, so receipt_contract_address is
    null). It must still be flagged as a contract via the trace."""
    from graphsenselib.deltaupdate.update.account.createdeltas import (
        get_contract_creation_deltas_trace,
        is_contract_trace,
    )
    from graphsenselib.deltaupdate.update.account.modelsraw import Trace

    created = b"\x7f[\xd5#Z\"\xeb\xe6\x7f\xf9\xc6\x19\x08&\x01'::\r;p"
    tx_hash = b"\x09\xa0\x4e\x21"

    create_trace = Trace(
        block_id=65717331,
        tx_hash=tx_hash,
        trace_index=1,
        from_address=b"\xc2-\xd1",
        to_address=created,
        value=0,
        call_type="create",
        status=1,
    )
    call_trace = Trace(
        block_id=65717331,
        tx_hash=tx_hash,
        trace_index=2,
        from_address=b"\xc2-\xd1",
        to_address=created,
        value=0,
        call_type="call",
        status=1,
    )

    assert is_contract_trace(create_trace, "TRX") is True
    assert is_contract_trace(call_trace, "TRX") is False

    deltas = get_contract_creation_deltas_trace(
        [create_trace, call_trace], {tx_hash: 42}, "TRX"
    )

    assert len(deltas) == 1
    delta = deltas[0]
    assert delta.identifier == created
    assert delta.is_contract is True
    assert delta.first_tx_id == 42
    # no value / tx-count side effects
    assert delta.no_incoming_txs == 0
    assert delta.no_outgoing_txs == 0
    assert delta.total_received.value == 0
    assert delta.total_spent.value == 0


def test_fee_only_sender_entity_delta_is_zero_stat():
    """A fee-only sender (failed-tx-only) must materialize an address row with no
    value flows, no tx participation, and the -1 tx sentinel — just enough to
    anchor its fee balance debit."""
    from graphsenselib.deltaupdate.update.account.createdeltas import (
        get_entitydelta_from_fee_only_sender,
    )

    addr = b"\xab\xcd\xef"
    eda = get_entitydelta_from_fee_only_sender(addr)

    assert eda.identifier == addr
    assert eda.first_tx_id == -1
    assert eda.last_tx_id == -1
    assert eda.total_received.value == 0
    assert eda.total_spent.value == 0
    assert eda.total_tokens_received == {}
    assert eda.total_tokens_spent == {}
    assert eda.no_incoming_txs == 0
    assert eda.no_outgoing_txs == 0
    assert eda.no_incoming_txs_zero_value == 0
    assert eda.no_outgoing_txs_zero_value == 0
    assert eda.is_contract is False


def test_failed_only_sender_fee_debit_conserves_supply():
    """ETH gas fee accounting must conserve supply: the miner is credited the
    fee for every tx (incl. failed), so the payer must be debited too. A sender
    that only appears in a failed tx is absent from the trace-derived address
    set; unless it is added to address_hash_to_id its debit is dropped and the
    fee is minted from nowhere. This is exactly what the fee-only-sender fix
    ensures upstream."""
    from graphsenselib.deltaupdate.update.account.createdeltas import (
        get_balance_deltas,
    )
    from graphsenselib.deltaupdate.update.account.modelsraw import Block, Transaction

    miner = b"\x11" * 20
    sender = b"\x22" * 20  # appears ONLY in the failed tx
    gas_used, gas_price = 21_000, 100

    tx = Transaction(
        transaction_index=0,
        tx_hash=b"\xaa" * 32,
        from_address=sender,
        to_address=b"\x33" * 20,
        value=0,
        gas_price=gas_price,
        transaction_type=0,
        receipt_gas_used=gas_used,
        receipt_status=0,  # failed
        block_id=1,
    )
    block = Block(block_id=1, miner=miner, base_fee_per_gas=0, gas_used=gas_used)

    def total_supply(deltas):
        return sum(d.asset_balances.get("ETH").value for d in deltas)

    # Fixed behavior: sender present in the map -> debit lands -> net zero.
    fixed = get_balance_deltas(
        [], [], [], [], [tx], [block], {miner: 1, sender: 2}, "ETH"
    )
    assert total_supply(fixed) == 0

    # Old behavior: sender missing from the map -> only the miner credit remains,
    # inflating supply by the full fee.
    inflated = get_balance_deltas([], [], [], [], [tx], [block], {miner: 1}, "ETH")
    assert total_supply(inflated) == gas_used * gas_price
