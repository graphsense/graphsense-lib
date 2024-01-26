from dataclasses import dataclass

from eth_abi import decode_single
from eth_utils import function_abi_to_4byte_selector, to_hex
from web3 import Web3

from src.graphsenselib.deltaupdate.update.resources.supported_tokens_eth import (
    SUPPORTED_TOKENS as eth_tokens,
)
from src.graphsenselib.deltaupdate.update.resources.supported_tokens_trx import (
    SUPPORTED_TOKENS as trx_tokens,
)


@dataclass
class TokenTransfer:
    from_address: bytes
    to_address: bytes
    value: int
    asset: str
    decimals: int
    coin_equivalent: int
    usd_equivalent: float
    block_id: int
    tx_hash: bytes
    log_index: int


class ERC20Decoder:
    def __init__(self, network="eth"):
        self.w3 = Web3()

        self.token_transfer_event_abi = {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "from", "type": "address"},
                {"indexed": True, "name": "to", "type": "address"},
                {"indexed": False, "name": "value", "type": "uint256"},
            ],
            "name": "Transfer",
            "type": "event",
        }

        self.token_transfer_event_selector = self.get_event_selector(
            self.token_transfer_event_abi
        )
        self.network = network

        # todo this should be in a config file
        if self.network == "eth":
            self.supported_tokens = eth_tokens
        elif self.network == "trx":
            self.supported_tokens = trx_tokens
        else:
            raise Exception("Unsupported network")

    def get_event_selector(self, event_abi):
        return to_hex(function_abi_to_4byte_selector(event_abi))

    def log_to_transfer(self, log):
        if "0x" + log.address.hex() in self.supported_tokens["address"].values:
            return self.decode_transfer(log)

    def decode_transfer(self, log):
        if "0x" + log.topics[0].hex()[:8] == self.token_transfer_event_selector:
            if "0x" + log.address.hex() not in self.supported_tokens["address"].values:
                raise Exception(
                    "Unsupported token, use the log_to_transfer function instead"
                )

            try:
                sender = bytes.fromhex(
                    self.w3.toChecksumAddress(decode_single("address", log.topics[1]))[
                        2:
                    ]
                )
                recipient = bytes.fromhex(
                    self.w3.toChecksumAddress(decode_single("address", log.topics[2]))[
                        2:
                    ]
                )
                value = decode_single("uint256", log.data)
                mask = self.supported_tokens["address"] == "0x" + log.address.hex()
                asset = self.supported_tokens[mask]["asset"].values[0]
                coin_equivalent = self.supported_tokens[mask]["coin_equivalent"].values[
                    0
                ]
                usd_equivalent = self.supported_tokens[mask]["usd_equivalent"].values[0]
                decimals = self.supported_tokens[mask]["decimals"].values[0]

                return TokenTransfer(
                    from_address=sender,
                    to_address=recipient,
                    value=value,
                    asset=asset,
                    decimals=decimals,
                    coin_equivalent=coin_equivalent,
                    usd_equivalent=usd_equivalent,
                    block_id=log.block_id,
                    tx_hash=log.tx_hash,
                    log_index=log.log_index,
                )
            except Exception:
                return None  # cant be decoded
        else:
            return None  # not a transfer event


if __name__ == "__main__":
    from src.graphsenselib.utils.adapters import AccountLogAdapter

    adapter = AccountLogAdapter()
    # todo move to tests
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

    decoder = ERC20Decoder("eth")
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
    decoder = ERC20Decoder("eth")
    decoded_transfer = decoder.log_to_transfer(example_log)
    check = TokenTransfer(
        from_address=bytes.fromhex("B3a8226461F0e6A9a1063fEBeA88C6f6A5a0857E"),
        to_address=bytes.fromhex("F04C54F6b6A29aF05A54958c4974C383B4D922ac"),
        value=29000000,
        asset="USDT",
        coin_equivalent=0,
        usd_equivalent=1,
        block_id=50000101,
        tx_hash=b"\xe0}\xe10\xe5\xc2\xb1\xde\x13\xcd\x88\xee!\xfa\x1e\xca]e\xba\xbb"
        b"\xecVG\xd3\x1c\xb7\x90\x1f\xc92wo",
        log_index=0,
        decimals=6,
    )

    assert decoded_transfer == check

    decoder = ERC20Decoder("trx")
    decoded_transfer = decoder.log_to_transfer(example_log)
    assert decoded_transfer is None
