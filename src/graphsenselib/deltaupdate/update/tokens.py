from dataclasses import dataclass

from eth_abi import decode_single
from eth_utils import function_abi_to_4byte_selector, to_hex
from web3 import Web3

from graphsenselib.deltaupdate.update.resources.supported_tokens_eth import (
    SUPPORTED_TOKENS as eth_tokens,
)
from graphsenselib.deltaupdate.update.resources.supported_tokens_trx import (
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
