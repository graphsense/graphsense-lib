# from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

import requests
from eth_abi import decode
from eth_hash.auto import keccak

from .accountmodel import strip_0x


@dataclass(frozen=True)
class DexPair:
    t0: str
    t1: Optional[str]
    version: str
    pool_address: str
    pair_id: Optional[str]
    issuer: str
    creation_log: str

    def get_id(self) -> int:
        return hash(str(self))


@dataclass(frozen=True)
class TokenMetadata:
    adr: str
    name: Optional[str]
    ticker: Optional[str]
    decimals: Optional[int]


def get_pair_from_decoded_log(dlog, log_raw):
    name = dlog["name"]
    issuer = dlog["address"]

    creation_log = "0x" + log_raw.tx_hash.hex() + "_L" + str(log_raw.log_index)

    if name == "PairCreated":
        t0 = dlog["parameters"]["token0"]
        t1 = dlog["parameters"]["token1"]
        v = "uni2"
        pool_address = dlog["parameters"]["pair"]
        pair_id = None
    elif name == "PoolCreated":
        t0 = dlog["parameters"]["token0"]
        t1 = dlog["parameters"]["token1"]
        v = "uni3"
        pool_address = dlog["parameters"]["pool"]
        pair_id = None
    elif name == "Initialize":
        t0 = dlog["parameters"]["currency0"]
        t1 = dlog["parameters"]["currency1"]
        v = "uni4"
        pool_address = "0x000000000004444c5dc75cB358380D2e3dE08A90"
        pair_id = dlog["parameters"]["id"]
    elif name == "NewExchange":
        t0 = dlog["parameters"]["token"]
        t1 = None
        pool_address = dlog["parameters"]["exchange"]
        v = "uni1"
        pair_id = None
    else:
        raise ValueError("Trading pair of type {name} not supported")

    return DexPair(t0, t1, v, pool_address, pair_id, issuer, creation_log)


def get_topic(signature: str) -> bytes:
    return keccak(signature.encode("utf-8"))


def get_function_selector(function_signature: str) -> str:
    return f"0x{get_topic(function_signature)[:4].hex()}"


def get_call_payload(to: str, payload: str, for_block: str):
    return {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {"to": to, "data": payload},
            for_block,
        ],
        "id": 1,
    }


def decode_string_result(result):
    return decode(["string"], result)[0]


def decode_bytes32_result(result):
    return decode(["bytes32"], result)[0]


def decode_uint8_result(result):
    return decode(["uint8"], result)[0]


def decode_text_result(data):
    if "result" in data:
        bytes_text = bytes.fromhex(strip_0x(data["result"]))
        if len(bytes_text) == 0:
            text = None
        else:
            try:
                text = decode_string_result(bytes_text)
            except OverflowError:
                # might be byte32 encoded e.g. like 0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2
                text = decode_bytes32_result(bytes_text).decode("utf-8").rstrip("\x00")
    else:
        text = None

    return text


def get_token_details(
    rpc_url: str, address: str, for_block: str = "latest"
) -> TokenMetadata:
    payload = get_call_payload(address, get_function_selector("name()"), for_block)
    response = requests.post(
        rpc_url, json=payload, headers={"Content-Type": "application/json"}
    )
    data = response.json()

    name = decode_text_result(data)

    payload = get_call_payload(address, get_function_selector("symbol()"), for_block)
    response = requests.post(
        rpc_url, json=payload, headers={"Content-Type": "application/json"}
    )
    data = response.json()

    symbol = decode_text_result(data)

    payload = get_call_payload(address, get_function_selector("decimals()"), for_block)
    response = requests.post(
        rpc_url, json=payload, headers={"Content-Type": "application/json"}
    )
    data = response.json()
    if "result" in data:
        bytes_decimals = bytes.fromhex(strip_0x(data["result"]))
        decimals = (
            None if len(bytes_decimals) == 0 else decode_uint8_result(bytes_decimals)
        )
    else:
        decimals = None

    return TokenMetadata(address, name, symbol, decimals)
