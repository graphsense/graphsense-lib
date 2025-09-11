import random
import requests
from collections import defaultdict
from graphsenselib.utils.signature import (
    eth_get_signature_data_from_rpc_json,
    eth_get_vrs_from_rpc_json,
    eth_get_msg_hash_from_signature_data,
    eth_get_msg_hash_and_vrs_from_raw,
    eth_recover_pubkey,
)
from hexbytes import HexBytes
from graphsenselib.utils.generic import custom_json_encoder
import json


def get_rpc(url, m, p):
    payload = {
        "jsonrpc": "2.0",
        "method": m,
        "params": p,
        "id": 1,
    }

    response = requests.post(
        url, json=payload, headers={"Content-Type": "application/json"}
    )
    data = response.json()
    return data


def get_raw_tx(tx_hash, url):
    data = get_rpc(url, "eth_getRawTransactionByHash", [tx_hash])
    if data["result"] is not None:
        return HexBytes(data["result"])
    else:
        return None


def get_tx(tx_hash, url):
    data = get_rpc(url, "eth_getTransactionByHash", [tx_hash])
    return data["result"]


def get_block_by_number(block_number, url):
    data = get_rpc(url, "eth_getBlockByNumber", [block_number, True])
    return data["result"]


if __name__ == "__main__":
    import sys

    rpc = sys.argv[1]

    # sample integers between 0 - 20_000_000
    random.seed(42)
    n = 10

    test_set = defaultdict(list)
    test_set[-1] = []  # for unknown types
    test_set[0] = []  # for legacy txs
    test_set[1] = []  # for EIP-2930 txs
    test_set[2] = []  # for EIP-1559 txs
    test_set[3] = []  # for EIP-4844 txs

    randomlist = random.sample(range(9000000, 20000000), 100)

    for x in randomlist:
        print(f"Processing block {x}")  # noqa: T201

        if all(len(s) >= n for s in test_set.values()):
            print("Done")  # noqa: T201
            break

        for tx in get_block_by_number(x, rpc)["transactions"]:
            tx_hash = tx["hash"]
            tx_raw_bytes = get_raw_tx(tx_hash, rpc)
            tx = tx

            t = tx.get("type", -1)

            if len(test_set[t]) >= n:
                continue

            sdata = eth_get_signature_data_from_rpc_json(tx)
            vrs_rpc = eth_get_vrs_from_rpc_json(tx)
            msg_hash_rpc = eth_get_msg_hash_from_signature_data(sdata)

            msg_hash, vrs = eth_get_msg_hash_and_vrs_from_raw(tx_raw_bytes)

            assert msg_hash_rpc == msg_hash, (
                f"Hash mismatch {msg_hash_rpc.hex()} != {msg_hash.hex()}"
            )
            assert vrs_rpc == vrs, f"VRS mismatch {vrs_rpc} != {vrs}"

            pubkey = eth_recover_pubkey(vrs, msg_hash)

            from_address = pubkey.to_checksum_address()
            recovered_from = str(from_address).lower()

            original_from = tx["from"].lower()

            assert original_from == recovered_from, (
                f"Address mismatch {recovered_from} != {original_from}"
            )

            test_set[t].append(
                (
                    tx_hash,
                    original_from,
                    t,
                    vrs,
                    msg_hash.hex(),
                    pubkey.to_compressed_bytes().hex(),
                    sdata,
                )
            )

            print(f"Got {len(test_set[t])} samples for type {t}")  # noqa: T201

        with open("ecrecover_test_dataset.json", "w") as f:
            json.dump(test_set, default=custom_json_encoder, fp=f)
