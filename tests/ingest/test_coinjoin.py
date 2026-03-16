import importlib
import json

from graphsenselib.ingest.btc import _parse_input, _parse_output
from graphsenselib.ingest.utxo import is_coinjoin

from . import resources


def raw_tx_to_dict(raw_tx):
    """Convert a raw Bitcoin RPC transaction JSON to the dict format expected by
    is_coinjoin (same format as BtcBlockExporter output)."""
    vin = raw_tx.get("vin", [])
    vout = raw_tx.get("vout", [])

    inputs = []
    idx = 0
    for v in vin:
        if "coinbase" not in v:
            inputs.append(_parse_input(v, idx))
            idx += 1

    outputs = [_parse_output(v) for v in vout]

    input_value = sum(inp["value"] for inp in inputs if inp["value"] is not None)
    output_value = sum(o["value"] for o in outputs if o["value"] is not None)

    return {
        "hash": raw_tx["txid"],
        "inputs": inputs,
        "outputs": outputs,
        "input_count": len(inputs),
        "output_count": len(outputs),
        "input_value": input_value,
        "output_value": output_value,
        "is_coinbase": False,
        "block_number": 0,
        "block_timestamp": 0,
        "index": 0,
    }


def test_c1():
    with (
        importlib.resources.files(resources)
        .joinpath(
            "ab188e2ae90a6e54dad6200f0d2ef188b723d2b393bbd01e65d753058afcba62.json"
        )
        .open() as f
    ):
        tx = json.load(f)

        converted = raw_tx_to_dict(tx)
        converted["inputs"][0]["addresses"] = ["1EFabZzqMDZALgJmZ7DMHdLj9SPRSuXAvU"]
        converted["inputs"][0]["value"] = 600
        converted["inputs"][1]["addresses"] = ["12JYmnfYU2ghzjwUAspzJsSnmJtK9bZPYR"]
        converted["inputs"][1]["value"] = 5277446

        assert is_coinjoin(converted)


def test_c2():
    with (
        importlib.resources.files(resources)
        .joinpath(
            "59d2780701c8352ad77c026652d67ea596fe4e9316580f02c171e2137fa91578.json",
        )
        .open() as f
    ):
        tx = json.load(f)

        converted = raw_tx_to_dict(tx)
        converted["inputs"][0]["addresses"] = ["3QxSEAwy5SirDifNnUYtrTmRFAkXcyF9xR"]
        converted["inputs"][0]["value"] = 2700
        converted["inputs"][1]["addresses"] = ["3KSZHyXAPLNmxs98CsAij8ksAENRkEa3zs"]
        converted["inputs"][1]["value"] = 2700
        converted["inputs"][2]["addresses"] = ["1F89hmmrtonJfAQNAqDmeDadcw7AsZcvXG"]
        converted["inputs"][2]["value"] = 200000
        converted["inputs"][3]["addresses"] = ["1F89hmmrtonJfAQNAqDmeDadcw7AsZcvXG"]
        converted["inputs"][3]["value"] = 200000

        assert is_coinjoin(converted)
