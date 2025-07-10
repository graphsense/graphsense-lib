import importlib
import json

import pytest

pytest.importorskip("bitcoinetl")

from bitcoinetl.domain.block import BtcBlock
from bitcoinetl.mappers.transaction_mapper import BtcTransactionMapper

from graphsenselib.ingest.utxo import is_coinjoin

from . import resources

mapper = BtcTransactionMapper()
block = BtcBlock()
block.number = 0
block.timestamp = 0
block.hash = 4


def test_c1():
    with (
        importlib.resources.files(resources)
        .joinpath(
            "ab188e2ae90a6e54dad6200f0d2ef188b723d2b393bbd01e65d753058afcba62.json"
        )
        .open() as f
    ):
        tx = json.load(f)

        converted = mapper.transaction_to_dict(
            mapper.json_dict_to_transaction(tx, block, 0)
        )
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

        converted = mapper.transaction_to_dict(
            mapper.json_dict_to_transaction(tx, block, 0)
        )
        converted["inputs"][0]["addresses"] = ["3QxSEAwy5SirDifNnUYtrTmRFAkXcyF9xR"]
        converted["inputs"][0]["value"] = 2700
        converted["inputs"][1]["addresses"] = ["3KSZHyXAPLNmxs98CsAij8ksAENRkEa3zs"]
        converted["inputs"][1]["value"] = 2700
        converted["inputs"][2]["addresses"] = ["1F89hmmrtonJfAQNAqDmeDadcw7AsZcvXG"]
        converted["inputs"][2]["value"] = 200000
        converted["inputs"][3]["addresses"] = ["1F89hmmrtonJfAQNAqDmeDadcw7AsZcvXG"]
        converted["inputs"][3]["value"] = 200000

        assert is_coinjoin(converted)
