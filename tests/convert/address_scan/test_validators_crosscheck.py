"""Cross-check the self-contained validators against graphsenselib's.

``convert/address_scan/validators.py`` is a stdlib-only reimplementation that
is vendored into the standalone graphsense-python client (graphsenselib itself
uses ``utils/address.py``). Two implementations can drift, so this test asserts
they return the *same* verdict for every currency over a broad corpus: known
valid addresses, checksum/case mutations of them, and random noise.
"""

from __future__ import annotations

import random

import pytest

from graphsenselib.convert.address_scan import validators as sc
from graphsenselib.utils import address as lib

CURRENCIES = ["btc", "ltc", "eth", "trx", "zec", "xrp"]

# Known-valid addresses (deterministically generated / canonical examples).
VALID = [
    "1KUCzSr49wPWckUDouJLybJuRYtViF5hfM",  # btc p2pkh
    "37MaqGePco8NRnndHVmm85hZHwqEtL2LUv",  # btc p2sh
    "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",  # btc bech32 v0
    "LPTmJ8osYPoZ5ScvRPvEKYuzNNi6VWpj5N",  # ltc L
    "MA9cnkeQdk43rto7ctrYiX4tLczEJZke1V",  # ltc M
    "TFkqQFpGQm7BkCBePHoxDziowfd2uW89vN",  # trx
    "t1MGDPc4oDip1MzypGrfKSb6Wb3UwRwL6Xk",  # zec t1
    "t3dFnE2T3v5Utkx1SFDhhZL1ES4LoViANp1",  # zec t3
    "0xaAA9402664f1a41F40EBbC52c9993eb66AeB3666",  # eth EIP-55 mixed
    "0x52908400098527886e0f7030069857d2e4169ee7",  # eth all-lower
    "0x52908400098527886E0F7030069857D2E4169EE7",  # eth all-upper
    "rMHQtBt4suHhw77cLCPX9d2MVhf9JUQ8JZ",  # xrp
    "rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh",  # xrp
]


def _mutations(addr: str) -> list[str]:
    """Checksum/case/length perturbations that stress the validators."""
    out = [addr, addr.lower(), addr.upper(), addr.swapcase(), addr[:-1], addr + "x"]
    if len(addr) > 5:
        # flip one interior char to a neighbour in the same rough class
        i = len(addr) // 2
        repl = "1" if addr[i] != "1" else "2"
        out.append(addr[:i] + repl + addr[i + 1 :])
    return out


def _random_corpus(n: int) -> list[str]:
    rng = random.Random(1234)
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz0x"
    prefixes = ["", "1", "3", "L", "M", "T", "t1", "t3", "r", "0x", "bc1", "ltc1"]
    out = []
    for _ in range(n):
        pre = rng.choice(prefixes)
        body = "".join(rng.choice(alphabet) for _ in range(rng.randint(20, 60)))
        out.append(pre + body)
    return out


@pytest.fixture(scope="module")
def corpus() -> list[str]:
    items: list[str] = []
    for addr in VALID:
        items.extend(_mutations(addr))
    items.extend(_random_corpus(400))
    return items


@pytest.mark.parametrize("currency", CURRENCIES)
def test_selfcontained_matches_graphsenselib(currency, corpus):
    disagreements = []
    for s in corpus:
        got = sc.validate_address(currency, s)
        want = lib.validate_address(currency, s)
        if got != want:
            disagreements.append((s, got, want))
    assert not disagreements, (
        f"{currency}: {len(disagreements)} disagreements, first few: "
        f"{disagreements[:5]}"
    )


def test_xrp_direct_functions_agree(corpus):
    for s in corpus:
        assert sc.validate_xrp_address(s) == lib.validate_xrp_address(s), s


def test_every_valid_fixture_validates_on_both():
    checks = {
        "1KUCzSr49wPWckUDouJLybJuRYtViF5hfM": "btc",
        "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4": "btc",
        "LPTmJ8osYPoZ5ScvRPvEKYuzNNi6VWpj5N": "ltc",
        "TFkqQFpGQm7BkCBePHoxDziowfd2uW89vN": "trx",
        "t1MGDPc4oDip1MzypGrfKSb6Wb3UwRwL6Xk": "zec",
        "0xaAA9402664f1a41F40EBbC52c9993eb66AeB3666": "eth",
        "rMHQtBt4suHhw77cLCPX9d2MVhf9JUQ8JZ": "xrp",
    }
    for addr, cur in checks.items():
        assert sc.validate_address(cur, addr) is True, (cur, addr)
        assert lib.validate_address(cur, addr) is True, (cur, addr)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
