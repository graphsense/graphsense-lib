"""graphsense-lib's transformed layout defaults must match the Spark job's.

`transformation raw-to-transformed` passes graphsense-lib's defaults to the job
explicitly, but anything that calls the job directly (spark/docker/submit.sh,
hand-written run scripts) gets the job's per-network defaults from
spark/.../config/NetworkDefaults.scala. If the two tables drift, the same
network gets a different keyspace layout depending on how the job was started.
The Scala file is parsed as text so this runs without sbt.
"""

import re
from pathlib import Path

import pytest

from graphsenselib.config.config import get_default_data_configuration

NETWORK_DEFAULTS = (
    Path(__file__).parents[2]
    / "spark/src/main/scala/org/graphsense/config/NetworkDefaults.scala"
)

# NetworkDefaults field -> data_configuration key
SCALA_TO_PYTHON = {
    "bucketSize": "bucket_size",
    "addressPrefixLength": "address_prefix_length",
    "bech32Prefix": "bech_32_prefix",
    "coinjoinFiltering": "coinjoin_filtering",
    "txPrefixLength": "tx_prefix_length",
    "blockBucketSizeAddressTxs": "block_bucket_size_address_txs",
    "addressrelationsIdsNbuckets": "addressrelations_ids_nbuckets",
}


def _scala_value(literal):
    if literal.startswith('"'):
        return literal[1:-1]
    if literal in ("true", "false"):
        return literal == "true"
    return int(literal)


def _scala_layouts():
    source = NETWORK_DEFAULTS.read_text()
    layouts = {}
    for network, fields in re.findall(
        r'"(\w+)"\s*->\s*(?:Utxo|Account)Layout\((.*?)\)', source, re.DOTALL
    ):
        layouts[network] = {
            SCALA_TO_PYTHON[name]: _scala_value(value)
            for name, value in re.findall(r'(\w+)\s*=\s*("[^"]*"|\w+)', fields)
        }
    return layouts


SCALA_LAYOUTS = _scala_layouts()


def test_scala_layouts_parsed():
    assert set(SCALA_LAYOUTS) == {"btc", "ltc", "bch", "zec", "eth", "trx"}


@pytest.mark.parametrize("network", sorted(SCALA_LAYOUTS))
def test_layout_defaults_match(network):
    python = get_default_data_configuration(network, "transformed")
    python_layout = {k: v for k, v in python.items() if k in SCALA_TO_PYTHON.values()}

    assert python_layout == SCALA_LAYOUTS[network]
