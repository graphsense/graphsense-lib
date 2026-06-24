"""The fork registry (config.chain_forks) is the single source of truth for
fork-awareness — both the pubkey job and the REST fork-overlap handler derive
from it."""

from graphsenselib.config import chain_forks


def test_chain_forks_registry():
    assert chain_forks["bch"] == {"base": "btc", "fork_block": 478558}


def test_pubkey_job_fork_block_comes_from_registry():
    from graphsenselib.pubkey.job import BCH_FORK_BLOCK

    assert BCH_FORK_BLOCK == chain_forks["bch"]["fork_block"]


def test_fork_tuples_derived_from_registry():
    from graphsenselib.db.asynchronous.services.addresses_service import FORK_TUPLES

    assert FORK_TUPLES == [(spec["base"], fork) for fork, spec in chain_forks.items()]
    assert ("btc", "bch") in FORK_TUPLES
