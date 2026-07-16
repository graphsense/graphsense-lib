"""The UTXO address normalizer must be importable on Spark executors.

The delta-to-raw normalization UDF pickles ``normalize_base58_p2pkh`` by
module reference, so executors import its defining module. The baked
spark-env (Dockerfile: wheel ``--no-deps`` + crypto stack) has no
methodtools/cassandra-driver/RPC deps — importing it from
``graphsenselib.ingest.utxo`` crashes every UDF task with
ModuleNotFoundError. The function therefore lives in a lean module whose
import pulls in no ingest machinery.
"""

import subprocess
import sys

from graphsenselib.utils.pubkey_to_address import base58check_encode

HASH160 = bytes.fromhex("ab" * 20)


def test_normalizer_module_import_is_lean():
    # Fresh interpreter: importing the normalizer's home module must not
    # drag in ingest-only dependencies (absent from the spark-env archive).
    code = (
        "import sys; import graphsenselib.utils.utxo_address; "
        "heavy = {'methodtools', 'cassandra', 'grpc', 'bitcoinetl', "
        "'graphsenselib.ingest', 'graphsenselib.db', 'graphsenselib.config'}; "
        "loaded = heavy & set(sys.modules); "
        "assert not loaded, f'lean import pulled in {sorted(loaded)}'"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_normalizer_behavior_from_lean_module():
    from graphsenselib.utils.utxo_address import normalize_base58_p2pkh

    btc_form = base58check_encode(b"\x00", HASH160)
    assert normalize_base58_p2pkh(btc_form, "ltc") == base58check_encode(
        b"\x30", HASH160
    )
    assert normalize_base58_p2pkh(btc_form, "btc") == btc_form
    assert normalize_base58_p2pkh(None, "ltc") is None


def test_ingest_reexport_is_same_object():
    # Ingest-side callers and the parity test keep importing from
    # ingest.utxo; both names must be the one implementation.
    from graphsenselib.ingest import utxo as ingest_utxo
    from graphsenselib.utils import utxo_address

    assert ingest_utxo.normalize_base58_p2pkh is utxo_address.normalize_base58_p2pkh
    assert ingest_utxo._NETWORK_SCRIPT_PARAMS is utxo_address._NETWORK_SCRIPT_PARAMS


def test_transformation_imports_the_lean_module():
    # The UDF closure must reference the lean module, not ingest.utxo —
    # that reference is what the executor unpickles and imports.
    import graphsenselib.transformation.utxo as tutxo

    assert tutxo.normalize_base58_p2pkh.__module__ == "graphsenselib.utils.utxo_address"
