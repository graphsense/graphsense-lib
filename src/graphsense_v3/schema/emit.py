"""Render the schema model to reviewable ``.sql`` artifacts.

The model is the source of truth -- the validator and the Spark writer both read
it structurally -- but nobody should have to run Python to see what CQL will
actually execute. These files are generated, committed, and guarded by a test
that fails if they drift from the model.
"""

from __future__ import annotations

from pathlib import Path

from graphsense_v3.schema.definitions import raw_account, raw_utxo, derived
from graphsense_v3.schema.model import Family, Kind, Schema
from graphsense_v3.schema.render import render_schema

#: Substituted at apply time, as the v2 loader does with its own placeholder.
KEYSPACE_PLACEHOLDER = "__KEYSPACE__"
REPLICATION_PLACEHOLDER = "__REPLICATION__"

GENERATED_DIR = Path(__file__).parent / "generated"


#: Chains whose account raw schema differs in its chain-specific tables.
ACCOUNT_CHAINS = ("eth", "trx")


def schemas() -> dict[str, Schema]:
    """The distinct schemas.

    Networks of a family differ only in keyspace name, so ``btc`` and ``ltc``
    share a file by construction -- the property the v2 layout lacked, and where
    three column types drifted between ``raw_account_schema.sql`` and
    ``raw_account_trx_schema.sql``. The account raw schema still renders once per
    chain, because traces and the TRC10 tables really are chain-specific; but the
    shared columns come from one definition, so only what genuinely differs can.
    """
    out: dict[str, Schema] = {
        f"{Kind.RAW.value}_{Family.UTXO.value}": raw_utxo(),
        f"{Kind.DERIVED.value}_{Family.UTXO.value}": derived(Family.UTXO),
        f"{Kind.DERIVED.value}_{Family.ACCOUNT.value}": derived(Family.ACCOUNT),
    }
    for chain in ACCOUNT_CHAINS:
        out[f"{Kind.RAW.value}_{Family.ACCOUNT.value}_{chain}"] = raw_account(chain)
    return out


def render_all() -> dict[str, str]:
    return {
        f"{name}.sql": render_schema(
            schema, KEYSPACE_PLACEHOLDER, REPLICATION_PLACEHOLDER
        )
        for name, schema in schemas().items()
    }


def write_all(directory: Path | None = None) -> list[Path]:
    target = directory or GENERATED_DIR
    target.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for filename, cql in render_all().items():
        path = target / filename
        path.write_text(cql, encoding="utf-8")
        written.append(path)
    return written


def main() -> None:
    for path in write_all():
        print(f"wrote {path}")  # noqa: T201


if __name__ == "__main__":
    main()
