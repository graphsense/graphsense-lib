"""Schema as data.

The v2 schema lives in hand-maintained ``.sql`` files, one per (family, kind).
Keeping several near-identical files in sync by hand is what let three column
types silently drift between the two raw account schemas, and what left two
byte-identical TRX migration files to be updated in lockstep. Here a schema is a
value: one definition, rendered per network.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Family(str, Enum):
    """Which shape a network's schema takes."""

    UTXO = "utxo"
    ACCOUNT = "account"


class Kind(str, Enum):
    """Which keyspace of a network's pair."""

    RAW = "raw"
    TRANSFORMED = "transformed"


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    comment: str | None = None


@dataclass(frozen=True)
class Key:
    """A primary key.

    ``partition`` is the partition key; ``clustering`` the clustering columns in
    order. Rule: the ordering column sits immediately after the partition key,
    and a discriminator that must be pushed down belongs in ``partition``, since
    Cassandra requires clustering restrictions to form a prefix.
    """

    partition: tuple[str, ...]
    clustering: tuple[str, ...] = ()
    order: tuple[tuple[str, str], ...] = ()  # (column, "ASC" | "DESC")


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    key: Key
    options: dict[str, str] = field(default_factory=dict)
    comment: str | None = None

    def column_names(self) -> frozenset[str]:
        return frozenset(c.name for c in self.columns)


@dataclass(frozen=True)
class UserType:
    name: str
    columns: tuple[Column, ...]


@dataclass(frozen=True)
class Schema:
    kind: Kind
    family: Family
    types: tuple[UserType, ...]
    tables: tuple[Table, ...]

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(f"no table {name!r} in {self.kind.value}/{self.family.value}")


# --- table options -----------------------------------------------------------
#
# Not one table in any v2 schema or migration file sets a single WITH option, so
# every table gets default STCS, default compression and default caching
# regardless of how it is written or read. These are the four profiles the v3
# tables actually fall into.

STCS: dict[str, str] = {
    "compaction": "{'class':'SizeTieredCompactionStrategy'}",
}

#: Write-once bulk data. Large chunks compress better and are never partially read.
BULK: dict[str, str] = {
    "compaction": "{'class':'SizeTieredCompactionStrategy'}",
    "compression": "{'class':'ZstdCompressor','chunk_length_in_kb':16}",
}

#: Read-heavy, overwritten in place. LCS keeps a read to few sstables.
LCS: dict[str, str] = {
    "compaction": "{'class':'LeveledCompactionStrategy','sstable_size_in_mb':'160'}",
}

#: Small and read on nearly every request.
CACHED: dict[str, str] = {
    "compaction": "{'class':'SizeTieredCompactionStrategy'}",
    "caching": "{'keys':'ALL','rows_per_partition':'ALL'}",
}

#: Delete-heavy working set (the epoch rows compaction absorbs).
CHURN: dict[str, str] = {
    "compaction": "{'class':'LeveledCompactionStrategy'}",
    "gc_grace_seconds": "259200",
}
