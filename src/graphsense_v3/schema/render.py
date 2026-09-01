"""Render a :class:`~graphsense_v3.schema.model.Schema` to CQL."""

from __future__ import annotations

from graphsense_v3.schema.model import Key, Schema, Table, UserType

_INDENT = " " * 4


def _wrap_comment(text: str, prefix: str = "--") -> str:
    """Prefix each line as a CQL comment.

    A blank line becomes a bare ``--`` with no trailing space: the repo's
    trailing-whitespace hook would otherwise rewrite these generated files, and
    the freshness test would then fail against a renderer that still emits it.
    """
    lines = []
    for line in text.strip().splitlines():
        stripped = line.rstrip()
        lines.append(f"{prefix} {stripped}" if stripped else prefix)
    return "\n".join(lines)


def render_key(key: Key) -> str:
    partition = ", ".join(key.partition)
    if len(key.partition) > 1:
        partition = f"({partition})"
    parts = [partition, *key.clustering]
    return f"PRIMARY KEY ({', '.join(parts)})"


def render_options(table: Table) -> str:
    if not table.options and not table.key.order:
        return ""
    clauses: list[str] = []
    if table.key.order:
        ordering = ", ".join(f"{col} {direction}" for col, direction in table.key.order)
        clauses.append(f"CLUSTERING ORDER BY ({ordering})")
    clauses.extend(f"{k} = {v}" for k, v in sorted(table.options.items()))
    return "\n" + f"\n{_INDENT}AND ".join([f"{_INDENT}WITH {clauses[0]}", *clauses[1:]])


def render_type(user_type: UserType) -> str:
    body = ",\n".join(f"{_INDENT}{c.name} {c.type}" for c in user_type.columns)
    return f"CREATE TYPE IF NOT EXISTS {user_type.name} (\n{body}\n);"


def render_table(table: Table) -> str:
    lines: list[str] = []
    if table.comment:
        lines.append(_wrap_comment(table.comment))
    body: list[str] = []
    for column in table.columns:
        entry = f"{_INDENT}{column.name} {column.type},"
        if column.comment:
            entry = f"{entry}{' ' * max(1, 44 - len(entry))}-- {column.comment}"
        body.append(entry)
    body.append(f"{_INDENT}{render_key(table.key)}")
    lines.append(
        f"CREATE TABLE IF NOT EXISTS {table.name} (\n"
        + "\n".join(body)
        + f"\n){render_options(table)};"
    )
    return "\n".join(lines)


def render_schema(schema: Schema, keyspace: str, replication: str) -> str:
    """Render a full keyspace definition.

    ``replication`` is a CQL map literal, e.g.
    ``{'class':'NetworkTopologyStrategy','DC1':'2'}``.
    """
    header = [
        f"-- generated: {schema.kind.value} / {schema.family.value}",
        "-- Do not edit by hand; edit graphsense_v3.schema.definitions.",
        "",
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace}",
        f"{_INDENT}WITH replication = {replication};",
        "",
        f"USE {keyspace};",
    ]
    parts = ["\n".join(header)]
    parts.extend(render_type(t) for t in schema.types)
    parts.extend(render_table(t) for t in schema.tables)
    return "\n\n".join(parts) + "\n"
