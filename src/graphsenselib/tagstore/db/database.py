import contextlib
from datetime import datetime
from functools import cache, partial
from importlib.resources import files as imprtlb_files
from typing import Iterable

from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Session, SQLModel, create_engine, create_mock_engine, text

from .. import db as db
from .models import (
    Actor,
    ActorConcept,
    ActorJurisdiction,
    ActorPack,
    Address,
    AddressClusterMapping,
    Concept,
    ConceptRelationAnnotation,
    Confidence,
    Country,
    Tag,
    TagConcept,
    TagPack,
    TagSubject,
    TagType,
    Taxonomy,
)

_MAIN_TABLES = [
    Taxonomy.__table__,
    Confidence.__table__,
    Country.__table__,
    TagSubject.__table__,
    TagType.__table__,
    Concept.__table__,
    ActorPack.__table__,
    Actor.__table__,
    TagPack.__table__,
    Tag.__table__,
    TagConcept.__table__,
    ActorConcept.__table__,
    ActorJurisdiction.__table__,
    Address.__table__,
    AddressClusterMapping.__table__,
    ConceptRelationAnnotation.__table__,
]

_REQUIRED_MATERIALIZED_VIEWS = (
    "statistics",
    "tag_count_by_cluster",
    "best_cluster_tag",
)

_QUALITY_MEASURES_MARKER = "-- Quality measures"


def get_db_engine(db_url, **kwargs):
    return create_engine(db_url, **kwargs)


def get_db_engine_async(db_url, **kwargs):
    return create_async_engine(db_url, **kwargs)


def to_sync_db_url(db_url: str) -> str:
    """Convert async Postgres URLs to sync SQLAlchemy URLs."""
    if db_url.startswith("postgresql+asyncpg://"):
        return db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
    return db_url


def get_table_ddl_sql():
    out = []

    def dump(out, sql, *multiparams, **params):
        out.append(str(sql.compile(dialect=engine.dialect)))

    engine = create_mock_engine("postgresql+psycopg2://", partial(dump, out))
    SQLModel.metadata.create_all(engine, checkfirst=False, tables=_MAIN_TABLES)

    return ";\n\n\n".join(out)


def get_views_ddl_sql(include_quality_measures: bool = True):
    with imprtlb_files(db).joinpath("init.sql").open("r") as file:
        ddl = file.read()

    if include_quality_measures:
        return ddl

    marker_pos = ddl.find(_QUALITY_MEASURES_MARKER)
    if marker_pos == -1:
        return ddl
    return ddl[:marker_pos]


def create_tables(engine):
    SQLModel.metadata.create_all(
        engine,
        tables=_MAIN_TABLES,
    )


@contextlib.contextmanager
def with_session(engine):
    with Session(engine) as session:
        yield session


def init_database(engine, include_quality_measures: bool = True):
    create_tables(engine)

    with Session(engine) as session:
        _add_fk_data(session)

        session.commit()

        # create view etc.
        views_sql_ddl = get_views_ddl_sql(
            include_quality_measures=include_quality_measures
        )

        session.execute(text(views_sql_ddl))

        session.commit()


def _relation_exists(session: Session, relation_name: str, relation_kind: str) -> bool:
    query = text(
        """
        SELECT EXISTS(
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = :relation_name
              AND c.relkind = :relation_kind
        )
        """
    )
    return bool(
        session.execute(
            query,
            {
                "relation_name": relation_name,
                "relation_kind": relation_kind,
            },
        ).scalar()
    )


def _all_relations_exist(
    session: Session,
    relation_names: Iterable[str],
    relation_kind: str,
) -> bool:
    for relation_name in relation_names:
        if not _relation_exists(session, relation_name, relation_kind):
            return False
    return True


def is_database_initialized(engine) -> bool:
    """Check whether core TagStore tables and runtime materialized views exist."""
    required_tables = [table.name for table in _MAIN_TABLES]

    with Session(engine) as session:
        tables_ok = _all_relations_exist(session, required_tables, relation_kind="r")
        if not tables_ok:
            return False

        views_ok = _all_relations_exist(
            session,
            _REQUIRED_MATERIALIZED_VIEWS,
            relation_kind="m",
        )
        return views_ok


def ensure_database_initialized(
    db_url: str,
    include_quality_measures: bool = False,
) -> bool:
    """Initialize TagStore schema only when required relations are missing.

    Returns True when initialization was executed, False when schema already existed.
    """
    engine = get_db_engine(to_sync_db_url(db_url))
    try:
        if is_database_initialized(engine):
            return False

        init_database(engine, include_quality_measures=include_quality_measures)
        return True
    finally:
        engine.dispose()


@cache
def _is_abuse_concept(tree, concept):
    from anytree import find  # Lazy import to avoid loading at module import time

    return any(
        x.name == "abuse"
        for x in find(tree, lambda node: node.name == concept).iter_path_reverse()
    )


def _add_fk_data(session):
    # Lazy imports to avoid loading tagpack dependencies at module import time
    from graphsenselib.tagpack.constants import DEFAULT_CONFIG
    from graphsenselib.tagpack.taxonomy import _load_taxonomies

    desc = f"Imported at {datetime.now().isoformat()}"
    tax = _load_taxonomies(DEFAULT_CONFIG)
    for tax_name, tax in tax.items():
        session.add(
            session.merge(
                Taxonomy(
                    id=tax_name.strip(),
                    source=tax.uri.strip(),
                    description=desc.strip(),
                ),
                load=True,
            )
        )

        tree = tax.get_concept_tree_id()
        for c in tax.concepts:
            data = {
                "id": c.id.strip(),
                "label": c.label.strip(),
                "source": c.uri.strip(),
                "description": c.description.strip(),
                "taxonomy": c.taxonomy.key.strip(),
            }
            if "concept" == tax_name:
                instance = Concept(
                    **{
                        **data,
                        "parent": c.parent,
                        "is_abuse": _is_abuse_concept(tree, data["id"]),
                    }
                )
            elif "tag_type" == tax_name:
                instance = TagType(**data)
            elif "tag_subject" == tax_name:
                instance = TagSubject(**data)
            elif "country" == tax_name:
                instance = Country(**data)
            elif "confidence" == tax_name:
                instance = Confidence(**{**data, "level": c.level})
            elif "concept_relation_annotation":
                instance = ConceptRelationAnnotation(**data)

            session.add(session.merge(instance, load=True))
