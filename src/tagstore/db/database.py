import contextlib
import sys
from datetime import datetime
from functools import cache, partial

from anytree import find
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Session, SQLModel, create_engine, create_mock_engine, text

if sys.version_info >= (3, 9):
    from importlib.resources import files as imprtlb_files
else:
    from importlib_resources import files as imprtlb_files

from tagpack.cli import DEFAULT_CONFIG, _load_taxonomies

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


def get_db_engine(db_url, **kwargs):
    return create_engine(db_url, **kwargs)


def get_db_engine_async(db_url, **kwargs):
    return create_async_engine(db_url, **kwargs)


def get_table_ddl_sql():
    out = []

    def dump(out, sql, *multiparams, **params):
        out.append(str(sql.compile(dialect=engine.dialect)))

    engine = create_mock_engine("postgresql+psycopg2://", partial(dump, out))
    SQLModel.metadata.create_all(engine, checkfirst=False, tables=_MAIN_TABLES)

    return ";\n\n\n".join(out)


def get_views_ddl_sql():
    with imprtlb_files(db).joinpath("init.sql").open("r") as file:
        return file.read()


def create_tables(engine):
    SQLModel.metadata.create_all(
        engine,
        tables=_MAIN_TABLES,
    )


@contextlib.contextmanager
def with_session(engine):
    with Session(engine) as session:
        yield session


def init_database(engine):
    create_tables(engine)

    with Session(engine) as session:
        _add_fk_data(session)

        session.commit()

        # create view etc.
        views_sql_ddl = get_views_ddl_sql()

        session.execute(text(views_sql_ddl))

        session.commit()


@cache
def _is_abuse_concept(tree, concept):
    return any(
        x.name == "abuse"
        for x in find(tree, lambda node: node.name == concept).iter_path_reverse()
    )


def _add_fk_data(session):
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
