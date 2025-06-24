from datetime import datetime
from typing import List, Optional

from sqlalchemy import Column, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.sql import func
from sqlmodel import Field, Relationship, SQLModel

_SCHEMA = "public"
_SHARED_TABLE_ARGS = {}


class Taxonomy(SQLModel, table=True):
    __tablename__ = "taxonomy"
    __table_args__ = _SHARED_TABLE_ARGS

    id: str = Field(primary_key=True)  # noqa
    source: str
    description: Optional[str] = Field(default=None)  # noqa


class Concept(SQLModel, table=True):
    __tablename__ = "concept"
    __table_args__ = _SHARED_TABLE_ARGS

    id: str = Field(primary_key=True)  # noqa
    label: str
    source: str
    description: str
    parent: Optional[str] = Field(foreign_key="concept.id")
    is_abuse: str
    taxonomy: str = Field(foreign_key="taxonomy.id")


class TagType(SQLModel, table=True):
    __tablename__ = "tag_type"
    __table_args__ = _SHARED_TABLE_ARGS

    id: str = Field(primary_key=True)  # noqa
    label: str
    source: str
    description: str
    taxonomy: str = Field(foreign_key="taxonomy.id")


class Country(SQLModel, table=True):
    __tablename__ = "country"
    __table_args__ = _SHARED_TABLE_ARGS

    id: str = Field(primary_key=True)  # noqa
    label: str
    source: str
    description: str
    taxonomy: str = Field(foreign_key="taxonomy.id")


class TagSubject(SQLModel, table=True):
    __tablename__ = "tag_subject"
    __table_args__ = _SHARED_TABLE_ARGS

    id: str = Field(primary_key=True)  # noqa
    label: str
    source: str
    description: str
    taxonomy: str = Field(foreign_key="taxonomy.id")


class ConceptRelationAnnotation(SQLModel, table=True):
    __tablename__ = "concept_relation_annotation"
    __table_args__ = _SHARED_TABLE_ARGS

    id: str = Field(primary_key=True)  # noqa
    label: str
    source: str
    description: str
    taxonomy: str = Field(foreign_key="taxonomy.id")


class Confidence(SQLModel, table=True):
    __tablename__ = "confidence"
    __table_args__ = _SHARED_TABLE_ARGS

    id: str = Field(default=None, primary_key=True)  # noqa
    label: str
    description: str
    level: int
    taxonomy: str = Field(foreign_key="taxonomy.id")


class ActorPack(SQLModel, table=True):
    __tablename__ = "actorpack"
    __table_args__ = _SHARED_TABLE_ARGS

    id: Optional[str] = Field(default=None, primary_key=True)  # noqa
    title: str
    creator: str
    description: str
    uri: Optional[str]
    lastmod: datetime = Field(sa_column_kwargs={"server_default": func.now()})


class Actor(SQLModel, table=True):
    __tablename__ = "actor"
    __table_args__ = _SHARED_TABLE_ARGS

    id: Optional[str] = Field(default=None, primary_key=True)  # noqa
    uri: Optional[str]
    label: str
    context: Optional[str]
    lastmod: datetime = Field(sa_column_kwargs={"server_default": func.now()})

    # FK data
    actorpack_id: str = Field(
        sa_column=Column(
            "actorpack", String, ForeignKey("actorpack.id", ondelete="CASCADE")
        )
    )
    actorpack: ActorPack = Relationship()

    concepts: List["ActorConcept"] = Relationship(
        cascade_delete=True, sa_relationship_kwargs={"lazy": "subquery"}
    )

    jurisdictions: List["ActorJurisdiction"] = Relationship(
        cascade_delete=True, sa_relationship_kwargs={"lazy": "subquery"}
    )


class TagPack(SQLModel, table=True):
    __tablename__ = "tagpack"
    __table_args__ = _SHARED_TABLE_ARGS

    id: Optional[str] = Field(default=None, primary_key=True)  # noqa
    title: str
    description: str
    creator: str
    uri: Optional[str]
    acl_group: str = Field(sa_column_kwargs={"server_default": "public"})
    lastmod: datetime = Field(sa_column_kwargs={"server_default": func.now()})


class Tag(SQLModel, table=True):
    __tablename__ = "tag"
    __table_args__ = (
        UniqueConstraint(
            "identifier", "network", "label", "tagpack", "source", name="unique_tag"
        ),
        _SHARED_TABLE_ARGS,
    )

    id: Optional[int] = Field(default=None, primary_key=True)  # noqa
    label: str = Field(index=True)
    source: Optional[str]
    context: Optional[str]
    is_cluster_definer: bool = Field(
        default=False, index=True, sa_column_kwargs={"server_default": "false"}
    )
    lastmod: datetime = Field(sa_column_kwargs={"server_default": func.now()})
    identifier: str = Field(index=True)
    asset: Optional[str]
    network: str

    # FK data
    confidence_id: str = Field(
        sa_column=Column(
            "confidence", String, ForeignKey("confidence.id"), nullable=False
        )
    )
    confidence: Confidence = Relationship(
        sa_relationship_kwargs={}  # {"lazy": "subquery"}
    )

    tag_type_id: str = Field(
        sa_column=Column("tag_type", String, ForeignKey("tag_type.id"), nullable=False)
    )
    tag_type: TagType = Relationship(
        sa_relationship_kwargs={"viewonly": True, "lazy": "subquery"}
    )

    tag_subject_id: str = Field(
        sa_column=Column(
            "tag_subject", String, ForeignKey("tag_subject.id"), nullable=False
        )
    )
    tag_subject: TagSubject = Relationship(
        sa_relationship_kwargs={"viewonly": True, "lazy": "subquery"}
    )

    tagpack_id: str = Field(
        sa_column=Column(
            "tagpack",
            String,
            ForeignKey("tagpack.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    tagpack: TagPack = Relationship()
    actor_id: Optional[str] = Field(
        sa_column=Column(
            "actor", String, ForeignKey("actor.id"), nullable=True, index=True
        )
    )
    actor: Optional[Actor] = Relationship()
    concepts: List["TagConcept"] = Relationship(
        cascade_delete=True, sa_relationship_kwargs={"lazy": "subquery"}
    )


class TagConcept(SQLModel, table=True):
    __tablename__ = "tag_concept"
    __table_args__ = (
        UniqueConstraint("tag_id", "concept_id", name="unique_concept"),
        _SHARED_TABLE_ARGS,
    )

    tag_id: int = Field(foreign_key="tag.id", primary_key=True, ondelete="CASCADE")
    concept_relation_annotation_id: Optional[str] = Field(
        foreign_key="concept_relation_annotation.id"
    )
    concept_relation_annotation: ConceptRelationAnnotation = Relationship()

    # FK data
    concept_id: str = Field(foreign_key="concept.id", primary_key=True)
    concept: Concept = Relationship()


class ActorJurisdiction(SQLModel, table=True):
    __tablename__ = "actor_jurisdiction"
    __table_args__ = (
        UniqueConstraint("actor_id", "country_id", name="unique_jurisdiction"),
        _SHARED_TABLE_ARGS,
    )

    actor_id: str = Field(foreign_key="actor.id", primary_key=True, ondelete="CASCADE")
    # FK data
    country_id: str = Field(foreign_key="country.id", primary_key=True)
    country: Country = Relationship()


class ActorConcept(SQLModel, table=True):
    __tablename__ = "actor_concept"
    __table_args__ = (
        UniqueConstraint("actor_id", "concept_id", name="unique_actor_concept"),
        _SHARED_TABLE_ARGS,
    )

    actor_id: str = Field(foreign_key="actor.id", primary_key=True, ondelete="CASCADE")
    # FK data
    concept_id: str = Field(foreign_key="concept.id", primary_key=True)
    concept: Concept = Relationship()


class Address(SQLModel, table=True):
    network: str = Field(primary_key=True)
    address: str = Field(primary_key=True)
    created: datetime = Field(sa_column_kwargs={"server_default": func.now()})
    is_mapped: bool = Field(sa_column_kwargs={"server_default": "false"})


class AddressClusterMapping(SQLModel, table=True):
    __tablename__ = "address_cluster_mapping"
    __table_args__ = (
        Index("acm_gs_cluster_id_index", "network", "gs_cluster_id"),
        _SHARED_TABLE_ARGS,
    )
    address: str = Field(primary_key=True)
    network: str = Field(primary_key=True)
    gs_cluster_id: int
    gs_cluster_def_addr: str
    gs_cluster_no_addr: Optional[int]


# Materialized views only to make access uniform


class BestClusterTagView(SQLModel, table=True):
    __tablename__ = "best_cluster_tag"
    cluster_id: int = Field(primary_key=True)
    network: str = Field(primary_key=True)
    tag_id: int = Field(primary_key=True)


class TagCountByClusterView(SQLModel, table=True):
    __tablename__ = "tag_count_by_cluster"
    gs_cluster_id: int = Field(primary_key=True)
    network: str = Field(primary_key=True)
    acl_group: str = Field(primary_key=True)
    count: int


class NetworkStatisticsView(SQLModel, tabel=True):
    __tablename__ = "statistics"

    network: str = Field(primary_key=True)
    nr_identifiers_explicit: int
    nr_identifiers_implicit: int
    nr_labels: int
    nr_tags: int
