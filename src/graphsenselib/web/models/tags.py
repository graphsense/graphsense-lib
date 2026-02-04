"""Tag-related API models."""

from typing import Optional

from graphsenselib.web.models.base import APIModel


class Tag(APIModel):
    """Base tag model."""

    label: Optional[str] = None
    tag_type: Optional[str] = None
    tagpack_title: Optional[str] = None
    tagpack_is_public: Optional[bool] = None
    tagpack_creator: Optional[str] = None
    is_cluster_definer: Optional[bool] = None
    currency: Optional[str] = None
    category: Optional[str] = None
    concepts: Optional[list[str]] = None
    actor: Optional[str] = None
    abuse: Optional[str] = None
    tagpack_uri: Optional[str] = None
    source: Optional[str] = None
    lastmod: Optional[int] = None
    confidence: Optional[str] = None
    confidence_level: Optional[int] = None
    inherited_from: Optional[str] = None


class AddressTag(Tag):
    """Address tag model with address-specific fields."""

    address: Optional[str] = None
    entity: Optional[int] = None


class AddressTags(APIModel):
    """Paginated list of address tags."""

    address_tags: list[AddressTag]
    next_page: Optional[str] = None


class TagCloudEntry(APIModel):
    """Tag cloud entry model."""

    cnt: int
    weighted: float


class LabelSummary(APIModel):
    """Label summary model."""

    label: str
    count: int
    confidence: float
    relevance: float
    creators: list[str]
    sources: list[str]
    concepts: list[str]
    lastmod: int
    inherited_from: Optional[str] = None


class TagSummary(APIModel):
    """Tag summary model."""

    broad_category: str
    tag_count: int
    label_summary: dict[str, LabelSummary]
    concept_tag_cloud: dict[str, TagCloudEntry]
    tag_count_indirect: Optional[int] = None
    best_actor: Optional[str] = None
    best_label: Optional[str] = None


class UserTagReportResponse(APIModel):
    """Response for user tag report submission."""

    id: str
