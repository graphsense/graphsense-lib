from typing import Annotated, List, Optional

from fastapi import Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlmodel import Session

from ..config import TagstoreSettings
from ..db import TagstoreDbAsync


def _get_session(request: Request):
    engine = request.app.state.db_engine
    with Session(engine) as session:
        yield session


def _get_tagstore_db_async(request: Request):
    return TagstoreDbAsync(request.app.state.db_engine)


class PageingState(BaseModel):
    page_nr: int
    page_size: int

    @property
    def offset(self) -> int:
        return self.page_nr * self.page_size

    @property
    def limit(self) -> int:
        return self.page_size


class TagsQueryParams(BaseModel):
    label: Optional[str] = None
    actor_id: Optional[str] = None
    cluster_id: Optional[int] = None
    subject_id: Optional[str] = None
    network: Optional[str] = None


def _get_tags_query_params(
    label: Optional[str] = None,
    actor_id: Optional[str] = None,
    cluster_id: Optional[int] = None,
    subject_id: Optional[str] = None,
    network: Optional[str] = None,
) -> TagsQueryParams:
    if sum(i is not None for i in [label, actor_id, cluster_id, subject_id]) != 1:
        raise HTTPException(
            400,
            "Please provide exactly one of either"
            " label, actor_id, cluster_id or subject_id",
        )
    elif cluster_id is not None and network is None:
        raise HTTPException(
            400,
            "network is mandatory for clusters",
        )
    else:
        return TagsQueryParams(
            label=label,
            actor_id=actor_id,
            cluster_id=cluster_id,
            subject_id=subject_id,
            network=network.upper() if network else None,
        )


async def _paging_parameters(
    page_nr: Annotated[
        int,
        Query(alias="page_nr", description="Which page to serve", ge=0),
    ] = 0,
    page_size: Annotated[
        int,
        Query(alias="page_size", description="size of a page", le=5000, gt=0),
    ] = 50,
) -> PageingState:
    return PageingState(page_nr=page_nr, page_size=page_size)


async def _acl_groups(
    groups: Annotated[
        Optional[List[str]],
        Query(alias="groups", description="Which groups of tags to consider."),
    ] = ["public"],
) -> Optional[List[str]]:
    return groups


def _gs_tagstore_settings() -> TagstoreSettings:
    return TagstoreSettings()


async def _db(request: Request):
    return request.app.state.db


TsSettingsParam = Annotated[TagstoreSettings, Depends(_gs_tagstore_settings)]

TsDbSessionParam = Annotated[Session, Depends(_get_session)]

TsDbParam = Annotated[TagstoreDbAsync, Depends(_get_tagstore_db_async)]

TsPagingParam = Annotated[PageingState, Depends(_paging_parameters)]

# TsIdentifierParam = Annotated[
#     str, Path(alias="identifier", description="Address or Transaction-hash")
# ]

TsTagsQueryParam = Annotated[TagsQueryParams, Depends(_get_tags_query_params)]

TsACLGroupsParam = Annotated[Optional[List[str]], Depends(_acl_groups)]
