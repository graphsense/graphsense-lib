import logging
from dataclasses import dataclass, field
from typing import Optional

from graphsenselib.errors import BadUserInputException
from graphsenselib.web.config import GSRestConfig
from graphsenselib.web.dependencies import ServiceContainer


@dataclass
class ServiceContext:
    services: ServiceContainer
    tagstore_groups: list[str]
    config: GSRestConfig
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))
    cache: dict = field(default_factory=dict)
    username: Optional[str] = None
    obfuscate_private_tags: bool = False


def parse_page_int_optional(page: Optional[str]) -> Optional[int]:
    if page is None:
        return None
    if isinstance(page, str):
        try:
            page = int(page)
        except ValueError:
            raise BadUserInputException("Invalid page number")

    return page
