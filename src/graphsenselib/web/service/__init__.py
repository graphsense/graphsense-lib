from typing import Optional

from graphsenselib.errors import BadUserInputException


def parse_page_int_optional(page: Optional[str]) -> Optional[int]:
    if page is None:
        return None
    if isinstance(page, str):
        try:
            page = int(page)
        except ValueError:
            raise BadUserInputException("Invalid page number")

    return page
