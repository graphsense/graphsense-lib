import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_timestamp(timestamp: int) -> datetime:
    check_timestamp(timestamp)
    return datetime.utcfromtimestamp(timestamp)  # ty: ignore[deprecated]


def check_timestamp(timestamp: int) -> None:
    # check if timestamp is between 2000 and 2100
    if timestamp < 946684800 or timestamp > 4102444800:
        # log warning
        logger.warning(f"Timestamp {timestamp} is not between the years 2000 and 2100")
