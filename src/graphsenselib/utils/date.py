from datetime import datetime


def parse_timestamp(timestamp: int) -> datetime:
    return datetime.utcfromtimestamp(timestamp)
