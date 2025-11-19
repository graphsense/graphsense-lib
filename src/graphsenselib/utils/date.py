import logging
from datetime import datetime, timedelta
import re

logger = logging.getLogger(__name__)


def parse_timestamp(timestamp: int) -> datetime:
    check_timestamp(timestamp)
    return datetime.utcfromtimestamp(timestamp)  # ty: ignore[deprecated]


def check_timestamp(timestamp: int) -> None:
    # check if timestamp is between 2000 and 2100
    if timestamp < 946684800 or timestamp > 4102444800:
        # log warning
        logger.warning(f"Timestamp {timestamp} is not between the years 2000 and 2100")


def is_weekday(date_to_check, weekday):
    """
    Check if a given date falls on a specific weekday.

    Args:
        date_to_check (datetime): The date to check
        weekday (int or str): Weekday to check for
                             - int: 0=Monday, 1=Tuesday, ..., 6=Sunday
                             - str: 'monday', 'tuesday', etc. (case insensitive)

    Returns:
        bool: True if the date is on the specified weekday, False otherwise
    """
    if date_to_check is None:
        return False

    # Convert string weekday to number if needed
    if isinstance(weekday, str):
        weekday_map = {
            "monday": 0,
            "mon": 0,
            "tuesday": 1,
            "tue": 1,
            "wednesday": 2,
            "wed": 2,
            "thursday": 3,
            "thu": 3,
            "friday": 4,
            "fri": 4,
            "saturday": 5,
            "sat": 5,
            "sunday": 6,
            "sun": 6,
        }
        weekday_num = weekday_map.get(weekday.lower())
        if weekday_num is None:
            raise ValueError(f"Invalid weekday: {weekday}")
        weekday = weekday_num

    return date_to_check.weekday() == weekday


def is_weekend(date_to_check):
    """
    Check if a given date falls on a weekend (Saturday or Sunday).

    Args:
        date_to_check (datetime): The date to check

    Returns:
        bool: True if the date is on weekend, False otherwise
    """
    if date_to_check is None:
        return False

    return date_to_check.weekday() in [5, 6]  # Saturday=5, Sunday=6


def is_weekday_business(date_to_check):
    """
    Check if a given date falls on a business day (Monday-Friday).

    Args:
        date_to_check (datetime): The date to check

    Returns:
        bool: True if the date is on a business day, False otherwise
    """
    if date_to_check is None:
        return False

    return date_to_check.weekday() < 5  # Monday=0 to Friday=4


def parse_time_period(period_str):
    """
    Parse a time period string like '1w', '2m', '3y', '1h' etc.

    Args:
        period_str (str): Time period string (e.g., '1w', '2m', '30d')

    Returns:
        timedelta: The parsed time period as a timedelta object

    Raises:
        ValueError: If the period string is invalid
    """

    if len(period_str) < 2:
        raise ValueError(
            f"Invalid time period format: {period_str}. Use format like '1w', '2m', '30d'"
        )

    # Match number followed by unit
    match = re.match(r"^(\d+)([smhdwMy])$", period_str.strip())

    if not match:
        raise ValueError(
            f"Invalid time period format: {period_str}. Use format like '1w', '2m', '30d'"
        )

    amount = int(match.group(1))
    unit = match.group(2)

    if unit == "s":  # seconds
        return timedelta(seconds=amount)
    elif unit == "m":  # minutes
        return timedelta(minutes=amount)
    elif unit == "h":  # hours
        return timedelta(hours=amount)
    elif unit == "d":  # days
        return timedelta(days=amount)
    elif unit == "w":  # weeks
        return timedelta(weeks=amount)
    elif unit == "M":  # months (approximate as 30 days)
        return timedelta(days=amount * 30)
    elif unit == "y":  # years (approximate as 365 days)
        return timedelta(days=amount * 365)
    else:
        raise ValueError(f"Unknown time unit: {unit}")


def is_date_older_than(date_to_check, period_str, now=datetime.now()):
    """
    Check if a given date is older than the specified time period.

    Args:
        date_to_check (datetime): The date to check
        period_str (str): Time period string (e.g., '1w', '2m', '30d')

    Returns:
        bool: True if the date is older than the specified period, False otherwise
    """
    if date_to_check is None:
        return True  # Consider None as "very old"

    time_delta = parse_time_period(period_str)
    threshold_date = now - time_delta

    return date_to_check < threshold_date


def parse_older_than_run_spec(spec: str, to_compare, now=datetime.now()) -> bool:
    """
    Parse a specification string for "older than" run checks.

    Args:
        spec (str): Specification string in the format '<period>;<weekday>'
                    e.g., '7d;sunday' means older than 7 days and on Sunday.

    Returns:
        tuple: (timedelta, weekday) where weekday is int (0=Monday,...6=Sunday) or None
    """
    parts = spec.split(";")
    if len(parts) == 0 or len(parts) > 2:
        raise ValueError(
            "Specification string cannot be empty or longer than two parts separated by ';'"
        )

    period_str = parts[0].strip()

    is_older = is_date_older_than(to_compare, period_str, now=now)

    is_right_day = True
    if len(parts) > 1:
        is_right_day = is_weekday(now, parts[1].strip())  # validate weekday

    return is_right_day and is_older
