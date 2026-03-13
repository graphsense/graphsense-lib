from typing import Optional

from pydantic import BaseModel


class OneTimeChangeDetails(BaseModel):
    same_script_type: list[str]
    not_nicely_divisible: list[str]
    output_less_than_input: list[str]
    not_reused: list[str]


class OneTimeChangeHeuristic(BaseModel):
    summary: dict[str, bool]
    details: OneTimeChangeDetails


class UtxoHeuristics(BaseModel):
    one_time_change: Optional[OneTimeChangeHeuristic] = None
