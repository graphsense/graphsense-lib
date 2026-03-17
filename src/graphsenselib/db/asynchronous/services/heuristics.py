from typing import Optional
from pydantic import BaseModel


class AddressOutput(BaseModel):
    address: str
    index: int


class OneTimeChangeDetails(BaseModel):
    same_script_type: list[AddressOutput]
    not_nicely_divisible: list[AddressOutput]
    output_less_than_input: list[AddressOutput]
    not_reused: list[AddressOutput]


class OneTimeChangeHeuristic(BaseModel):
    summary: list[AddressOutput]
    details: Optional[OneTimeChangeDetails]
    confidence: int = 50


class DirectChangeHeuristic(BaseModel):
    summary: list[AddressOutput]
    details: Optional[OneTimeChangeDetails] = None
    confidence: int = 100


class MultiInputClusterEvidence(BaseModel):
    matching_input_address: str
    output: AddressOutput


class MultiInputChangeDetails(BaseModel):
    cluster: dict[int, list[MultiInputClusterEvidence]] = {}


class MultiInputChangeHeuristic(BaseModel):
    summary: list[AddressOutput]
    details: Optional[MultiInputChangeDetails]
    confidence: int = 50


class ConsensusEntry(BaseModel):
    output: AddressOutput
    confidence: int
    sources: list[str]


class ChangeHeuristics(BaseModel):
    consensus: list[ConsensusEntry]
    one_time_change: Optional[OneTimeChangeHeuristic] = None
    direct_change: Optional[DirectChangeHeuristic] = None
    multi_input_change: Optional[MultiInputChangeHeuristic] = None


class UtxoHeuristics(BaseModel):
    change_heuristics: Optional[ChangeHeuristics] = None
