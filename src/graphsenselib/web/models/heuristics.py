"""Web-facing heuristic models for UTXO transactions.

These models are intentionally decoupled from service-layer heuristics models.
The web representation currently omits per-heuristic ``details`` sections.
"""

from typing import Optional

from graphsenselib.web.models.base import APIModel


class AddressOutput(APIModel):
    address: str
    index: int


class OneTimeChangeHeuristic(APIModel):
    summary: list[AddressOutput]
    confidence: int = 50


class DirectChangeHeuristic(APIModel):
    summary: list[AddressOutput]
    confidence: int = 100


class MultiInputChangeHeuristic(APIModel):
    summary: list[AddressOutput]
    confidence: int = 50


class ConsensusEntry(APIModel):
    output: AddressOutput
    confidence: int
    sources: list[str]


class ChangeHeuristics(APIModel):
    consensus: list[ConsensusEntry]
    one_time_change: Optional[OneTimeChangeHeuristic] = None
    direct_change: Optional[DirectChangeHeuristic] = None
    multi_input_change: Optional[MultiInputChangeHeuristic] = None


class UtxoHeuristics(APIModel):
    change_heuristics: Optional[ChangeHeuristics] = None
