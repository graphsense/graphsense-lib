from typing import Optional, Literal
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


class JoinMarketHeuristic(BaseModel):
    detected: bool
    confidence: int
    n_participants: int
    denomination_sat: int


class WasabiHeuristic(BaseModel):
    detected: bool
    confidence: int
    version: Literal["1.0", "1.1", "2.0"]
    n_participants: int
    denominations: list[int]


class WhirlpoolTx0Heuristic(BaseModel):
    detected: bool
    confidence: int
    pool_denomination_sat: int
    n_premix_outputs: int


class WhirlpoolCoinJoinHeuristic(BaseModel):
    detected: bool
    confidence: int
    pool_denomination_sat: int
    n_remixers: int
    n_new_entrants: int


class CoinJoinConsensus(BaseModel):
    detected: bool
    confidence: int
    sources: list[str]


class CoinJoinHeuristics(BaseModel):
    consensus: Optional[CoinJoinConsensus] = None
    joinmarket: Optional[JoinMarketHeuristic] = None
    wasabi: Optional[WasabiHeuristic] = None
    whirlpool_tx0: Optional[WhirlpoolTx0Heuristic] = None
    whirlpool_coinjoin: Optional[WhirlpoolCoinJoinHeuristic] = None


class UtxoHeuristics(BaseModel):
    change_heuristics: Optional[ChangeHeuristics] = None
    coinjoin_heuristics: Optional[CoinJoinHeuristics] = None
