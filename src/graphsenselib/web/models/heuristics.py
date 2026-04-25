"""Web-facing heuristic models for UTXO transactions.

These models are intentionally decoupled from service-layer heuristics models.
The web representation currently omits per-heuristic ``details`` sections.
"""

from typing import Literal, Optional

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


# ---------------------------------------------------------------------------
# CoinJoin heuristics
# ---------------------------------------------------------------------------


class JoinMarketHeuristic(APIModel):
    detected: bool
    confidence: int
    n_participants: int
    denomination_sat: int


class WasabiHeuristic(APIModel):
    detected: bool
    confidence: int
    version: Literal["1.0", "1.1", "2.0"]
    n_participants: int
    denominations: list[int]


class WhirlpoolTx0Heuristic(APIModel):
    detected: bool
    confidence: int
    pool_denomination_sat: int
    n_premix_outputs: int


class WhirlpoolCoinJoinHeuristic(APIModel):
    detected: bool
    confidence: int
    pool_denomination_sat: int
    n_remixers: int
    n_new_entrants: int


class CoinJoinConsensus(APIModel):
    detected: bool
    confidence: int
    sources: list[str]


class CoinJoinHeuristics(APIModel):
    consensus: Optional[CoinJoinConsensus] = None
    joinmarket: Optional[JoinMarketHeuristic] = None
    wasabi: Optional[WasabiHeuristic] = None
    whirlpool_tx0: Optional[WhirlpoolTx0Heuristic] = None
    whirlpool_coinjoin: Optional[WhirlpoolCoinJoinHeuristic] = None


class UtxoHeuristics(APIModel):
    change_heuristics: Optional[ChangeHeuristics] = None
    coinjoin_heuristics: Optional[CoinJoinHeuristics] = None
