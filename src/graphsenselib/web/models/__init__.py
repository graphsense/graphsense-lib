"""Slim Pydantic models for the GraphSense REST API.

This module provides the API response models as native Pydantic v2 models,
replacing the generated OpenAPI models with a clean, minimal implementation.
"""

from graphsenselib.web.models.addresses import (
    Address,
    NeighborAddress,
    NeighborAddresses,
)
from graphsenselib.web.models.base import APIModel
from graphsenselib.web.models.blocks import Block, BlockAtDate
from graphsenselib.web.models.common import LabeledItemRef
from graphsenselib.web.models.entities import (
    Cluster,
    ClusterAddresses,
    Entity,
    EntityAddresses,
    NeighborCluster,
    NeighborClusters,
    NeighborEntities,
    NeighborEntity,
)
from graphsenselib.web.models.general import (
    Actor,
    ActorContext,
    Concept,
    CurrencyStats,
    ExternalConversion,
    Rates,
    RelatedAddress,
    RelatedAddresses,
    Stats,
    Taxonomy,
    TokenConfig,
    TokenConfigs,
)
from graphsenselib.web.models.compare import (
    ComparisonSignal,
    ComparisonVerdict,
    LineageEdge,
    TransactionComparison,
    TxCharacteristics,
    TxComparedItem,
)
from graphsenselib.web.models.subgraph import (
    SubgraphAddressSummary,
    SubgraphSummary,
    SubgraphSummaryRequest,
    SubgraphTxSummary,
)

from graphsenselib.web.models.graph import (
    GraphAddressNetworkSummary,
    GraphAddressOverall,
    GraphAddressRef,
    GraphAddressSummary,
    GraphCompareRequest,
    GraphSummary,
    GraphSummaryRequest,
    GraphTxNetworkSummary,
    GraphTxOverall,
    GraphTxRef,
    GraphTxSummary,
)
from graphsenselib.web.models.heuristics import (
    AddressOutput,
    ChangeHeuristics,
    CoinJoinConsensus,
    CoinJoinHeuristics,
    ConsensusEntry,
    DirectChangeHeuristic,
    JoinMarketHeuristic,
    MultiInputChangeHeuristic,
    OneTimeChangeHeuristic,
    UtxoHeuristics,
    WasabiHeuristic,
    WhirlpoolCoinJoinHeuristic,
    WhirlpoolTx0Heuristic,
)
from graphsenselib.web.models.search import (
    SearchResult,
    SearchResultByCurrency,
    SearchResultLeaf,
    SearchResultLevel1,
    SearchResultLevel2,
    SearchResultLevel3,
    SearchResultLevel4,
    SearchResultLevel5,
    SearchResultLevel6,
)
from graphsenselib.web.models.tags import (
    AddressTag,
    AddressTags,
    LabelSummary,
    Tag,
    TagCloudEntry,
    TagSummary,
    UserTagReportResponse,
)
from graphsenselib.web.models.transactions import (
    AddressTxs,
    AddressTxUtxo,
    Link,
    LinkUtxo,
    Links,
    Tx,
    TxAccount,
    TxRef,
    Txs,
    TxSummary,
    TxUtxo,
    TxValue,
)
from graphsenselib.web.models.values import Rate, Values

__all__ = [
    # Base
    "APIModel",
    # Common
    "LabeledItemRef",
    # Values
    "Rate",
    "Values",
    # Transactions
    "TxSummary",
    "TxRef",
    "TxValue",
    "TxUtxo",
    "TxAccount",
    "Tx",
    "Txs",
    "AddressTxUtxo",
    "AddressTxs",
    "LinkUtxo",
    "Link",
    "Links",
    # Tags
    "Tag",
    "AddressTag",
    "AddressTags",
    "TagCloudEntry",
    "LabelSummary",
    "TagSummary",
    "UserTagReportResponse",
    # Addresses
    "Address",
    "NeighborAddress",
    "NeighborAddresses",
    # Entities (deprecated names, kept for backwards compatibility)
    "Entity",
    "NeighborEntity",
    "NeighborEntities",
    "EntityAddresses",
    # Clusters (canonical names; aliases of Entity*)
    "Cluster",
    "NeighborCluster",
    "NeighborClusters",
    "ClusterAddresses",
    # Blocks
    "Block",
    "BlockAtDate",
    # Search
    "SearchResultByCurrency",
    "SearchResult",
    "SearchResultLeaf",
    "SearchResultLevel1",
    "SearchResultLevel2",
    "SearchResultLevel3",
    "SearchResultLevel4",
    "SearchResultLevel5",
    "SearchResultLevel6",
    # General
    "CurrencyStats",
    "Stats",
    "Rates",
    "Taxonomy",
    "Concept",
    "ActorContext",
    "Actor",
    # Heuristics
    "AddressOutput",
    "ConsensusEntry",
    "OneTimeChangeHeuristic",
    "DirectChangeHeuristic",
    "MultiInputChangeHeuristic",
    "ChangeHeuristics",
    "CoinJoinConsensus",
    "CoinJoinHeuristics",
    "JoinMarketHeuristic",
    "WasabiHeuristic",
    "WhirlpoolTx0Heuristic",
    "WhirlpoolCoinJoinHeuristic",
    "UtxoHeuristics",
    "TokenConfig",
    "TokenConfigs",
    "RelatedAddress",
    "RelatedAddresses",
    "ExternalConversion",
    # Comparison
    "TxCharacteristics",
    "TxComparedItem",
    "ComparisonSignal",
    "LineageEdge",
    "ComparisonVerdict",
    "TransactionComparison",
    # Subgraph
    "SubgraphAddressSummary",
    "SubgraphSummary",
    "SubgraphTxSummary",
    "SubgraphSummaryRequest",
    # Graph
    "GraphTxRef",
    "GraphAddressRef",
    "GraphSummaryRequest",
    "GraphCompareRequest",
    "GraphTxOverall",
    "GraphTxNetworkSummary",
    "GraphTxSummary",
    "GraphAddressOverall",
    "GraphAddressNetworkSummary",
    "GraphAddressSummary",
    "GraphSummary",
]
