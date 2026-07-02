from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

from graphsenselib.db.asynchronous.services.heuristics import UtxoHeuristics


class SearchRequestConfig(BaseModel):
    include_sub_tx_identifiers: bool = True
    include_labels: bool = True
    include_actors: bool = True
    include_txs: bool = True
    include_addresses: bool = True


class TokenConfig(BaseModel):
    ticker: str
    decimals: int
    peg_currency: str
    contract_address: str


class TokenConfigs(BaseModel):
    token_configs: List[TokenConfig] = Field(default_factory=list)


class CurrencyStats(BaseModel):
    name: str
    no_blocks: int
    no_address_relations: int
    no_addresses: int
    no_entities: int
    no_txs: int
    no_labels: int
    no_tagged_addresses: int
    timestamp: int
    network_type: str


class FiatValue(BaseModel):
    code: str
    value: float


class Values(BaseModel):
    value: int
    fiat_values: List[FiatValue] = Field(default_factory=list)


class TxSummary(BaseModel):
    height: int
    timestamp: int
    tx_hash: str


class AddressTag(BaseModel):
    address: Optional[str] = None
    entity: Optional[int] = None
    category: Optional[str] = None
    concepts: Optional[List[str]] = None
    actor: Optional[str] = None
    tag_type: Optional[str] = None
    abuse: Optional[str] = None
    label: str
    lastmod: Optional[int] = None
    source: Optional[str] = None
    tagpack_is_public: Optional[bool] = None
    tagpack_uri: Optional[str] = None
    tagpack_creator: Optional[str] = None
    tagpack_title: Optional[str] = None
    confidence: Optional[str] = None
    confidence_level: Optional[int] = None
    is_cluster_definer: Optional[bool] = None
    inherited_from: Optional[str] = None
    currency: Optional[str] = None


class LabeledItemRef(BaseModel):
    id: str
    label: str


class Entity(BaseModel):
    currency: str
    entity: int
    root_address: str
    first_tx: TxSummary
    last_tx: TxSummary
    no_addresses: int
    no_incoming_txs: int
    no_outgoing_txs: int
    total_received: Values
    total_tokens_received: Optional[Dict[str, Values]] = None
    total_spent: Values
    total_tokens_spent: Optional[Dict[str, Values]] = None
    in_degree: int
    out_degree: int
    balance: Values
    token_balances: Optional[Dict[str, Values]] = None
    best_address_tag: Optional[AddressTag] = None
    no_address_tags: int
    actors: Optional[List[LabeledItemRef]] = None


class Address(BaseModel):
    address: str
    currency: str
    entity: Optional[int] = None
    first_tx: Optional[TxSummary] = None
    last_tx: Optional[TxSummary] = None
    no_incoming_txs: int = 0
    no_outgoing_txs: int = 0
    total_received: Values
    total_tokens_received: Optional[Dict[str, Values]] = None
    total_spent: Values
    total_tokens_spent: Optional[Dict[str, Values]] = None
    in_degree: int = 0
    out_degree: int = 0
    balance: Values
    token_balances: Optional[Dict[str, Values]] = None
    is_contract: Optional[bool] = None
    actors: Optional[List[LabeledItemRef]] = None
    status: Optional[str] = None


class Rate(BaseModel):
    code: str
    value: float

    def __getitem__(self, key):
        """Allow dictionary-style access like r["code"] or r["value"]"""
        if key == "code":
            return self.code
        elif key == "value":
            return self.value
        else:
            raise KeyError(f"Key '{key}' not found")


class RatesResponse(BaseModel):
    height: int
    rates: List[Rate]


class AddressTx(BaseModel):
    tx_hash: str
    height: int
    timestamp: int
    coinbase: bool
    total_input: Values
    total_output: Values


class AddressTagResult(BaseModel):
    next_page: Optional[str] = None
    address_tags: List[AddressTag]


class EntityAddresses(BaseModel):
    next_page: Optional[str] = None
    addresses: List[Address]


class NeighborEntity(BaseModel):
    labels: Optional[List[str]]
    value: Values
    token_values: Optional[Dict[str, Values]] = None
    no_txs: int
    entity: Union[int, Entity]


class NeighborEntities(BaseModel):
    next_page: Optional[str] = None
    neighbors: List[NeighborEntity]


class NeighborAddress(BaseModel):
    labels: Optional[List[str]]
    value: Values
    token_values: Optional[Dict[str, Values]] = None
    no_txs: int
    address: Address


class NeighborAddresses(BaseModel):
    next_page: Optional[str] = None
    neighbors: List[NeighborAddress]


class TxValue(BaseModel):
    address: List[str]
    value: Values
    index: Optional[int] = None
    script_hex: Optional[str] = None  # Raw script hex for OP_RETURN outputs
    has_witness: Optional[bool] = None  # True if input carries witness data
    sequence: Optional[int] = None  # Per-input nSequence (BIP125 RBF signaling)


class TxRef(BaseModel):
    input_index: int
    output_index: int
    tx_hash: str


class Parameter(BaseModel):
    name: str
    type: str


class ParameterDetails(Parameter):
    name: str
    type: str
    value: Union[str, int, float, bool]


class FunctionDefinition(BaseModel):
    name: str
    selector: str
    arguments: List[Parameter] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class FunctionCall(BaseModel):
    parameter_details: List[ParameterDetails]
    parameter_values: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict
    )
    function_definition: FunctionDefinition


class TxAccount(BaseModel):
    currency: str
    network: str
    tx_type: str = "account"
    identifier: str
    tx_hash: str
    timestamp: int
    height: int
    from_address: str
    to_address: str
    token_tx_id: Optional[int] = None
    contract_creation: Optional[bool] = None
    value: Values
    fee: Optional[Values] = None
    is_external: Optional[bool] = None
    input: Optional[bytes] = None
    parsed_input: Optional[FunctionCall] = None


class TxUtxo(BaseModel):
    tx_type: str = "utxo"
    currency: str
    tx_hash: str
    coinbase: bool
    height: int
    no_inputs: int
    no_outputs: int
    inputs: Optional[List[TxValue]] = None
    outputs: Optional[List[TxValue]] = None
    timestamp: int
    total_input: Values
    total_output: Values
    heuristics: Optional[UtxoHeuristics] = None
    version: Optional[int] = None
    lock_time: Optional[int] = None


class Block(BaseModel):
    currency: str
    height: int
    block_hash: str
    timestamp: int
    no_txs: int


class Tx(BaseModel):
    currency: str
    tx_hash: str
    height: int
    timestamp: int
    coinbase: bool
    total_input: Values
    total_output: Values
    inputs: Optional[List[TxValue]] = None
    outputs: Optional[List[TxValue]] = None


class BlockAtDate(BaseModel):
    before_block: Optional[int] = None
    before_timestamp: Optional[int] = None
    after_block: Optional[int] = None
    after_timestamp: Optional[int] = None


class GeneralStats(BaseModel):
    currencies: List[CurrencyStats]


class SearchResultByCurrency(BaseModel):
    currency: str
    addresses: List[str] = Field(default_factory=list)
    txs: List[str] = Field(default_factory=list)


class SearchResult(BaseModel):
    currencies: List[SearchResultByCurrency] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)
    actors: List[LabeledItemRef] = Field(default_factory=list)


class Stats(BaseModel):
    currencies: List[CurrencyStats]
    version: str
    request_timestamp: str


class Actor(BaseModel):
    id: str
    uri: str
    label: str
    jurisdictions: List[LabeledItemRef] = Field(default_factory=list)
    categories: List[LabeledItemRef] = Field(default_factory=list)
    nr_tags: int
    context: Optional["ActorContext"] = None


class ActorContext(BaseModel):
    uris: Optional[List[str]] = None
    images: Optional[List[str]] = None
    refs: Optional[List[str]] = None
    coingecko_ids: Optional[List[str]] = None
    defilama_ids: Optional[List[str]] = None
    twitter_handle: Optional[str] = None
    github_organisation: Optional[str] = None
    legal_name: Optional[str] = None


class Concept(BaseModel):
    id: str
    label: str
    description: Optional[str] = None
    taxonomy: str
    uri: Optional[str] = None


class Taxonomy(BaseModel):
    taxonomy: str
    uri: str


class ExternalConversion(BaseModel):
    conversion_type: str
    from_address: str
    to_address: str
    from_asset: str
    to_asset: str
    from_amount: str
    to_amount: str
    from_asset_transfer: str
    to_asset_transfer: str
    from_network: str
    to_network: str
    from_is_supported_asset: bool
    to_is_supported_asset: bool


class LinkUtxo(BaseModel):
    tx_type: str = "utxo"
    tx_hash: str
    height: int
    currency: str
    timestamp: int
    input_value: Values
    output_value: Values


class Links(BaseModel):
    next_page: Optional[str] = None
    links: List[Union[LinkUtxo, TxAccount]]


class AddressTxUtxo(BaseModel):
    currency: str
    height: int
    timestamp: int
    coinbase: bool
    tx_hash: str
    value: Values
    tx_type: str = "utxo"


class AddressTxs(BaseModel):
    next_page: Optional[str] = None
    address_txs: List[Union[TxAccount, AddressTxUtxo]]


class TagSummary(BaseModel):
    broad_category: Optional[str] = None
    tag_count: int
    tag_count_indirect: int
    best_actor: Optional[str] = None
    best_label: Optional[str] = None
    concept_tag_cloud: Dict[str, "TagCloudEntry"] = Field(default_factory=dict)
    label_summary: Dict[str, "LabelSummary"] = Field(default_factory=dict)


class AddressTagQueryInput(BaseModel):
    network: str
    address: Union[str, bytes]
    inherited_from_marker: Optional[str] = None


class TagCloudEntry(BaseModel):
    cnt: int
    weighted: float


class LabelSummary(BaseModel):
    label: str
    count: int
    confidence: Optional[float] = None
    relevance: float
    creators: List[str] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list)
    concepts: List[str] = Field(default_factory=list)
    lastmod: Optional[int] = None
    inherited_from: Optional[str] = None


class CrossChainPubkeyRelatedAddress(BaseModel):
    network: str = Field(alias="currency")
    type: str
    address: str
    pubkey: Optional[bytes] = None


class CrossChainPubkeyRelatedAddresses(BaseModel):
    addresses: List[CrossChainPubkeyRelatedAddress] = Field(default_factory=list)
    next_page: Optional[int] = None


class Txs(BaseModel):
    txs: List[Union[TxAccount, TxUtxo]] = Field(default_factory=list)
    next_page: Optional[int] = None


# Comparison internal models. API counterparts live in web/models/compare.py;
# the translator at web/translators.py:to_api_transaction_comparison maps
# between the two. To add a field that should reach the API, update all three.


class TxCharacteristicsInternal(BaseModel):
    inputs_script_types: List[str] = Field(default_factory=list)
    outputs_script_types: List[str] = Field(default_factory=list)
    # True/False if every input agrees, None if mixed or unresolvable.
    # Prefers row-level TxValue.has_witness; falls back to script-type
    # inference (which leaves P2SH ambiguous).
    inputs_have_witness: Optional[bool] = None
    n_inputs: int
    n_outputs: int
    total_input_sat: int
    total_output_sat: int
    fee_sat: Optional[int] = None
    tx_version: Optional[int] = None
    locktime: Optional[int] = None
    # True if any input signals BIP125 opt-in RBF (sequence < 0xfffffffe);
    # None when no inputs are available or all sequences are missing.
    inputs_signal_rbf: Optional[bool] = None
    # Block height the tx was mined in. Anchor for height-relative signals
    # like locktime anti-fee-sniping classification.
    block_height: Optional[int] = None
    # True if outputs are strictly ascending by amount (BIP69-compatible).
    # None when fewer than two outputs exist or amount ties prevent a
    # definitive call (the schema only stores script_hex for OP_RETURNs,
    # so amount-tie tiebreaking by script is unavailable).
    bip69_outputs_sorted: Optional[bool] = None
    # True if any input address is tagged as an exchange (broad_category ==
    # "exchange"). When tags are unavailable (no tag service or no
    # resolvable address), this is None.
    inputs_have_exchange: Optional[bool] = None
    # Canonicalized input addresses across all inputs of this tx. Populated
    # during compare_txs orchestration (canonicalization needs ``currency``)
    # and used by signal_direct_input_overlap / signal_change_chain.
    input_addresses_canon: List[str] = Field(default_factory=list)
    # Canonicalized change-output addresses for this tx, taken from the
    # consensus entries of the change heuristics. Empty when no consensus
    # change was detected. Populated during compare_txs orchestration.
    change_addresses_canon: List[str] = Field(default_factory=list)
    # Tx hashes whose outputs this tx spends (i.e., one-hop ancestors).
    # Populated during compare_txs orchestration via get_spending_txs.
    parent_tx_hashes: List[str] = Field(default_factory=list)
    input_cluster_ids: List[int] = Field(default_factory=list)
    coinjoin_detected: bool = False
    coinjoin_protocol: Optional[str] = None
    # Internal-only: indexes of *compared* txs whose outputs this tx directly
    # spends. Populated during compare_txs orchestration; not surfaced on the
    # API characteristics model.
    utxo_parent_indexes: List[int] = Field(default_factory=list)


class ComparisonSignalInternal(BaseModel):
    name: str
    kind: str  # "discriminator" | "score" | "linkage"
    per_tx: List[Optional[str]]
    verdict: str  # "match" | "mismatch" | "inconclusive"
    weight: int = 0


class LineageEdgeInternal(BaseModel):
    from_idx: int
    to_idx: int
    kind: str
    out_index: Optional[int] = None
    in_index: Optional[int] = None


class SubgraphTxSummaryInternal(BaseModel):
    tx_count: int
    # total_value is in the queried currency's native base unit (satoshi for
    # UTXO, wei/sun for account chains) and sums native transfers only;
    # total_value_fiat sums the fiat value (in fiat_currency) across all
    # transfers (incl. tokens). total_fee stays in the native unit. notes
    # flags caveats (partial fiat totals, excluded token transfers).
    total_value: int
    total_value_fiat: Optional[float] = None
    # Which fiat currency total_value_fiat is denominated in (e.g. "usd").
    fiat_currency: str = "usd"
    total_fee: Optional[int] = None
    # io counts are UTXO-only; None for account-model (ETH/TRX) summaries.
    total_inputs: Optional[int] = None
    total_outputs: Optional[int] = None
    block_min: int
    block_max: int
    timestamp_min: int
    timestamp_max: int
    notes: List[str] = Field(default_factory=list)


class SubgraphAddressSummaryInternal(BaseModel):
    address_count: int
    # Native base-unit sums (satoshi / wei / sun) over the selected
    # addresses. Account-chain token holdings are not folded in; notes
    # flags them. The *_fiat fields sum the fiat_currency value across
    # the set, partial when some addresses lack a rate (noted).
    total_received: int
    total_received_fiat: Optional[float] = None
    total_spent: int
    total_spent_fiat: Optional[float] = None
    balance: int
    balance_fiat: Optional[float] = None
    fiat_currency: str = "usd"
    # min/max activity timestamps over the set; None when no selected
    # address has any on-chain activity.
    first_usage: Optional[int] = None
    last_usage: Optional[int] = None
    # Addresses with at least one tag visible to the caller's groups.
    tagged_address_count: int = 0
    # Distinct actors across all tags on the set, deduped by id.
    actors: List[LabeledItemRef] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)


class SubgraphSummaryInternal(BaseModel):
    currency: str
    # Each block is present iff the request carried that node type.
    txs: Optional[SubgraphTxSummaryInternal] = None
    addresses: Optional[SubgraphAddressSummaryInternal] = None


class TxRefInternal(BaseModel):
    network: str
    tx_hash: str


class AddressRefInternal(BaseModel):
    network: str
    address: str


class GraphTxNetworkSummaryInternal(BaseModel):
    network: str
    tx_count: int
    # total_value.value is the network's native base unit (satoshi for UTXO,
    # wei/sun for account chains) and sums native transfers only; its
    # fiat_values sum the fiat value per code (eur, usd) across all
    # transfers (incl. tokens). notes flags caveats (partial fiat totals,
    # excluded token transfers). total_fee stays a plain native amount.
    total_value: Values
    total_fee: Optional[int] = None
    # io counts are UTXO-only; None for account-model (ETH/TRX) summaries.
    total_inputs: Optional[int] = None
    total_outputs: Optional[int] = None
    block_min: int
    block_max: int
    timestamp_min: int
    timestamp_max: int
    notes: List[str] = Field(default_factory=list)


class GraphTxOverallInternal(BaseModel):
    # Network-agnostic aggregate: fiat only (base units are not comparable
    # across chains), timestamps only (block heights are not either).
    tx_count: int
    total_value_fiat: List[FiatValue] = Field(default_factory=list)
    timestamp_min: int
    timestamp_max: int
    notes: List[str] = Field(default_factory=list)


class GraphTxSummaryInternal(BaseModel):
    overall: GraphTxOverallInternal
    networks: List[GraphTxNetworkSummaryInternal]


class GraphAddressNetworkSummaryInternal(BaseModel):
    network: str
    address_count: int
    # Native base-unit sums with per-code fiat sums; account-chain token
    # holdings are not folded into the native values (noted).
    total_received: Values
    total_spent: Values
    balance: Values
    first_usage: Optional[int] = None
    last_usage: Optional[int] = None
    tagged_address_count: int = 0
    actors: List[LabeledItemRef] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)


class GraphAddressOverallInternal(BaseModel):
    address_count: int
    total_received_fiat: List[FiatValue] = Field(default_factory=list)
    total_spent_fiat: List[FiatValue] = Field(default_factory=list)
    balance_fiat: List[FiatValue] = Field(default_factory=list)
    first_usage: Optional[int] = None
    last_usage: Optional[int] = None
    tagged_address_count: int = 0
    # Distinct actors across all networks, deduped by id.
    actors: List[LabeledItemRef] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)


class GraphAddressSummaryInternal(BaseModel):
    overall: GraphAddressOverallInternal
    networks: List[GraphAddressNetworkSummaryInternal]


class GraphSummaryInternal(BaseModel):
    # Each block is present iff the request carried that node type.
    txs: Optional[GraphTxSummaryInternal] = None
    addresses: Optional[GraphAddressSummaryInternal] = None


class ComparisonVerdictInternal(BaseModel):
    relation: str
    confidence: int
    cluster_verdict: str
    discriminator_hits: List[str] = Field(default_factory=list)
    score_total: float = 0.0
    notes: List[str] = Field(default_factory=list)


class TxComparedItemInternal(BaseModel):
    tx_hash: str
    characteristics: Optional[TxCharacteristicsInternal] = None
    details: Optional[Union[TxUtxo, TxAccount]] = None


class TransactionComparisonInternal(BaseModel):
    txs: List[TxComparedItemInternal] = Field(default_factory=list)
    signals: List[ComparisonSignalInternal] = Field(default_factory=list)
    lineage: List[LineageEdgeInternal] = Field(default_factory=list)
    # None when the verdict is excluded from the include list.
    verdict: Optional[ComparisonVerdictInternal] = None


# Update forward references
Values.model_rebuild()
