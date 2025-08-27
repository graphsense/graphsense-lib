from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field


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
    pubkey: bytes


class CrossChainPubkeyRelatedAddresses(BaseModel):
    addresses: List[CrossChainPubkeyRelatedAddress] = Field(default_factory=list)
    next_page: Optional[int] = None


class Txs(BaseModel):
    txs: List[Union[TxAccount, TxUtxo]] = Field(default_factory=list)
    next_page: Optional[int] = None


# Update forward references
Values.model_rebuild()
