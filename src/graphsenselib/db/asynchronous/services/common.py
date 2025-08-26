import asyncio
from typing import Any, Dict, List, Optional, Protocol, Union

import graphsenselib.utils.address
from graphsenselib.datatypes.common import NodeType
from graphsenselib.db.asynchronous.cassandra import get_tx_identifier
from graphsenselib.errors import (
    AddressNotFoundException,
    BadUserInputException,
    NetworkNotFoundException,
)
from graphsenselib.utils.address import address_to_user_format
from graphsenselib.utils.rest_utils import get_first_key_present, is_eth_like

from .models import (
    Address,
    AddressTxUtxo,
    FiatValue,
    FunctionCall,
    FunctionDefinition,
    LabeledItemRef,
    Links,
    LinkUtxo,
    Parameter,
    ParameterDetails,
    RatesResponse,
    TxAccount,
    TxSummary,
    TxUtxo,
    TxValue,
    Values,
)


def make_values(value, eur, usd):
    return Values(
        value=value,
        fiat_values=[
            FiatValue(code="eur", value=round(eur, 2)),
            FiatValue(code="usd", value=round(usd, 2)),
        ],
    )


def catchNaN(v):
    if v != v:
        return None
    return v


def map_rates_for_peged_tokens(rates, token_config):
    """Map rates for pegged tokens - handle both dict and RatesResponse types"""
    if isinstance(rates, RatesResponse):
        rates_dict = rates.rates
    elif isinstance(rates, dict):
        rates_dict = rates
    else:
        rates_dict = rates

    peg = token_config["peg_currency"].lower()
    if peg == "usd":
        if len(rates_dict) != 2:
            raise Exception(
                f"Rates structure is expected to be a list of length 2: {rates_dict}"
            )
        r = {i["code"]: i["value"] for i in rates_dict}

        return [
            {"code": "eur", "value": r["eur"] / r["usd"]},
            {"code": "usd", "value": 1},
        ]
    elif peg == "eur":
        if len(rates_dict) != 2:
            raise Exception(
                f"Rates structure is expected to be a list of length 2: {rates_dict}"
            )
        r = {i["code"]: i["value"] for i in rates_dict}

        return [
            {"code": "eur", "value": 1},
            {"code": "usd", "value": r["usd"] / r["eur"]},
        ]

    elif is_eth_like(peg):
        return rates
    else:
        raise Exception(
            "Currently only tokens pegged to ether, euro or usd are supported"
        )


def convert_token_values_map(currency, value_map, rates, token_configs):
    if value_map is None:
        return None
    else:
        return {
            token_currency.lower(): convert_token_value(
                value, rates, token_configs[token_currency]
            )
            for token_currency, value in value_map.items()
        }


def convert_value_impl(value, rates, factor):
    # Convert dict format to list format if needed
    if isinstance(rates, dict):
        rates_list = [{"code": k, "value": v} for k, v in rates.items()]
    else:
        rates_list = rates

    return Values(
        value=catchNaN(value),
        fiat_values=[
            FiatValue(
                code=r["code"], value=catchNaN(round(value * r["value"] * factor, 2))
            )
            for r in rates_list
        ],
    )


def convert_token_value(value, rates, token_config):
    """Convert token value using rates - handle both dict and RatesResponse types"""
    if isinstance(rates, RatesResponse):
        rates_dict = rates.rates
    elif isinstance(rates, dict) and "rates" in rates:
        rates_dict = rates["rates"]
    else:
        rates_dict = rates

    return convert_value_impl(
        value,
        map_rates_for_peged_tokens(rates_dict, token_config),
        1 / token_config["decimal_divisor"],
    )


def convert_value(currency, value, rates):
    """Convert value using rates - handle both dict and RatesResponse types"""
    if isinstance(rates, RatesResponse):
        rates_dict = rates.rates
    elif isinstance(rates, dict) and "rates" in rates:
        rates_dict = rates["rates"]
    else:
        rates_dict = rates

    if currency == "eth":
        factor = 1e-18
    elif currency == "trx":
        factor = 1e-6
    else:
        factor = 1e-8

    return convert_value_impl(value, rates_dict, factor)


def to_values_tokens(token_values):
    if token_values is None:
        return None
    return {k.lower(): to_values(value) for k, value in token_values.items()}


def to_values(value):
    return Values(
        value=catchNaN(value.value),
        fiat_values=[
            FiatValue(code=r["code"], value=catchNaN(round(r["value"], 2)))
            for r in value.fiat_values
        ],
    )


class TagstoreProtocol(Protocol):
    async def get_actors_by_subjectid(
        self, subject_id: str, groups: List[str]
    ) -> List[Any]: ...
    async def get_labels_by_subjectid(
        self, subject_id: str, groups: List[str]
    ) -> List[str]: ...
    async def get_labels_by_clusterid(
        self, cluster_id: str, groups: List[str]
    ) -> List[str]: ...


class DatabaseProtocol(Protocol):
    async def get_address_entity_id(self, currency: str, address: str) -> int: ...
    async def get_address(self, currency: str, address: str) -> Dict[str, Any]: ...
    async def new_address(self, currency: str, address: str) -> Dict[str, Any]: ...
    async def list_neighbors(
        self,
        currency: str,
        id: str,
        is_outgoing: bool,
        node_type: NodeType,
        targets: Optional[List[str]],
        page: Optional[str],
        pagesize: Optional[int],
    ) -> tuple: ...
    def get_token_configuration(self, currency: str) -> Dict[str, Any]: ...


def tx_summary_from_row(row: Dict[str, Any]) -> TxSummary:
    return TxSummary(
        height=row.height,
        timestamp=row.timestamp,
        tx_hash=row.tx_hash.hex() if hasattr(row.tx_hash, "hex") else str(row.tx_hash),
    )


# def address_tag_from_public_tag(
#     self, tag: Any, entity: Optional[int] = None
# ) -> AddressTag:
#     return AddressTag(
#         id=getattr(tag, "id", None),
#         address=getattr(tag, "address", None),
#         address_link=getattr(tag, "address_link", None),
#         category=getattr(tag, "category", None),
#         label=getattr(tag, "label", ""),
#         lastmod=getattr(tag, "lastmod", None),
#         source=getattr(tag, "source", None),
#         tagpack_uri=getattr(tag, "tagpack_uri", None),
#         confidence=getattr(tag, "confidence", None),
#         is_cluster_definer=getattr(tag, "is_cluster_definer", None),
#     )


def get_type_account(row):
    if row["type"] == "internal":
        return "account"
    elif row["type"] == "erc20":
        return "account"
    elif row["type"] == "external":
        return "account"
    else:
        raise Exception(f"Unknown transaction type {row}")


def labeled_item_ref_from_actor(actor: Any) -> LabeledItemRef:
    return LabeledItemRef(id=str(actor.id), label=actor.label)


def cannonicalize_address(currency: str, address: str) -> str:
    try:
        return graphsenselib.utils.address.cannonicalize_address(currency, address)
    except ValueError:
        raise BadUserInputException(
            "The address provided does not look"
            f" like a {currency.upper()} address: {address}"
        )


async def try_get_cluster_id(
    db: DatabaseProtocol, network: str, address: str, cache=None
) -> Optional[int]:
    key = f"cluster_id_{network}_{address}"
    if cache is not None and key in cache:
        return cache[key]

    try:
        network = network.lower()
        address_canonical = cannonicalize_address(network, address)
        returnv = await db.get_address_entity_id(network, address_canonical)
    except (
        AddressNotFoundException,
        NetworkNotFoundException,
        BadUserInputException,
    ):
        returnv = None

    if cache is not None:
        cache[key] = returnv

    return returnv


def address_from_row(
    currency: str,
    row: Dict[str, Any],
    rates: Dict[str, float],
    token_config: Dict[str, Any],
    actors: Optional[List[Any]] = None,
) -> Address:
    # Convert actors to LabeledItemRef if they aren't already
    converted_actors = None
    if actors:
        converted_actors = []
        for actor in actors:
            if isinstance(actor, LabeledItemRef):
                converted_actors.append(actor)
            else:
                # Convert from raw actor object
                converted_actors.append(labeled_item_ref_from_actor(actor))

    return Address(
        currency=currency,
        address=address_to_user_format(currency, row["address"]),
        entity=row.get("cluster_id"),
        first_tx=TxSummary(
            height=row["first_tx"].height,
            timestamp=row["first_tx"].timestamp,
            tx_hash=row["first_tx"].tx_hash.hex(),
        )
        if row.get("first_tx")
        else None,
        last_tx=TxSummary(
            height=row["last_tx"].height,
            timestamp=row["last_tx"].timestamp,
            tx_hash=row["last_tx"].tx_hash.hex(),
        )
        if row.get("last_tx")
        else None,
        no_incoming_txs=row.get("no_incoming_txs", 0),
        no_outgoing_txs=row.get("no_outgoing_txs", 0),
        total_received=to_values(row["total_received"]),
        total_tokens_received=to_values_tokens(row.get("total_tokens_received")),
        total_spent=to_values(row["total_spent"]),
        total_tokens_spent=to_values_tokens(row.get("total_tokens_spent")),
        in_degree=row.get("in_degree", 0),
        out_degree=row.get("out_degree", 0),
        balance=convert_value(currency, row["balance"], rates),
        token_balances=convert_token_values_map(
            currency, row.get("token_balances"), rates, token_config
        ),
        is_contract=row.get("is_contract"),
        actors=converted_actors,
        status=row.get("status"),
    )


def _get_type_account(row: Dict[str, Any]) -> str:
    if row["type"] == "internal":
        return "account"
    elif row["type"] == "erc20":
        return "account"
    elif row["type"] == "external":
        return "account"
    else:
        raise Exception(f"Unknown transaction type {row}")


def function_call_from_row(
    parsed_input: Optional[Dict[str, Any]],
) -> Optional[FunctionCall]:
    if parsed_input is None:
        return None
    return FunctionCall(
        parameter_details=[
            ParameterDetails(name=v["name"], type=v["type"], value=v["value"])
            for v in parsed_input.get("inputs", [])
        ],
        parameter_values=parsed_input.get("parameters", {}),
        function_definition=FunctionDefinition(
            name=parsed_input.get("function_def", {}).get("name", "unknown"),
            selector=parsed_input.get("selector", "unknown"),
            arguments=[
                Parameter(name=i["name"], type=i["type"])
                for i in parsed_input.get("function_def", {}).get("inputs", [])
            ],
            tags=parsed_input.get("function_def", {}).get("tags", []),
        ),
    )


async def _tx_account_from_row(
    currency: str,
    row: Dict[str, Any],
    rates: Dict[str, float],
    token_config: Dict[str, Any],
) -> TxAccount:
    height_keys = ["height", "block_id"]
    timestamp_keys = ["timestamp", "block_timestamp"]
    height = get_first_key_present(row, height_keys)

    r = rates[height] if isinstance(rates, dict) else rates
    is_external = row["type"] == "external"
    if row["type"] == "erc20":
        is_external = None

    input = row.get("input", None)

    return TxAccount(
        currency=currency if "token_tx_id" not in row else row["currency"].lower(),
        network=currency,
        tx_type=_get_type_account(row),
        identifier=get_tx_identifier(row, currency),
        tx_hash=row["tx_hash"].hex(),
        timestamp=get_first_key_present(row, timestamp_keys),
        height=height,
        from_address=address_to_user_format(currency, row["from_address"]),
        to_address=address_to_user_format(currency, row["to_address"]),
        token_tx_id=row.get("token_tx_id", None),
        contract_creation=row.get("contract_creation", None),
        value=convert_value(currency, row["value"], r)
        if "token_tx_id" not in row
        else convert_token_value(row["value"], r, token_config[row["currency"]]),
        is_external=is_external,
        input=input,
        parsed_input=function_call_from_row(row.get("input_parsed", None)),
    )


async def txs_from_rows(
    currency: str,
    rows: List[Dict[str, Any]],
    rates_service: Any,
    token_config: Dict[str, Any],
) -> List[Union[AddressTxUtxo, TxAccount]]:
    height_keys = ["height", "block_id"]
    heights = [get_first_key_present(row, height_keys) for row in rows]
    rates = await rates_service.list_rates(currency, heights)

    if is_eth_like(currency):
        results = []
        for row in rows:
            tx_result = await _tx_account_from_row(currency, row, rates, token_config)
            results.append(tx_result)
        return results

    return [
        AddressTxUtxo(
            currency=currency,
            height=row["height"],
            timestamp=row["timestamp"],
            coinbase=row["coinbase"],
            tx_hash=row["tx_hash"].hex(),
            value=convert_value(currency, row["value"], rates[row["height"]]),
        )
        for row in rows
    ]


async def get_address(
    db: DatabaseProtocol,
    tagstore: TagstoreProtocol,
    rates_service: Any,
    currency: str,
    address: str,
    tagstore_groups: List[str],
    include_actors: bool = True,
) -> Address:
    address_canonical = cannonicalize_address(currency, address)

    if len(address_canonical) == 0:
        raise BadUserInputException(
            f"{address} does not look like a valid {currency} address"
        )

    try:
        result = await db.get_address(currency, address_canonical)
    except AddressNotFoundException:
        result = await db.new_address(currency, address_canonical)

    actors = None
    if include_actors:
        actor_res = await tagstore.get_actors_by_subjectid(address, tagstore_groups)
        actors = [labeled_item_ref_from_actor(a) for a in actor_res]

    rates = await rates_service.get_rates(currency)
    return address_from_row(
        currency,
        result,
        rates.rates,
        db.get_token_configuration(currency),
        actors,
    )


async def list_neighbors(
    db: DatabaseProtocol,
    currency: str,
    id: Union[str, int],
    direction: str,
    node_type: NodeType,
    ids: Optional[List[Union[str, int]]] = None,
    include_labels: bool = False,
    page: Optional[str] = None,
    pagesize: Optional[int] = None,
    tagstore: Optional[TagstoreProtocol] = None,
    tagstore_groups: Optional[List[str]] = None,
) -> tuple:
    is_outgoing = "out" in direction
    results, paging_state = await db.list_neighbors(
        currency, id, is_outgoing, node_type, targets=ids, page=page, pagesize=pagesize
    )

    if results is not None:
        for row in results:
            row["labels"] = row["labels"] if "labels" in row else None
            row["value"] = to_values(row["value"])
            row["token_values"] = to_values_tokens(row.get("token_values", None))

    dst = "dst" if is_outgoing else "src"

    if results and include_labels and tagstore and tagstore_groups:
        await _add_labels(tagstore, currency, node_type, dst, results, tagstore_groups)

    return results, paging_state


async def _add_labels(
    tagstore: TagstoreProtocol,
    currency: str,
    node_type: NodeType,
    that: str,
    nodes: List[Dict[str, Any]],
    tagstore_groups: List[str],
):
    def identity(x, y):
        return y

    (field, tfield, fun, fmt) = (
        ("address", "address", "list_labels_for_addresses", address_to_user_format)
        if node_type == NodeType.ADDRESS
        else ("cluster_id", "gs_cluster_id", "list_labels_for_entities", identity)
    )
    thatfield = that + "_" + field
    ids = tuple((fmt(currency, node[thatfield]) for node in nodes))

    if node_type == NodeType.ADDRESS:
        tstasks = [
            tagstore.get_labels_by_subjectid(addr, tagstore_groups) for addr in ids
        ]
    else:
        tstasks = [
            tagstore.get_labels_by_clusterid(cluster_id, tagstore_groups)
            for cluster_id in ids
        ]

    tsresults = {k: v for k, v in zip(ids, await asyncio.gather(*tstasks))}

    for node in nodes:
        nid = node[thatfield]
        node["labels"] = tsresults.get(nid, [])

    return nodes


async def links_response(
    currency: str,
    result: tuple,
    rates_service: Any,
    token_config: Dict[str, Any],
    txs_service: Optional[Any] = None,
) -> Links:
    links, next_page = result

    if is_eth_like(currency):
        # For ETH-like currencies, process as transactions
        tx_results = await txs_from_rows(currency, links, rates_service, token_config)
        return Links(links=tx_results, next_page=next_page)
    else:
        # For UTXO currencies
        heights = [row["block_id"] for row in links]
        rates_dict = await rates_service.list_rates(currency, heights)

        link_results = [
            LinkUtxo(
                tx_hash=e["tx_hash"].hex(),
                height=e["block_id"],
                currency=currency,
                timestamp=e["timestamp"],
                input_value=convert_value(
                    currency, e["input_value"], rates_dict[e["block_id"]]
                ),
                output_value=convert_value(
                    currency, e["output_value"], rates_dict[e["block_id"]]
                ),
            )
            for e in links
        ]

        return Links(links=link_results, next_page=next_page)


def io_from_rows(
    currency: str,
    values: Dict[str, Any],
    key: str,
    rates: Dict[str, float],
    include_io: bool,
    include_nonstandard_io: bool,
    include_io_index: bool,
) -> Optional[List[TxValue]]:
    if not include_io:
        return None
    if key not in values:
        return None
    if not values[key]:
        return []

    results = []
    for idx, i in enumerate(values[key]):
        if i.address is not None:
            results.append(
                TxValue(
                    address=i.address,
                    value=convert_value(currency, i.value, rates),
                    index=idx if include_io_index else None,
                )
            )
        elif include_nonstandard_io:
            results.append(
                TxValue(
                    address=[],
                    value=convert_value(currency, i.value, rates),
                    index=idx if include_io_index else None,
                )
            )
    return results


async def std_tx_from_row(
    currency: str,
    row: Dict[str, Any],
    rates: Dict[str, float],
    token_config: Dict[str, Any],
    include_io: bool = False,
    include_nonstandard_io: bool = False,
    include_io_index: bool = False,
) -> Union[TxAccount, TxUtxo]:
    if is_eth_like(currency):
        return await _tx_account_from_row(currency, row, rates, token_config)

    coinbase = row.get("coinbase", False)

    inputs = io_from_rows(
        currency,
        row,
        "inputs",
        rates,
        include_io,
        include_nonstandard_io,
        include_io_index,
    )

    if coinbase and (inputs is None or inputs == []):
        inputs = [
            TxValue(
                address=["coinbase"],
                value=convert_value(currency, row["total_output"], rates),
                index=None if not include_io_index else 0,
            )
        ]

    total_input = convert_value(currency, row["total_input"], rates)
    total_output = convert_value(currency, row["total_output"], rates)

    if coinbase:
        total_input = total_output

    return TxUtxo(
        currency=currency,
        tx_hash=row["tx_hash"].hex(),
        coinbase=coinbase,
        height=row["block_id"],
        no_inputs=(0 if not row["inputs"] else len(row["inputs"]))
        + (1 if coinbase else 0),
        no_outputs=0 if not row["outputs"] else len(row["outputs"]),
        inputs=inputs,
        outputs=io_from_rows(
            currency,
            row,
            "outputs",
            rates,
            include_io,
            include_nonstandard_io,
            include_io_index,
        ),
        timestamp=row["timestamp"],
        total_input=total_input,
        total_output=total_output,
    )
