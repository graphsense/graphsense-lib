from typing import Any, Dict, List, Optional, Protocol, Union, Tuple

from graphsenselib.defi import Bridge, ExternalSwap
from graphsenselib.defi.conversions import get_conversions_from_db
from graphsenselib.errors import (
    BadUserInputException,
    NotFoundException,
    TransactionNotFoundException,
)
from graphsenselib.utils.accountmodel import hex_to_bytes, strip_0x
from graphsenselib.utils.function_call_parser import (
    parse_function_call,
    function_signatures as function_call_signatures,
)
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)
from graphsenselib.utils.rest_utils import is_eth_like

from .common import std_tx_from_row
from .models import (
    ExternalConversion,
    TxAccount,
    TxRef,
    TxUtxo,
    TxValue,
    Txs,
)
from .rates_service import RatesService


def is_supported_asset(
    asset_address: str, token_config: Optional[Dict[str, Any]]
) -> bool:
    """
    Check if the asset address is supported based on the token configuration.
    """
    if token_config is None:
        supported_tokens = set()
    else:
        supported_tokens = {x["token_address"].hex() for x in token_config.values()}

    return (
        strip_0x(asset_address.lower()) in supported_tokens or asset_address == "native"
    )


async def _raw_trace_to_std_tx(
    currency: str,
    result: Dict[str, Any],
    tx_timestamp: int,
    rates,
    tokenConfig,
    trace_index: Optional[int],
    is_first_trace: bool,
) -> Dict[str, Any]:
    result["type"] = "internal"
    result["timestamp"] = tx_timestamp
    # result["is_tx_trace"] = False
    if "input" in result:
        result["input_parsed"] = parse_function_call(
            result["input"], function_call_signatures
        )

    if currency == "trx":
        result["from_address"] = result["caller_address"]
        result["to_address"] = result["transferto_address"]
        result["value"] = result["call_value"]
    else:
        result["contract_creation"] = result["trace_type"] == "create"

        if trace_index is None or is_first_trace:
            result["type"] = "external"
        else:
            # if trace_index is not None:
            is_external = (
                result["trace_address"] is None or result["trace_address"].strip() == ""
            )

            # if they are the same, then we know it is external
            if is_external:
                result["type"] = "external"

    return await std_tx_from_row(currency, result, rates.rates, tokenConfig)


class DatabaseProtocol(Protocol):
    async def get_tx_by_hash(
        self, currency: str, tx_hash_bytes: bytes
    ) -> Optional[Dict[str, Any]]: ...

    async def get_tx(self, currency: str, tx_hash: str) -> Optional[Dict[str, Any]]: ...

    def get_token_configuration(self, currency: str) -> Any: ...

    async def list_token_txs(
        self, currency: str, tx_hash: str, log_index: Optional[int] = None
    ) -> List[Dict[str, Any]]: ...

    async def get_spent_in_txs(
        self, currency: str, tx_hash: str, io_index: Optional[int] = None
    ) -> Any: ...

    async def get_spending_txs(
        self, currency: str, tx_hash: str, io_index: Optional[int] = None
    ) -> Any: ...

    async def list_matching_txs(
        self, currency: str, expression: str
    ) -> List[Dict[str, Any]]: ...

    async def fetch_transaction_trace(
        self, currency: str, tx: Dict[str, Any], trace_index: Optional[int]
    ) -> Optional[Dict[str, Any]]: ...


class TxsService:
    def __init__(
        self,
        db: DatabaseProtocol,
        rates_service: RatesService,
        logger: Any,
    ):
        self.db = db
        self.rates_service = rates_service
        self.logger = logger

    async def get_tx(
        self,
        currency: str,
        tx_hash: str,
        token_tx_id: Optional[int] = None,
        include_io: bool = False,
        include_nonstandard_io: bool = False,
        include_io_index: bool = False,
    ) -> Union[TxAccount, TxUtxo]:
        trace_index = None
        tx_ident = tx_hash

        try:
            subtxIdent = SubTransactionIdentifier.from_string(tx_hash)
        except ValueError as e:
            raise BadUserInputException(str(e))

        if subtxIdent.tx_type == SubTransactionType.InternalTx:
            tx_hash = subtxIdent.tx_hash
            trace_index = subtxIdent.sub_index
        elif subtxIdent.tx_type == SubTransactionType.ERC20:
            tx_hash = subtxIdent.tx_hash
            if token_tx_id is None:
                token_tx_id = subtxIdent.sub_index

        if token_tx_id is not None:
            if is_eth_like(currency):
                results = await self.list_token_txs(currency, tx_hash, token_tx_id)
                if len(results):
                    return results[0]
                else:
                    raise TransactionNotFoundException(currency, tx_ident, token_tx_id)
            else:
                raise BadUserInputException(
                    f"{currency} does not support token transactions."
                )
        elif trace_index is not None:
            if is_eth_like(currency):
                tx = await self.db.get_tx(currency, tx_hash)
                res = await self._get_trace_txs(currency, tx, trace_index)
                if res:
                    return res
                else:
                    raise TransactionNotFoundException(currency, tx_ident, token_tx_id)
            else:
                raise BadUserInputException(
                    f"{currency} does not support trace transactions."
                )
        else:
            result = await self.db.get_tx(currency, tx_hash)
            rates = await self.rates_service.get_rates(currency, result["block_id"])
            if currency == "eth":
                return await self._get_trace_txs(
                    currency, result, None, get_first_trace=True
                )

            if result:
                result["type"] = "external"

            return await std_tx_from_row(
                currency,
                result,
                rates.rates,
                self.db.get_token_configuration(currency),
                include_io,
                include_nonstandard_io,
                include_io_index,
            )

    async def get_asset_flows_within_tx(
        self,
        network: str,
        tx_hash: str,
        include_token_txs: bool = True,
        include_internal_txs: bool = True,
        include_zero_value: bool = True,
        include_base_transaction: bool = True,
        include_io: bool = True,
        include_nonstandard_io: bool = True,
        include_io_index: bool = True,
        asset: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> Txs:
        tx = await self.db.get_tx(network, tx_hash)
        tokenConfig = self.db.get_token_configuration(network)
        rates = await self.rates_service.get_rates(network, tx["block_id"])

        results_list = []
        if is_eth_like(network):
            if include_internal_txs:
                traces = await self.db.fetch_transaction_traces(network, tx)
                traces_converted = [
                    await _raw_trace_to_std_tx(
                        network,
                        result,
                        tx["block_timestamp"],
                        rates,
                        tokenConfig,
                        trace_index=result["trace_index"],
                        is_first_trace=(i == 0),
                    )
                    for i, result in enumerate(traces)
                ]

                results_list.extend(traces_converted)
            else:
                results_list.append(
                    await self._get_trace_txs(network, tx, None, get_first_trace=True)
                )

            if include_token_txs:
                tokens = await self.db.list_token_txs(network, tx_hash)
                tokens_converted = [
                    await std_tx_from_row(network, result, rates.rates, tokenConfig)
                    for result in tokens
                ]

                results_list.extend(tokens_converted)

            if not include_zero_value:
                results_list = [x for x in results_list if x.value.value != 0]

            if not include_base_transaction:
                results_list = [x for x in results_list if x.is_external is not True]

        else:
            results_list.append(
                await std_tx_from_row(
                    network,
                    tx,
                    rates,
                    tokenConfig,
                    include_io=include_io,
                    include_nonstandard_io=include_nonstandard_io,
                    include_io_index=include_io_index,
                )
            )

        if asset is not None:
            results_list = [
                x
                for x in results_list
                if x.currency.strip().upper() == asset.strip().upper()
            ]

        next_page = None
        if page is not None and page_size is not None:
            page = page - 1
            start = page * page_size
            end = start + page_size
            if end < len(results_list):
                next_page = page + 2
            results_list = results_list[start:end]

        return Txs(txs=results_list, next_page=next_page)

    async def get_tx_io(
        self,
        currency: str,
        tx_hash: str,
        io: str,
        include_nonstandard_io: bool = False,
        include_io_index: bool = False,
    ) -> Optional[List[TxValue]]:
        if is_eth_like(currency):
            raise NotFoundException("get_tx_io not implemented for ETH")

        result = await self.get_tx(
            currency,
            tx_hash,
            include_io=True,
            include_nonstandard_io=include_nonstandard_io,
            include_io_index=include_io_index,
        )
        return getattr(result, io)

    async def list_token_txs(
        self, currency: str, tx_hash: str, token_tx_id: Optional[int] = None
    ) -> List[TxAccount]:
        results = await self.db.list_token_txs(currency, tx_hash, log_index=token_tx_id)

        txs = []
        for result in results:
            rates = await self.rates_service.get_rates(currency, result["block_id"])
            tx = await std_tx_from_row(
                currency,
                result,
                rates.rates,
                self.db.get_token_configuration(currency),
            )
            txs.append(tx)

        return txs

    async def get_spent_in_txs(
        self, currency: str, tx_hash: str, io_index: Optional[int]
    ) -> List[TxRef]:
        results = await self.db.get_spent_in_txs(currency, tx_hash, io_index=io_index)

        return [
            TxRef(
                input_index=t["spending_input_index"],
                output_index=t["spent_output_index"],
                tx_hash=t["spending_tx_hash"].hex(),
            )
            for t in results.current_rows
        ]

    async def get_spending_txs(
        self, currency: str, tx_hash: str, io_index: Optional[int]
    ) -> List[TxRef]:
        results = await self.db.get_spending_txs(currency, tx_hash, io_index=io_index)

        return [
            TxRef(
                input_index=t["spending_input_index"],
                output_index=t["spent_output_index"],
                tx_hash=t["spent_tx_hash"].hex(),
            )
            for t in results.current_rows
        ]

    async def list_matching_txs(self, currency: str, expression: str) -> List[str]:
        results = await self.db.list_matching_txs(currency, expression)

        leading_zeros = 0
        pos = 0
        while pos < len(expression) and expression[pos] == "0":
            pos += 1
            leading_zeros += 1

        txs = [
            "0" * leading_zeros
            + str(hex(int.from_bytes(row["tx_hash"], byteorder="big")))[2:]
            for row in results
        ]
        return [tx for tx in txs if tx.startswith(expression)]

    async def _get_trace_txs(
        self,
        currency: str,
        tx: Dict[str, Any],
        trace_index: Optional[int],
        get_first_trace: bool = False,
    ) -> Optional[Union[TxAccount, TxUtxo]]:
        result = await self.db.fetch_transaction_trace(
            currency, tx, trace_index, get_first_trace=get_first_trace
        )

        if result and result["tx_hash"] == tx["tx_hash"]:
            tokenConfig = self.db.get_token_configuration(currency)
            rates = await self.rates_service.get_rates(currency, tx["block_id"])
            return await _raw_trace_to_std_tx(
                currency,
                result,
                tx["block_timestamp"],
                rates,
                tokenConfig,
                trace_index,
                get_first_trace,
            )
        else:
            return None

    def _conversion_from_external_swap(
        self, network: str, swap: ExternalSwap
    ) -> ExternalConversion:
        token_config = self.db.get_token_configuration(network)

        return ExternalConversion(
            conversion_type="dex_swap",
            from_address=swap.fromAddress,
            to_address=swap.toAddress,
            from_asset=swap.fromAsset,
            to_asset=swap.toAsset,
            from_amount=hex(swap.fromAmount),
            to_amount=hex(swap.toAmount),
            from_asset_transfer=swap.fromPayment,
            to_asset_transfer=swap.toPayment,
            from_network=network,
            to_network=network,
            from_is_supported_asset=is_supported_asset(swap.fromAsset, token_config),
            to_is_supported_asset=is_supported_asset(swap.toAsset, token_config),
        )

    def _conversion_from_bridge(self, bridge: Bridge) -> ExternalConversion:
        token_config_from = self.db.get_token_configuration(bridge.fromNetwork)
        token_config_to = self.db.get_token_configuration(bridge.toNetwork)

        return ExternalConversion(
            conversion_type="bridge",
            from_address=bridge.fromAddress,
            to_address=bridge.toAddress,
            from_asset=bridge.fromAsset,
            to_asset=bridge.toAsset,
            from_amount=hex(bridge.fromAmount),
            to_amount=hex(bridge.toAmount),
            from_asset_transfer=bridge.fromPayment,
            to_asset_transfer=bridge.toPayment,
            from_network=bridge.fromNetwork,
            to_network=bridge.toNetwork,
            from_is_supported_asset=is_supported_asset(
                bridge.fromAsset, token_config_from
            ),
            to_is_supported_asset=is_supported_asset(bridge.toAsset, token_config_to),
        )

    async def get_conversions(
        self, currency: str, identifier: str, included_bridges: Tuple[str, ...] = ()
    ) -> List[ExternalConversion]:
        """Extract swap information from a single transaction hash."""
        if not is_eth_like(currency):
            raise BadUserInputException(
                f"Swap extraction is only supported for EVM-like networks, not {currency}"
            )

        tx_obj = SubTransactionIdentifier.from_string(identifier)
        tx_hash = tx_obj.tx_hash

        # Get tx to get block_id
        try:
            tx_hash_bytes = hex_to_bytes(tx_hash)
            tx = await self.db.get_tx_by_hash(currency, tx_hash_bytes)
        except ValueError:
            raise BadUserInputException(
                f"{tx_hash} does not look like a valid transaction hash"
            )

        if not tx:
            raise TransactionNotFoundException(currency, tx_hash)

        conversions_gslib = await get_conversions_from_db(
            currency, self.db, tx, included_bridges=included_bridges
        )

        # if it is a raw tx hash without a subtx, dont filter, otherwise
        # filter the conversions to the ones that have either fromPayment or toPayment as identifier
        if tx_obj.tx_type is SubTransactionType.ExternalTx:
            filtered_conversions = conversions_gslib
        else:
            filtered_conversions = [
                c
                for c in conversions_gslib
                if strip_0x(c.fromPayment.lower())
                == strip_0x(tx_obj.to_string()).lower()
                or strip_0x(c.toPayment.lower()) == strip_0x(tx_obj.to_string()).lower()
            ]

        conversions = []

        for conversion in filtered_conversions:
            if isinstance(conversion, ExternalSwap):
                conversions.append(
                    self._conversion_from_external_swap(currency, conversion)
                )
            elif isinstance(conversion, Bridge):
                conversions.append(self._conversion_from_bridge(conversion))
            else:
                raise ValueError(f"Unknown conversion type: {type(conversion)}")

        return conversions
