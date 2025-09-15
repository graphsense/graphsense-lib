from typing import Dict, Optional, Any, Tuple, List, AsyncGenerator, Generator
from graphsenselib.utils.httpx import RetryHTTPClient  # Changed from import requests
from graphsenselib.utils import strip_0x
from graphsenselib.utils.transactions import (
    SubTransactionIdentifier,
    SubTransactionType,
)
from graphsenselib.defi.bridging.models import (
    Bridge,
    BridgeSendTransfer,
    BridgeReceiveTransfer,
    BridgeSendReference,
    BridgeReceiveReference,
)
from graphsenselib.utils.logging import logger
from graphsenselib.utils.accountmodel import is_native_placeholder
from graphsenselib.datatypes.abi import decode_logs_db, log_signatures
from graphsenselib.db.asynchronous.cassandra import Cassandra
from graphsenselib.defi.models import Trace


UTXO_NETWORKS = ["btc", "bch", "ltc", "zec"]
ACCOUNT_NETWORKS = ["eth", "trx"]
THOR_TO_GRAPHSENSE_NETWORK = {"BTC": "btc", "ETH": "eth"}
THORNODE_URLS = [
    "https://thornode.ninerealms.com/thorchain/tx/status/",
    "https://thornode-v1.ninerealms.com/thorchain/tx/details/",
]


async def try_thornode_endpoints(tx_hash_upper: str):
    """
    Try all THORNODE_URLS endpoints for the given transaction hash.
    Returns the first successful and decodable JSON response.
    """
    client = RetryHTTPClient()

    for base_url in THORNODE_URLS:
        try:
            url = f"{base_url}{tx_hash_upper}"
            logger.debug(f"Trying THORChain endpoint: {url}")

            response = await client.get(url)
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get("out_txs") is None or data.get("tx") is None:
                        continue

                    logger.debug(f"Successfully got decodable response from {url}")
                    return data
                except Exception as json_error:
                    logger.warning(
                        f"Response from {url} returned 200 but was not decodable JSON: {json_error}"
                    )
                    continue
            else:
                logger.debug(f"Endpoint {url} returned status {response.status_code}")
        except Exception as e:
            logger.warning(f"Error trying endpoint {base_url}: {e}")
            continue

    raise ValueError(
        "All THORChain endpoints failed or returned non-decodable responses"
    )


class SwapDecoder:
    """Minimal decoder for THORChain swap memos"""

    # Common asset abbreviations
    ASSETS = {
        "r": "THOR.RUNE",
        "b": "BTC.BTC",
        "e": "ETH.ETH",
        "a": "AVAX.AVAX",
        "d": "DOGE.DOGE",
        "l": "LTC.LTC",
        "g": "GAIA.ATOM",
    }

    def decode(self, memo: str) -> Dict[str, Any]:
        """
        Decode a swap memo

        Returns dict with:
        - is_swap: bool
        - asset: target asset
        - destination: destination address
        - limit: minimum output (optional)
        - streaming_interval: blocks between swaps (optional)
        - streaming_quantity: number of swaps (optional)
        - affiliate: affiliate address (optional)
        - affiliate_fee: fee in basis points (optional)
        """
        if not memo:
            return {"is_swap": False, "error": "Empty memo"}

        parts = memo.split(":")
        if not parts:
            return {"is_swap": False, "error": "Invalid format"}

        # Check if it's a swap
        func = parts[0].strip().lower()
        if func not in ["=", "s", "swap", "=<"]:
            return {
                "is_swap": False,
                "error": f"Not a swap memo (function: {parts[0]})",
            }

        result = {"is_swap": True, "raw_memo": memo}

        # Parse asset
        if len(parts) > 1 and parts[1]:
            result["asset"] = self._expand_asset(parts[1])

        # Parse destination address
        if len(parts) > 2 and parts[2]:
            if "/" in parts[2]:
                addrs = parts[2].split("/")
                result["destination"] = addrs[0]
                if len(addrs) > 1:
                    result["refund_address"] = addrs[1]
            else:
                result["destination"] = parts[2]

        # Parse limit/streaming params
        if len(parts) > 3 and parts[3]:
            streaming = parts[3].split("/")
            if streaming[0]:
                result["limit"] = self._to_int(streaming[0])
            if len(streaming) > 1 and streaming[1]:
                result["streaming_interval"] = int(streaming[1])
            if len(streaming) > 2 and streaming[2]:
                result["streaming_quantity"] = int(streaming[2])

        # Parse affiliate
        if len(parts) > 4 and parts[4]:
            result["affiliate"] = parts[4]

        # Parse affiliate fee
        if len(parts) > 5 and parts[5]:
            result["affiliate_fee"] = int(parts[5])

        return result

    def _expand_asset(self, asset: str) -> str:
        """Expand asset abbreviation or return as-is"""
        # If it's already in chain.asset format, return it
        if "." in asset:
            return asset
        # Check abbreviations
        return self.ASSETS.get(asset.lower(), asset)

    def _to_int(self, value: str) -> Optional[int]:
        """Convert to int, handling scientific notation"""
        if not value:
            return None
        try:
            # Handle scientific notation like 1e6
            if "e" in value.lower():
                return int(float(value))
            return int(value)
        except ValueError:
            return None


# Simple usage function
def decode_swap(memo: str) -> Dict[str, Any]:
    """Decode a THORChain swap memo"""
    decoder = SwapDecoder()
    return decoder.decode(memo)


class TransferOutDecoder:
    """Minimal decoder for THORChain TransferOut event memos"""

    def decode(self, memo: str) -> Dict[str, Any]:
        """
        Decode a TransferOut memo

        TransferOut memos typically have format:
        OUT:<TXID> or REFUND:<TXID>

        Returns dict with:
        - is_withdrawal: bool
        - type: 'OUT' or 'REFUND'
        - tx_id: transaction ID
        """
        if not memo:
            return {"is_withdrawal": False, "error": "Empty memo"}

        parts = memo.split(":")
        if not parts:
            return {"is_withdrawal": False, "error": "Invalid format"}

        # Check if it's a transfer out
        func = parts[0].strip().upper()
        if func not in ["OUT", "REFUND"]:
            return {
                "is_withdrawal": False,
                "error": f"Not a transfer out memo (function: {parts[0]})",
            }

        result = {"is_withdrawal": True, "type": func, "raw_memo": memo}

        # Parse transaction ID
        if len(parts) > 1 and parts[1]:
            result["tx_id"] = parts[1].strip()

        # Some OUT memos might have additional info
        if len(parts) > 2:
            result["additional_info"] = ":".join(parts[2:])

        return result


def decode_withdrawal(memo: str) -> Dict[str, Any]:
    """Decode a THORChain TransferOut memo"""
    decoder = TransferOutDecoder()
    return decoder.decode(memo)


async def get_bridges_from_thorchain_send_from_tx_hash_account(
    network: str,
    db: Cassandra,
    tx_hash: str,
) -> AsyncGenerator[BridgeSendTransfer, None]:
    """Get bridge send transfers from transaction hash for account-based networks"""
    if network in ACCOUNT_NETWORKS:
        tx_hash_bytes = bytes.fromhex(strip_0x(tx_hash))
        tx = await db.get_tx_by_hash(network, tx_hash_bytes)

        if tx is None:
            raise ValueError(f"Transaction {tx_hash} not found in DB")

        block_number = tx["block_id"]
        # Get all logs and traces in the block
        # todo wasteful
        logs_set = await db.get_logs_in_block_eth(
            network, block_number, tx_hash=tx_hash_bytes
        )
        traces_set = await db.get_traces_in_block(
            network, block_number, tx_hash=tx_hash_bytes
        )

        traces_set = Trace.dicts_to_normalized(network, traces_set, tx)

        dlogs_logs = decode_logs_db(logs_set, log_signatures_local=log_signatures)
        dlogs_filtered = [dlog for dlog, _ in dlogs_logs]
        logs_raw_filtered = [log for _, log in dlogs_logs]
        traces_filtered = [trace for trace in traces_set]

        for bridge_send, _ in get_bridges_from_thorchain_send(
            network, tx, dlogs_filtered, logs_raw_filtered, traces_filtered
        ):
            yield bridge_send


def get_bridges_from_thorchain_send(
    network: str,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
) -> Generator[Tuple[BridgeSendTransfer, BridgeReceiveReference], None, None]:
    """
    # example tx 6d65123e246d752de3f39e0fdf5b788baad35a29b7e95b74c714e6c7c1ea61dd Bybit hack bridge to BTC

    TODO Tag all addresses funded by Thorchain
    """

    def from_hex(address):
        return "0x" + address.hex()

    from_address = from_hex(tx["from_address"])

    deposits = [dlog for dlog in dlogs if dlog["name"] == "Deposit"]
    assert len(deposits) == 1, "Expected exactly one deposit"
    deposit = deposits[0]
    # to_ = deposit["parameters"]["to"]
    from_asset = deposit["parameters"]["asset"]
    # amount = deposit["parameters"]["amount"]
    # # '=:b:bc1q6fxj6446h2gr40wtfkzqyfk848gw4q4t8akr2p:0/1/0:dx:30' or
    # '=:ETH.ETH:0x19317e026ef473d44D746d364062539Ba7Cb0fa3:117890211/1/0:wr:100'
    memo = deposit["parameters"]["memo"]
    swap_info = decode_swap(memo)

    if swap_info["is_swap"]:
        to_address = swap_info["destination"]
        asset_code = swap_info["asset"]  # e.g. BTC.BTC
        to_network_thor = asset_code.split(".")[0]
        # asset = asset_code.split(".")[1]
        to_network = THOR_TO_GRAPHSENSE_NETWORK.get(to_network_thor)
        # to_asset = "native" if asset == network else asset
        # to_amount = swap_info["limit"]  # should be determined from the linked tx

    else:
        logger.warning(f"Skipping thorchain action, not implemented memo: {memo}")
        return None

    from_network = network

    if is_native_placeholder(from_asset):
        from_asset = "native"

    # correlate the payment on the target network
    # get the traces with nonzero value and the transfers
    token_transfers = [dlog for dlog in dlogs if dlog["name"] == "Transfer"]

    eth_transfers = [
        trace
        for trace in traces
        if trace.value > 0 and trace.from_address == from_address
    ]

    from_tx = tx["tx_hash"].hex()
    # Handle both ETH transfers (native) and token transfers (ERC-20)
    if len(token_transfers) > 0:
        # ERC-20 token transfer case
        # Find token transfers from the sender
        sender_token_transfers = [
            (i, dlog)
            for i, dlog in enumerate(dlogs)
            if dlog["name"] == "Transfer"
            and dlog["parameters"]["from"].lower() == from_address.lower()
        ]

        if len(sender_token_transfers) != 1:
            raise ValueError(
                f"Expected exactly one token transfer from sender, got {len(sender_token_transfers)}"
            )

        log_index, transfer_log = sender_token_transfers[0]
        from_amount = transfer_log["parameters"]["value"]
        log_index_raw = logs_raw[log_index]["log_index"]

        from_payment = SubTransactionIdentifier(
            tx_hash=from_tx,
            tx_type=SubTransactionType.ERC20,
            sub_index=log_index_raw,
        ).to_string()

    elif len(eth_transfers) > 0:
        # ETH transfer case (native token)
        if len(eth_transfers) != 1:
            raise ValueError(
                f"Expected exactly one ETH transfer from sender, got {len(eth_transfers)}"
            )

        trace_index = eth_transfers[0].trace_index
        from_amount = eth_transfers[0].value

        from_payment = SubTransactionIdentifier(
            tx_hash=from_tx,
            tx_type=SubTransactionType.InternalTx,
            sub_index=trace_index,
        ).to_string()
    else:
        raise ValueError("No token transfers or ETH transfers found from sender")

    yield (
        BridgeSendTransfer(
            fromAddress=from_address,
            fromAsset=from_asset,
            fromAmount=from_amount,
            fromPayment=from_payment,
            fromNetwork=from_network,
        ),
        BridgeReceiveReference(
            toAddress=to_address,
            toNetwork=to_network,
            fromTxHash=from_tx,
        ),
    )


async def get_bridges_from_thorchain_receive(
    network: str,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Trace],
) -> AsyncGenerator[Tuple[BridgeReceiveTransfer, BridgeSendReference], None]:
    """
    TODO: This doesnt work yet for BTC. Would need OP_RETURN data.
    """
    hash_field_name = "tx_hash" if network in ACCOUNT_NETWORKS else "tx_hash"
    tx_hash = tx[hash_field_name].hex()

    relevant_dlogs = [
        dlog
        for dlog in dlogs
        if dlog["name"] == "TransferOut" and "thorchain" in dlog["log_def"]["tags"]
    ]
    assert len(relevant_dlogs) == 1, "Expected exactly one TransferOut log"
    relevant_dlog = relevant_dlogs[0]

    params = relevant_dlog["parameters"]
    # vault = params["vault"]
    to_address = params["to"]
    asset_log = params["asset"]
    # amount = params["amount"]
    memo = params["memo"]

    parsed_memo = decode_withdrawal(memo)

    if "error" in parsed_memo:
        # e.g. 9b1ad0f967a06891e444477fbd254bea40179acd074ea697a305a2b4e91cbe86_I506
        logger.warning(
            f"There was an error in the thorchain memo: {parsed_memo['error']}, skipping."
        )
        return  # Early return for async generator

    to_tx_hash = parsed_memo["tx_id"].lower()

    if not parsed_memo["is_withdrawal"]:
        raise ValueError("Not a withdrawal")

    # Determine asset and get transfer payment identifier
    if is_native_placeholder(asset_log):
        to_asset = "native"
        # Find ETH transfer trace
        eth_transfers = [
            trace
            for trace in traces
            if trace.value > 0 and trace.to_address == to_address
        ]
        if len(eth_transfers) != 1:
            raise ValueError(
                f"Expected exactly one ETH transfer, got {len(eth_transfers)}"
            )

        trace_index = eth_transfers[0].trace_index
        to_amount = eth_transfers[0].value

        to_payment = SubTransactionIdentifier(
            tx_hash=tx_hash,
            tx_type=SubTransactionType.InternalTx,
            sub_index=trace_index,
        ).to_string()
    else:
        to_asset = asset_log
        # Find ERC20 transfer
        token_transfers = [
            (i, dlog)
            for i, dlog in enumerate(dlogs)
            if dlog["name"] == "Transfer" and dlog["parameters"]["to"] == to_address
        ]
        if len(token_transfers) != 1:
            raise ValueError(
                f"Expected exactly one token transfer, got {len(token_transfers)}"
            )

        log_index, transfer_log = token_transfers[0]
        to_amount = transfer_log["parameters"]["value"]
        log_index_raw = logs_raw[log_index]["log_index"]

        to_payment = SubTransactionIdentifier(
            tx_hash=tx_hash.lower(),
            tx_type=SubTransactionType.ERC20,
            sub_index=log_index_raw,
        ).to_string()

    yield (
        BridgeReceiveTransfer(
            toAddress=to_address,
            toAsset=to_asset,
            toAmount=int(to_amount),
            toPayment=to_payment,
            toNetwork=network,
        ),
        BridgeSendReference(
            fromTxHash=to_tx_hash,
        ),
    )


def combine_bridge_transfers(
    bridge_send: BridgeSendTransfer, bridge_receive: BridgeReceiveTransfer
) -> Bridge:
    """
    Combine a BridgeSendTransfer and BridgeReceiveTransfer into a full Bridge object.
    """
    return Bridge(
        fromAddress=bridge_send.fromAddress,
        fromAsset=bridge_send.fromAsset,
        fromAmount=bridge_send.fromAmount,
        fromPayment=bridge_send.fromPayment,
        fromNetwork=bridge_send.fromNetwork,
        toAddress=bridge_receive.toAddress,
        toAsset=bridge_receive.toAsset,
        toAmount=bridge_receive.toAmount,
        toPayment=bridge_receive.toPayment,
        toNetwork=bridge_receive.toNetwork,
    )


async def get_full_bridges_from_thorchain_send(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Dict[str, Any]],
) -> AsyncGenerator[Bridge, None]:
    """
    Get full Bridge objects from THORCHAIN send (deposit) transactions.
    Combines send transfers with their corresponding receive transfers.
    """

    result = list(get_bridges_from_thorchain_send(network, tx, dlogs, logs_raw, traces))
    if len(result) == 0:
        logger.warning(f"No send transfers found for {tx['tx_hash'].hex()}")
        return  # This stops the generator iteration

    send_transfers, receive_references = zip(*result)
    matcher = ThorchainTransactionMatcher(network, db)

    for send_transfer, receive_reference in zip(send_transfers, receive_references):
        receive_transfers = await matcher.match_receiving_transactions(
            receive_reference
        )
        if receive_transfers is None:
            continue

        for receive_transfer in receive_transfers:
            yield combine_bridge_transfers(send_transfer, receive_transfer)


async def get_full_bridges_from_thorchain_receive(
    network: str,
    db: Cassandra,
    tx: Dict[str, Any],
    dlogs: List[Dict[str, Any]],
    logs_raw: List[Dict[str, Any]],
    traces: List[Dict[str, Any]],
) -> AsyncGenerator[Bridge, None]:
    """
    Get full Bridge objects from THORCHAIN receive (withdrawal) transactions.
    """
    result = []
    async for item in get_bridges_from_thorchain_receive(
        network, tx, dlogs, logs_raw, traces
    ):
        if item:
            result.append(item)

    if len(result) == 0:
        logger.warning(f"No receive transfers found for {tx['tx_hash'].hex()}")
        return  # This is fine in async generators - just stops iteration

    receive_transfers, send_references = zip(*result)
    matcher = ThorchainTransactionMatcher(network, db)

    for receive_transfer, send_reference in zip(receive_transfers, send_references):
        send_transfers = await matcher.match_sending_transactions(send_reference)
        if send_transfers is None:
            continue

        for send_transfer in send_transfers:
            yield combine_bridge_transfers(send_transfer, receive_transfer)


async def preliminary_utxo_handling_receive(
    receive_reference: BridgeReceiveReference,
) -> Optional[List[BridgeReceiveTransfer]]:
    tx_hash_upper = receive_reference.fromTxHash.upper()

    thorchain_data = await try_thornode_endpoints(tx_hash_upper)

    # Find the outbound transaction for the target network
    target_out_tx = None
    for out_tx in thorchain_data.get("out_txs", []):
        if out_tx["chain"].lower() == receive_reference.toNetwork and not out_tx.get(
            "refund", False
        ):
            target_out_tx = out_tx
            break

    if target_out_tx:
        # Use the actual outbound transaction ID and amount
        toPayment = target_out_tx["id"].lower()
        # Get the actual amount received (not the planned amount)
        if target_out_tx["coins"]:
            toAmount = target_out_tx["coins"][0]["amount"]
        else:
            raise ValueError("No coins found")
    else:
        raise ValueError("No outbound transaction found")

    return [
        BridgeReceiveTransfer(
            toAddress=receive_reference.toAddress,
            toAsset="native",
            toAmount=toAmount,
            toPayment=toPayment,
            toNetwork=receive_reference.toNetwork,
        )
    ]


async def preliminary_utxo_handling_send(
    fromNetwork: str, send_reference: BridgeSendReference
) -> Optional[List[BridgeSendTransfer]]:
    tx_hash_upper = send_reference.fromTxHash.upper()

    thorchain_data = await try_thornode_endpoints(tx_hash_upper)

    # Find the inbound transaction (UTXO)
    tx = thorchain_data.get("tx")
    if not tx or tx.get("chain", "").lower() not in UTXO_NETWORKS:
        raise ValueError("No UTXO inbound transaction found")
    fromAddress = tx["from_address"]
    fromAsset = "native"
    fromAmount = tx["coins"][0]["amount"] if tx["coins"] else None

    fromPayment = send_reference.fromTxHash  # For UTXO, just the txid
    if not fromAmount:
        raise ValueError("No UTXO amount found in inbound transaction")
    return [
        BridgeSendTransfer(
            fromAddress=fromAddress,
            fromAsset=fromAsset,
            fromAmount=fromAmount,
            fromNetwork=fromNetwork,
            fromPayment=fromPayment,
        )
    ]


class ThorchainTransactionMatcher:
    def __init__(self, network: str, db: Cassandra):
        self.network = network  # base network
        self.db = db

    async def match_receiving_transactions(
        self,
        receive_reference: BridgeReceiveReference,
        min_height: int = 0,
    ) -> Optional[List[BridgeReceiveTransfer]]:
        """
        We want to find the fromTransferHash in a tx in the incoming txs of the address
        1. Get the incoming txs of the address
        2. For UTXO check the OP_RETURN, for ETH check the logs - WE currently dont have that in UTXO

        Multiple receiving transfers are possible
        """

        def get_tags(dlog):
            return dlog["log_def"]["tags"]

        if receive_reference.toNetwork in UTXO_NETWORKS:
            # todo replace this asap
            return await preliminary_utxo_handling_receive(receive_reference)

        elif receive_reference.toNetwork == "eth":
            matched = []
            address = receive_reference.toAddress
            address_bytes = bytes.fromhex(address[2:])
            txs = await self.db.list_address_txs(
                receive_reference.toNetwork,
                address_bytes,
                direction="in",
                min_height=min_height,
                order="asc",
            )
            txs = txs[0]

            for tx in txs:
                tx_hash = (
                    tx["tx_hash"].hex()
                )  # 18afc09b68ffd6797d8c89cca38fde2ad8e0319f46c38c0b00cc16a98d16521c
                block_of_tx = tx["height"]
                # get the logs, decode, check

                logs = await self.db.get_logs_in_block_eth(self.network, block_of_tx)
                logs = logs.current_rows  # todo paginate?

                relevant_logs = [log for log in logs if log["tx_hash"].hex() == tx_hash]
                decoded_logs = decode_logs_db(relevant_logs)

                for dlog, log in decoded_logs:
                    if dlog["name"] == "TransferOut" and "thorchain" in get_tags(dlog):
                        params = dlog["parameters"]
                        value_thorchain_log = params["amount"]
                        asset_thorchain_log = params["asset"]
                        memo = params["memo"]

                        decoded_memo = decode_withdrawal(memo)
                        if (
                            decoded_memo["is_withdrawal"]
                            and decoded_memo["tx_id"].lower()
                            == strip_0x(receive_reference.fromTxHash).lower()
                        ):
                            pass
                        else:
                            continue
                    else:
                        continue

                    if is_native_placeholder(asset_thorchain_log):
                        asset = "native"
                        transfer = tx_hash  # tx hash itself without an _I
                        matched.append(
                            BridgeReceiveTransfer(
                                toAddress=receive_reference.toAddress,
                                toAsset=asset,
                                toAmount=value_thorchain_log,
                                toPayment=transfer,
                                toNetwork=receive_reference.toNetwork,
                            )
                        )
                        break

                    # note that assets that we dont natively support wont show up here because there are no tx links
                    transfers = [
                        dlog for dlog in decoded_logs if dlog[0]["name"] == "Transfer"
                    ]

                    assert len(transfers) == 1, (
                        f"Expected 1 transfer, got {len(transfers)}"
                    )
                    transfer = transfers[0][0]
                    transfer_tx = transfers[0][1]

                    value_transfer_log = transfer["parameters"]["value"]

                    assert value_thorchain_log == value_transfer_log, (
                        f"Value mismatch: {value_thorchain_log} != {value_transfer_log}"
                    )
                    asset = transfer["address"].lower()
                    toPayment = SubTransactionIdentifier(
                        tx_hash=transfer_tx["tx_hash"].hex().lower(),
                        tx_type=SubTransactionType.ERC20,
                        sub_index=transfer_tx["log_index"],
                    ).to_string()
                    matched.append(
                        BridgeReceiveTransfer(
                            toAddress=receive_reference.toAddress,
                            toAsset=asset,
                            toAmount=int(value_transfer_log),
                            toPayment=toPayment,
                            toNetwork=receive_reference.toNetwork,
                        )
                    )

            return matched

    async def match_sending_transactions(
        self, send_reference: BridgeSendReference
    ) -> Optional[List[BridgeSendTransfer]]:
        # todo dont use api client, use the self.db and try to get the tx from the db
        currencies_supported = self.db.config["currencies"]
        hits = []
        for n in currencies_supported:
            try:
                tx_hash_bytes = bytes.fromhex(send_reference.fromTxHash)
                hit = await self.db.get_tx_by_hash(n, tx_hash_bytes)
                hits.append({"currency": n, "tx": hit})
            except Exception as _:
                pass

        # Get all networks that have matches
        networks_with_matches = []
        for hit in hits:
            if hit["tx"] is not None:
                networks_with_matches.append(hit["currency"])

        if len(networks_with_matches) == 0:
            logger.warning(
                f"No txs found for sender tx {send_reference.fromTxHash} on any network"
            )
            return None

        if len(networks_with_matches) > 1:
            logger.warning(
                f"Found matches on multiple networks for tx {send_reference.fromTxHash}: {networks_with_matches}"
            )
            return None

        fromNetwork = networks_with_matches[0]

        # Get transactions for the matching network
        txs = [
            x["tx"]["tx_hash"].hex()
            for x in hits
            if x["currency"] == fromNetwork and x["tx"] is not None
        ]
        # remove postfixes _ if exists and get the prefix
        txs = [tx.split("_")[0] if "_" in tx else tx for tx in txs]
        # get unique
        txs = list(set(txs))

        if len(txs) != 1:
            logger.warning(
                f"Expected exactly one tx, got {len(txs)} for {send_reference.fromTxHash}"
            )
            return None

        if fromNetwork in UTXO_NETWORKS:
            return await preliminary_utxo_handling_send(fromNetwork, send_reference)

        elif fromNetwork in ACCOUNT_NETWORKS:
            bridges_generator = get_bridges_from_thorchain_send_from_tx_hash_account(
                fromNetwork, self.db, send_reference.fromTxHash
            )
            return [bridge async for bridge in bridges_generator]

        else:
            raise ValueError(f"Unsupported network: {fromNetwork}")


# Example test cases
if __name__ == "__main__":
    swap_examples = [
        "=:b:bc1q6fxj6446h2gr40wtfkzqyfk848gw4q4t8akr2p:0/1/0:dx:30",
        "=:ETH.ETH:0x19317e026ef473d44D746d364062539Ba7Cb0fa3:117890211/1/0:wr:100",
    ]
    for memo in swap_examples:
        print(f"\nMemo: {memo}")  # noqa: T201
        result = decode_swap(memo)
        if result["is_swap"]:
            print(f"  Target asset: {result.get('asset')}")  # noqa: T201
            print(f"  Destination: {result.get('destination')}")  # noqa: T201
            if "limit" in result:
                print(f"  Limit: {result['limit']}")  # noqa: T201
            if "affiliate" in result:
                # noqa: T201
                print(  # noqa: T201
                    f"  Affiliate: {result['affiliate']} (fee: {result.get('affiliate_fee')} bps)"
                )
        else:
            print(f"  Not a swap: {result.get('error')}")  # noqa: T201

    transfer_examples = [
        "OUT:EA7D80B3EB709319A6577AF6CF4DEFF67975D4F5A93CD8817E7FF04A048D1C5C",
        "REFUND:1234567890ABCDEF",
        "out:abcdef123456",  # lowercase
        "=:BTC.BTC:bc1q...",  # Not a transfer out
    ]

    for memo in transfer_examples:
        print(f"\nMemo: {memo}")  # noqa: T201
        result = decode_withdrawal(memo)
        if result["is_withdrawal"]:
            print(f"  Type: {result['type']}")  # noqa: T201
            print(f"  TX ID: {result.get('tx_id')}")  # noqa: T201
            if "additional_info" in result:
                print(f"  Additional: {result['additional_info']}")  # noqa: T201
        else:
            print(f"  Not a transfer out: {result.get('error')}")  # noqa: T201
