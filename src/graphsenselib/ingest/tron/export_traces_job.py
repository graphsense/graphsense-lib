from typing import List

import grpc

from ...utils import remove_prefix
from .grpc.api.tron_api_pb2 import NumberMessage
from .grpc.api.tron_api_pb2_grpc import WalletStub
from .grpc.core.response_pb2 import TransactionInfoList

# todo check if traces are saved in correct order
# / take note at the correct place that this is unchecked for now


def decode_block_to_traces(block_number: int, block: TransactionInfoList) -> List:
    """decode block of TransactionInfoList protobuf object to get a list of traces

    Args:
        block_number (int): Description
        block (TransactionInfoList): Description

    Returns:
        list: Description
    """
    transactionInfo = block.transactionInfo
    traces_per_block = []

    # i = 73 #  interesting index for block 50_003_457
    # transactionInfo_i = transactionInfo[i]

    trace_index = 0  # unique per block

    for i, transactionInfo_i in enumerate(transactionInfo):
        internal_transactions = transactionInfo_i.internal_transactions

        if len(internal_transactions) == 0:
            continue

        block_number = block_number
        transaction_hash = transactionInfo_i.id.hex()
        internal_index = i

        # convert RepeatedCompositeContainer to list
        for internal_tx in internal_transactions:
            caller_address = (
                internal_tx.caller_address.hex()
            )  # evm style address as str
            transferTo_address = (
                internal_tx.transferTo_address.hex()
            )  # evm style address as str
            callValueInfo = internal_tx.callValueInfo

            note = internal_tx.note.decode("utf-8")
            rejected = internal_tx.rejected

            if len(callValueInfo) == 0:
                call_info_index = None
                call_token_id = None
                call_value = None
                data = {
                    "block_number": block_number,
                    "transaction_hash": transaction_hash,
                    "internal_index": internal_index,
                    "caller_address": caller_address,
                    "transferTo_address": transferTo_address,
                    "call_info_index": call_info_index,
                    "call_token_id": call_token_id,
                    "call_value": call_value,
                    "note": note,
                    "rejected": rejected,
                    "trace_index": trace_index,
                }
                trace_index += 1
                traces_per_block.append(data)
                continue

            for j, callValueInfo_j in enumerate(callValueInfo):
                call_info_index = j
                call_token_id = (
                    callValueInfo_j.tokenId
                )  # this returns an empty string if it is TRX #
                call_token_id = None if call_token_id == "" else int(call_token_id)
                call_value = callValueInfo_j.callValue

                internal_transactions = list(internal_transactions)
                data = {
                    "block_number": block_number,
                    "transaction_hash": transaction_hash,
                    "internal_index": internal_index,
                    "caller_address": caller_address,
                    "transferTo_address": transferTo_address,
                    "call_info_index": call_info_index,
                    "call_token_id": call_token_id,
                    "call_value": call_value,
                    "note": note,
                    "rejected": rejected,
                    "trace_index": trace_index,
                }
                trace_index += 1
                traces_per_block.append(data)

    return traces_per_block


def decode_fees(block_number: int, block: TransactionInfoList) -> List:
    transactionInfo = block.transactionInfo

    return [
        {
            "fee": tx.fee,
            "tx_hash": tx.id.hex(),
            "energy_usage": tx.receipt.energy_usage,
            "energy_fee": tx.receipt.energy_fee,
            "origin_energy_usage": tx.receipt.origin_energy_usage,
            "energy_usage_total": tx.receipt.energy_usage_total,
            "net_usage": tx.receipt.net_usage,
            "net_fee": tx.receipt.net_fee,
            "result": tx.receipt.result,
            "energy_penalty_total": tx.receipt.net_fee,
        }
        for tx in transactionInfo
    ]


class TronExportTracesJob:
    def __init__(
        self,
        start_block: int,
        end_block: int,
        batch_size: int,
        grpc_endpoint: str,
        max_workers: int,
    ):
        self.start_block = start_block
        self.end_block = end_block
        self.batch_size = batch_size
        self.grpc_endpoint = remove_prefix(grpc_endpoint, "grpc://")
        self.max_workers = max_workers

    def run(self):
        channel = grpc.insecure_channel(self.grpc_endpoint)
        wallet_stub = WalletStub(channel)

        traces = []
        fees = []
        for i in range(self.start_block, self.end_block + 1):
            block = wallet_stub.GetTransactionInfoByBlockNum(NumberMessage(num=i))

            traces_per_block = decode_block_to_traces(i, block)
            fees_per_block = decode_fees(i, block)

            traces.extend(traces_per_block)
            fees.extend(fees_per_block)

        return traces, fees
