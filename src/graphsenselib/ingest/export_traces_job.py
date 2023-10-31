import requests
from tron.proto.core.contract_pb2 import TransferContract
from tron.proto.api.api_pb2 import EmptyMessage, NumberMessage, BytesMessage
from tron.client import TronClient
from tron.types import HEX, ADDR
import grpc
from tron.proto.api.api_pb2_grpc import WalletStub
from pprint import pprint
import pandas as pd
import requests
from google.protobuf.json_format import MessageToJson, MessageToDict
import json
from tron.proto.core.response_pb2 import TransactionInfoList



# todo add to requirements: pip install git+https://github.com/tronprotocol/tron-sdk-py
# todo add new field in the graphsense.yaml for the grpc port of the tron node
# todo check if traces are saved in correct order / take note at the correct place that this is unchecked for now


"""
Data we need from the profobuf

block_number	uint64
transaction_hash	hex_string
internal_index	uint
caller_address	address
transferTo_address	address
call_info_index	uint, index of the call info
call_token_id	uint, token id (empty means TRX)
call_value	int64, the amount of the transfered token
note	hex_string
rejected	bool


"""



class TronExportTracesJob:

    def __init__(self,
            start_block: int,
            end_block: int,
            batch_size: int,
            web3,
            max_workers: int
        ):
        self.start_block = start_block
        self.end_block = end_block
        self.batch_size = batch_size
        self.web3 = web3
        self.max_workers = max_workers

    def run(self):

        endpoint_uri = "ikn-vie02-client01:50051" # todo add to config

        channel = grpc.insecure_channel(endpoint_uri)
        wallet_stub = WalletStub(channel)

        # self.start_block, self.end_block = 50_003457, 50_003600

        # decode block of TransactionInfoList protobuf object to get a list of traces
        def decode_block_to_traces(block_number: int, block: TransactionInfoList) -> list:
            transactionInfo = block.transactionInfo
            traces_per_block = []

            #i = 73 #  interesting index for block 50_003_457
            #transactionInfo_i = transactionInfo[i]

            trace_index = 0 # unique per block

            for i, transactionInfo_i in enumerate(transactionInfo):

                internal_transactions = transactionInfo_i.internal_transactions

                if len(internal_transactions) == 0:
                    continue
                
                block_number = block_number
                transaction_hash = transactionInfo_i.id.hex()
                internal_index = i

                # convert RepeatedCompositeContainer to list
                for internal_tx in internal_transactions:
                    caller_address = internal_tx.caller_address.hex() # evm style address as str
                    transferTo_address = internal_tx.transferTo_address.hex()  # evm style address as str
                    callValueInfo = internal_tx.callValueInfo
    
                    note = internal_tx.note.decode("utf-8")
                    rejected = internal_tx.rejected

                    if len(callValueInfo) == 0:
                        call_info_index = None
                        call_token_id = None
                        call_value = None
                        data = {"block_number": block_number,
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
                        call_token_id = callValueInfo_j.tokenId # this returns an empty string if it is TRX #
                        call_token_id = None if call_token_id == "" else call_token_id
                        call_value = callValueInfo_j.callValue

                        internal_transactions = list(internal_transactions)
                        data = {"block_number": block_number,
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

        traces = []
        for i in range(self.start_block, self.end_block + 1):
            msg = NumberMessage(num=i)
            block = wallet_stub.GetTransactionInfoByBlockNum(msg)

            traces_per_block = decode_block_to_traces(i, block)

            traces.extend(traces_per_block)


        # df = pd.DataFrame(traces)


        return traces
