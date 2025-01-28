from typing import Optional

import pandas as pd
from pydantic import BaseModel


class BlockchainAdapter:
    datamodel = None
    name_remapping = {}
    field_processing = {}
    dataclass_name = ""

    def dict_to_dataclass(self, data_dict):
        return self.datamodel.model_validate(data_dict)

    def process_fields(self, data_object):
        # Check if the object is an instance of a dataclass
        for field_name, field_processor in self.field_processing.items():
            setattr(
                data_object,
                field_name,
                field_processor(getattr(data_object, field_name)),
            )
        return data_object

    def rename_dict(self, data_dict):
        for old_name, new_name in self.name_remapping.items():
            if old_name in data_dict:
                data_dict[new_name] = data_dict.pop(old_name)
        return data_dict

    def dict_to_renamed_dataclass(self, data_dict):
        dc = self.datamodel
        renamed_dict = self.rename_dict(data_dict)
        data_req = {k: v for k, v in renamed_dict.items() if k in dc.__annotations__}
        return dc(**data_req)

    def dicts_to_dataclasses(self, data_dicts):
        return [self.dict_to_dataclass(data_dict) for data_dict in data_dicts]

    def df_to_dataclasses(self, df: pd.DataFrame):
        return [
            self.dict_to_dataclass(data_dict)
            for data_dict in df.to_dict(orient="records")
        ]

    def dicts_to_renamed_dataclasses(self, data_dicts):
        return [self.dict_to_renamed_dataclass(data_dict) for data_dict in data_dicts]

    def df_to_renamed_dataclasses(self, df: pd.DataFrame):
        return [
            self.dict_to_renamed_dataclass(data_dict)
            for data_dict in df.to_dict(orient="records")
        ]

    def process_fields_in_list(self, data_list):
        return [self.process_fields(data_object) for data_object in data_list]


class Trace(BaseModel):
    block_id: int
    tx_hash: Optional[bytes]
    trace_index: int
    from_address: Optional[bytes]
    to_address: Optional[bytes]
    value: int
    call_type: Optional[str]
    status: int


class Transaction(BaseModel):
    transaction_index: int
    tx_hash: bytes
    from_address: Optional[bytes]
    to_address: Optional[bytes]
    value: int
    gas_price: int
    transaction_type: int
    receipt_gas_used: int
    receipt_status: int
    block_id: int


class TronTransaction(Transaction):
    fee: Optional[int]
    receipt_contract_address: Optional[bytes]

    # ugly hack because annotations are not inherited
    __annotations__ = {
        **Transaction.__annotations__,
        **{"fee": Optional[int], "receipt_contract_address": Optional[bytes]},
    }


class Log(BaseModel):
    block_id: int
    tx_hash: bytes
    log_index: int
    address: bytes
    topics: list
    data: bytes


class Block(BaseModel):
    block_id: int
    miner: bytes
    base_fee_per_gas: Optional[int]
    gas_used: int


class EthTrace(Trace):
    trace_type: str

    # ugly hack because annotations are not inherited
    __annotations__ = {**Trace.__annotations__, **{"trace_type": str}}


class EthTraceAdapter(BlockchainAdapter):
    datamodel = EthTrace


class AccountTransactionAdapter(BlockchainAdapter):
    datamodel = Transaction


class TrxTransactionAdapter(BlockchainAdapter):
    datamodel = TronTransaction


class AccountLogAdapter(BlockchainAdapter):
    datamodel = Log


class TrxTraceAdapter(BlockchainAdapter):
    datamodel = Trace

    def __init__(self):
        self.name_remapping = {
            "caller_address": "from_address",
            "transferto_address": "to_address",
            "rejected": "status",
            "note": "call_type",
            "call_value": "value",
        }
        self.field_processing = {"status": lambda x: int(not x)}  # cast boolean to int


def zero_if_none(x):
    return x if x is not None else 0


class AccountBlockAdapter(BlockchainAdapter):
    datamodel = Block

    def __init__(self):
        self.field_processing = {"base_fee_per_gas": zero_if_none}
