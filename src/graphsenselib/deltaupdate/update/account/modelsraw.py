from typing import Union

from pydantic import BaseModel


class BlockchainAdapter:
    datamodel = None
    name_remapping = {}
    field_processing = {}
    dataclass_name = ""

    def dict_to_dataclass(self, data_dict):
        return self.datamodel.parse_obj(data_dict)

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

    def dicts_to_renamed_dataclasses(self, data_dicts):
        return [self.dict_to_renamed_dataclass(data_dict) for data_dict in data_dicts]

    def process_fields_in_list(self, data_list):
        return [self.process_fields(data_object) for data_object in data_list]


class Trace(BaseModel):
    block_id: int
    tx_hash: Union[bytes, None]
    trace_index: bytes
    from_address: Union[bytes, None]
    to_address: bytes
    value: int
    call_type: Union[str, None]
    status: int


class Transaction(BaseModel):
    transaction_index: int
    tx_hash: bytes
    from_address: Union[bytes, None]
    to_address: bytes
    value: int
    gas_price: int
    transaction_type: int
    receipt_gas_used: int
    receipt_status: int
    block_id: int


class TronTransaction(Transaction):
    fee: Union[int, None]


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
    base_fee_per_gas: int
    gas_used: int


class AccountTraceAdapter(BlockchainAdapter):
    datamodel = Trace


class AccountTransactionAdapter(BlockchainAdapter):
    datamodel = Transaction


class TrxTransactionAdapter(BlockchainAdapter):
    datamodel = TronTransaction


class AccountLogAdapter(BlockchainAdapter):
    datamodel = Log


class TrxTraceAdapter(AccountTraceAdapter):
    def __init__(self):
        self.name_remapping = {
            "caller_address": "from_address",
            "transferto_address": "to_address",
            "rejected": "status",
            "note": "call_type",
            "call_value": "value",
        }
        self.field_processing = {"status": lambda x: int(not x)}  # cast boolean to int


class EthTraceAdapter(AccountTraceAdapter):
    pass


class AccountBlockAdapter(BlockchainAdapter):
    datamodel = Block
