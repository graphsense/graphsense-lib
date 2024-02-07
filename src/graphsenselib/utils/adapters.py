from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass, make_dataclass


class BlockchainAdapter(ABC):
    @abstractmethod
    def __init__(self):
        self.name_remapping = (
            {}
        )  # if we want to change the name of a field we can do it here
        self.field_processing = (
            {}
        )  # if we want to change the way a field is processed we can do it here
        self.dataclass_name = ""

    def cassandra_row_to_dataclass(self, row):
        # create a dataclass object from the row, a row is not a dataclass object
        # so we need to convert it
        fields = row._fields  # this is a tuple
        data_dict = {name: getattr(row, name) for name in fields}
        dc = make_dataclass(
            self.dataclass_name,
            [(name, type(field)) for name, field in data_dict.items()],
        )
        return dc(**data_dict)

    def dict_to_dataclass(self, data_dict):
        dc = self.datamodel
        data_req = {k: v for k, v in data_dict.items() if k in self.field_dict}
        return dc(**data_req)

    def rename_fields(self, data_object):
        # Check if the object is an instance of a dataclass

        if not is_dataclass(data_object):
            raise ValueError("Provided object is not a dataclass")

        # Create a dictionary of the dataclass fields
        field_dict = asdict(data_object)

        # Rename fields based on the remapping dictionary
        for old_name, new_name in self.name_remapping.items():
            if old_name in field_dict:
                field_dict[new_name] = field_dict.pop(old_name)

        # Create a new dataclass with the new field names
        NewDataClass = self.datamodel
        # only use the required fields
        req_fields = {k: v for k, v in field_dict.items() if k in self.field_dict}
        return NewDataClass(**req_fields)

    def process_fields(self, data_object):
        # Check if the object is an instance of a dataclass
        for field_name, field_processor in self.field_processing.items():
            setattr(
                data_object,
                field_name,
                field_processor(getattr(data_object, field_name)),
            )
        return data_object

    def dicts_to_dataclasses(self, data_dicts):
        return [self.dict_to_dataclass(data_dict) for data_dict in data_dicts]

    def cassandra_rows_to_dataclasses(self, rows):
        return [self.cassandra_row_to_dataclass(row) for row in rows]

    def process_fields_in_list(self, data_list):
        return [self.process_fields(data_object) for data_object in data_list]

    def rename_fields_in_list(self, data_list):
        return [self.rename_fields(data_object) for data_object in data_list]


class AccountTraceAdapter(BlockchainAdapter):
    dataclass_name = "Trace"
    field_dict = {
        "block_id": int,
        "tx_hash": bytes,
        "trace_index": bytes,
        "from_address": bytes,
        "to_address": bytes,
        "value": int,
        "call_type": str,
        "status": int,
        "error": str,
    }
    datamodel = make_dataclass(  # todo could be placed better
        dataclass_name,
        [(name, type(field)) for name, field in field_dict.items()],
    )


class AccountTransactionAdapter(BlockchainAdapter):
    dataclass_name = "Transaction"
    field_dict = {
        "transaction_index": int,
        "tx_hash": bytes,
        "from_address": bytes,
        "to_address": bytes,
        "value": int,
        "gas_price": int,
        "transaction_type": int,
        "receipt_gas_used": int,
        "receipt_contract_address": bytes,
        "receipt_status": int,
        "block_id": int,
    }
    datamodel = make_dataclass(  # todo could be placed better
        dataclass_name,
        [(name, type(field)) for name, field in field_dict.items()],
    )

    def __init__(self):
        self.name_remapping = {}
        self.field_processing = {}


class AccountLogAdapter(BlockchainAdapter):
    dataclass_name = "Log"
    field_dict = {
        "block_id": int,
        "tx_hash": bytes,
        "log_index": int,
        "address": bytes,
        "topics": list,
        "data": bytes,
    }

    datamodel = make_dataclass(  # todo could be placed better
        dataclass_name,
        [(name, type(field)) for name, field in field_dict.items()],
    )

    def __init__(self):
        self.name_remapping = {}
        self.field_processing = {}


class TrxTraceAdapter(AccountTraceAdapter):
    def __init__(self):
        self.name_remapping = {
            "caller_address": "from_address",
            "transferto_address": "to_address",
            "rejected": "status",
        }
        self.field_processing = {"status": lambda x: int(x)}  # cast boolean to int


class EthTraceAdapter(AccountTraceAdapter):
    def __init__(self):
        self.name_remapping = {}
        self.field_processing = {}


class AccountBlockAdapter(BlockchainAdapter):
    dataclass_name = "Block"
    field_dict = {
        "block_id": int,
        "miner": bytes,
        "base_fee_per_gas": int,
        "gas_used": int,
    }
    datamodel = make_dataclass(  # todo could be placed better
        dataclass_name,
        [(name, type(field)) for name, field in field_dict.items()],
    )

    def __init__(self):
        self.name_remapping = {}
        self.field_processing = {}
