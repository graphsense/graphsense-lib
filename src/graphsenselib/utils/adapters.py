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
        dc = make_dataclass(
            self.dataclass_name,
            [(name, type(field)) for name, field in data_dict.items()],
        )
        return dc(**data_dict)

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
        NewDataClass = make_dataclass(
            self.dataclass_name,
            [(name, type(field)) for name, field in field_dict.items()],
        )
        return NewDataClass(**field_dict)

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


class AccountTransactionAdapter(BlockchainAdapter):
    dataclass_name = "Transaction"

    def __init__(self):
        self.name_remapping = {}
        self.field_processing = {}


class AccountLogAdapter(BlockchainAdapter):
    dataclass_name = "Log"

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

    def __init__(self):
        self.name_remapping = {}
        self.field_processing = {}
