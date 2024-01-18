from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass, make_dataclass


class BlockchainAdapter(ABC):
    @abstractmethod
    def __init__(self):
        self.remapping = {}
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

    def cassandra_rows_to_dataclass(self, rows):
        return [self.cassandra_row_to_dataclass(row) for row in rows]

    def rename_fields(self, data_object):
        # Check if the object is an instance of a dataclass

        if not is_dataclass(data_object):
            raise ValueError("Provided object is not a dataclass")

        # Create a dictionary of the dataclass fields
        field_dict = asdict(data_object)

        # Rename fields based on the remapping dictionary
        for old_name, new_name in self.remapping.items():
            if old_name in field_dict:
                field_dict[new_name] = field_dict.pop(old_name)

        # Create a new dataclass with the new field names
        NewDataClass = make_dataclass(
            self.dataclass_name,
            [(name, type(field)) for name, field in field_dict.items()],
        )
        return NewDataClass(**field_dict)

    def rename_fields_in_list(self, data_list):
        return [self.rename_fields(data_object) for data_object in data_list]


class AccountTraceAdapter(BlockchainAdapter):
    dataclass_name = "Trace"


class TrxTraceAdapter(AccountTraceAdapter):
    def __init__(self):
        self.remapping = {
            "caller_address": "from_address",
            "transferto_address": "to_address",
        }


class EthTraceAdapter(AccountTraceAdapter):
    def __init__(self):
        self.remapping = {}
