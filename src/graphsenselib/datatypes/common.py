from enum import Enum


class EntityType(Enum):
    CLUSTER = "cluster"
    ADDRESS = "address"

    def __str__(self):
        return str(self.value)


class DbChangeType(Enum):
    NEW = "new"
    UPDATE = "update"
    TRUNCATE = "truncate"
    DELETE = "delete"

    def __str__(self):
        return str(self.value)


class FlowDirection(Enum):
    IN = "in"
    OUT = "out"

    def __str__(self):
        return str(self.value)


class NodeType(Enum):
    ADDRESS = "address"
    CLUSTER = "cluster"

    def __str__(self):
        return self.value


# TODO add as enums and remove from config
# schema_types = ["utxo", "account"]
# keyspace_types = ["raw", "transformed"]
