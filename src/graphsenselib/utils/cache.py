from typing import Dict, Tuple, Union

from . import group_by

DEFAULT_KEY_ENCODERS = {
    bytes: lambda x: x.hex(),
    int: lambda x: str(x),
    str: lambda x: x,
}


class TableBasedCache:
    def __init__(
        self,
        internal_key_value_cache: Dict,
        table_delimiter: str = "|",
        key_encoder=DEFAULT_KEY_ENCODERS,
    ):
        self.internal_key_value_cache = internal_key_value_cache
        self.delim = table_delimiter
        self.key_encoder = key_encoder

    def get_key(self, table: str, key: Union[bytes, int, str]):
        assert self.delim not in table
        kt = type(key)
        if kt not in self.key_encoder:
            raise ValueError(f"Don't know how to encode key of type {kt}")
        ekey = self.key_encoder[kt](key)
        return f"{ekey}{self.delim}{table}"

    def __delitem__(self, kv: Tuple[str, Union[bytes, int, str]]):
        table, key = kv
        self.delete_item(table, key)

    def delete_item(self, table: str, key: Union[bytes, int, str]):
        self.internal_key_value_cache.delete(self.get_key(table, key), retry=True)

    def put_item(self, table: str, key: Union[bytes, int, str], item):
        self.internal_key_value_cache[self.get_key(table, key)] = item

    def put_items(self, table: str, items: Tuple[str, object]):
        for k, v in items:
            self.put_item(table, k, v)

    def put_items_keyed_by(self, table: str, items: Tuple[str, object], key: str):
        by_key = group_by(items, key=lambda x: x[key])

        self.put_items(table, by_key.items())

    def __setitem__(self, kv: Tuple[str, Union[bytes, int, str]], data):
        table, key = kv
        self.put_item(table, key, data)

    def __getitem__(self, kv: Tuple[str, Union[bytes, int, str]]):
        table, key = kv
        return self.get_item(table, key)

    def get(self, kv: Tuple[str, Union[bytes, int, str]], default=None):
        try:
            return self[kv]
        except KeyError:
            return default

    def get_item(self, table: str, key: str):
        return self.internal_key_value_cache[self.get_key(table, key)]
