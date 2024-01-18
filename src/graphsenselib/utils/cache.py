from typing import Dict, Tuple

from . import group_by


class TableBasedCache:
    def __init__(self, internal_key_value_cache: Dict, table_delimiter: str = "-"):
        self.internal_key_value_cache = internal_key_value_cache
        self.delim = table_delimiter

    def get_key(self, table: str, key: str):
        assert self.delim not in table
        return f"{key}{self.delim}{table}"

    def put_item(self, table: str, key: str, item):
        self.internal_key_value_cache[self.get_key(table, key)] = item

    def put_items(self, table: str, items: Tuple[str, object]):
        for k, v in items:
            self.put_item(table, k, v)

    def put_items_keyed_by(self, table: str, items: Tuple[str, object], key: str):
        by_key = group_by(items, key=lambda x: x[key])

        self.put_items(table, by_key.items())

    def __setitem__(self, kv: Tuple[str, str], data):
        table, key = kv
        self.put_item(table, key, data)

    def __getitem__(self, kv: Tuple[str, str]):
        table, key = kv
        return self.get_item(table, key)

    def get_item(self, table: str, key: str):
        return self.internal_key_value_cache[self.get_key(table, key)]
