from typing import Iterable, List, Tuple

try:
    import deltalake
    import duckdb
except ImportError:
    _has_delta_dependencies = False
else:
    _has_delta_dependencies = True


import pandas as pd

from graphsenselib.ingest.account import from_bytes_df
from graphsenselib.ingest.dump import PARTITIONSIZES
from graphsenselib.schema.resources.parquet.account import (
    BINARY_COL_CONVERSION_MAP_ACCOUNT,
)
from graphsenselib.schema.resources.parquet.account_trx import (
    BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX,
)

DEFAULT_KEY_ENCODERS = {
    bytes: lambda x: x.hex(),
    int: lambda x: str(x),
    str: lambda x: x,
}


class EmptyDeltaTableException(Exception):
    pass


class BinaryInterpreter:
    def __init__(self, network: str):
        self.network = network
        if self.network == "trx":
            self.binary_col_conversion_map = BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX
        elif self.network == "eth":
            self.binary_col_conversion_map = BINARY_COL_CONVERSION_MAP_ACCOUNT
        elif self.network == "btc":
            self.binary_col_conversion_map = {}
        else:
            raise NotImplementedError(f"Network {self.network} not supported")

    def interpret(self, df, tablename) -> pd.DataFrame:
        conversion_map_table = self.binary_col_conversion_map.get(tablename)

        if not conversion_map_table:
            return df
        return from_bytes_df(df, conversion_map_table)


class DeltaTableConnector:
    def __init__(self, base_directory: str, s3_credentials: str):
        if not _has_delta_dependencies:
            raise ImportError(
                "The Connector needs duckdb and deltalake installed. Please install gslib with ingest dependencies."
            )
        self.base_directory = base_directory
        self.s3_credentials = s3_credentials
        # get network from last part of base_directory
        self.network = base_directory.split("/")[-1]
        self.interpreter = BinaryInterpreter(self.network)

    def get_table_path(self, table: str) -> str:
        return f"{self.base_directory}/{table}"

    def get_table_files(self, table_path: str) -> List[str]:
        # takes about 1s locally accessing MINIO on cluster
        storage_options = self.get_storage_options()
        delta_table = deltalake.DeltaTable(table_path, storage_options=storage_options)
        files = delta_table.files()
        files = [f"{table_path}/{file}" for file in files]
        return files

    def list_partitions(self, table: str) -> List[int]:
        table_path = self.get_table_path(table)
        storage_options = self.get_storage_options()
        delta_table = deltalake.DeltaTable(table_path, storage_options=storage_options)
        partitions = delta_table._table.get_active_partitions()
        partitions = [int(list(p)[0][1]) for p in partitions]
        partitions.sort()

        return partitions

    def get_auth_query(self):
        if self.s3_credentials:
            endpoint_URL = self.s3_credentials.get("AWS_ENDPOINT_URL").replace(
                "http://", ""
            )
            password = self.s3_credentials.get("AWS_SECRET_ACCESS_KEY")
            access_key = self.s3_credentials.get("AWS_ACCESS_KEY_ID")

            auth_query = f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style='path';
            SET s3_use_ssl=0;
            SET s3_region='us-east-1';
            SET s3_endpoint='{endpoint_URL}';
            SET s3_access_key_id='{access_key}';
            SET s3_secret_access_key='{password}';
            """
        else:
            auth_query = ""

        return auth_query

    def get_storage_options(self):
        if self.s3_credentials:
            storage_options = {
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            }
            storage_options.update(self.s3_credentials)
        else:
            storage_options = {}

        return storage_options

    def iterable_to_str(self, it: Iterable, no_ticks=False) -> str:
        s = ""
        for i in it:
            if not no_ticks:
                s += f"'{i}',"
            else:
                s += str(i) + ","

        res = "(" + s[:-1] + ")"

        return res

    def make_displayable(self, data: pd.DataFrame):
        # iterate through all fields and convert bytes to hex

        def converter(x):
            if isinstance(x, bytearray):
                return x.hex()
            else:
                return x

        for col in data.columns:
            data[col] = data[col].apply(converter)

        return data

    def get_items(self, table: str, block_ids: List[int]) -> pd.DataFrame:
        table_path = self.get_table_path(table)
        list_str = self.iterable_to_str(block_ids)
        table_files = self.get_table_files(table_path)
        auth_query = self.get_auth_query()
        partitionsize = PARTITIONSIZES[self.network]
        partitions = [block_id // partitionsize for block_id in block_ids]
        partition_str = self.iterable_to_str(partitions)

        # todo use scan_delta as soon as we get it to run
        # get all active delta_table_files
        # the following line doesnt work because it tries to access an
        # (i think) aws URL
        # from     delta_scan('{table_path}') WHERE block_id = '{block_id}';
        content_query = f"""
        SELECT *
        from     parquet_scan({table_files},HIVE_PARTITIONING=1)
        WHERE partition IN {partition_str}
        AND block_id IN {list_str};
        """

        query = auth_query + content_query

        con = duckdb.connect()
        con.execute(query)
        data = con.fetchdf()

        if not data.empty:
            return self.interpreter.interpret(data, table)
        else:
            raise EmptyDeltaTableException(
                f"block_ids {block_ids} not found in table {table}"
            )

    def __getitem__(self, kv: Tuple[str, List[int]]):
        table, key = kv
        return self.get_items(table, key)

    def get(self, kv: Tuple[str, List[int]], default=None):
        try:
            return self[kv]
        except EmptyDeltaTableException:
            return default
