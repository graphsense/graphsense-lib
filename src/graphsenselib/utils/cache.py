from typing import Any, Iterable, List, Tuple

import deltalake
import duckdb
import pandas as pd

# todo might be good to move this to utils
from graphsenselib.ingest.account import from_bytes_df
from graphsenselib.schema.resources.parquet.account_trx import (
    BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX,
)

DEFAULT_KEY_ENCODERS = {
    bytes: lambda x: x.hex(),
    int: lambda x: str(x),
    str: lambda x: x,
}

# todo search repo for fs-cache and delete everything related


class BinaryInterpreter:

    def __init__(self, network: str):
        self.network = network
        if self.network == "trx":
            self.binary_col_conversion_map = (
                BINARY_COL_CONVERSION_MAP_ACCOUNT_TRX  # todo adapt to eth too
            )
        else:
            raise NotImplementedError(f"Network {self.network} not supported")

    def interpret(self, df, tablename) -> Any:
        return from_bytes_df(df, self.binary_col_conversion_map[tablename])


class DeltaTableConnector:
    def __init__(self, base_directory: str, s3_credentials: str):
        self.base_directory = base_directory
        self.s3_credentials = s3_credentials
        # get network from last part of base_directory
        self.network = base_directory.split("/")[-1]
        self.interpreter = BinaryInterpreter(self.network)

    def get_table_path(self, table: str) -> str:
        return f"{self.base_directory}/{table}"

    def get_table_files(self, table_path: str, storage_options: dict) -> List[str]:
        delta_table = deltalake.DeltaTable(table_path, storage_options=storage_options)
        files = delta_table.files()
        files = [f"{table_path}/{file}" for file in files]
        return files

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

    def get_items_fee(self, transactions: pd.DataFrame, default=pd.DataFrame()) -> Any:
        # have to do this since our fees table doesnt save the block_id

        partitions = transactions["partition"].unique()
        # tx_hash_prefixes = transactions["tx_hash_prefix"].unique()
        tx_hashes = transactions["tx_hash"].values

        def transform_to_blob_data(bytearr):
            byte_string = bytes(bytearr)
            escaped_string = "".join(f"\\x{byte:02x}" for byte in byte_string)
            blob_data = f"'{escaped_string}'::BLOB"
            return blob_data

        tx_hashes_blobstr = [transform_to_blob_data(tx_hash) for tx_hash in tx_hashes]
        tx_hashes_str = self.iterable_to_str(tx_hashes_blobstr, True)

        table_path = self.get_table_path("fee")
        table_files = self.get_table_files(table_path, self.get_storage_options())
        auth_query = self.get_auth_query()

        partitions_str = self.iterable_to_str(partitions)

        # this query should take like 10s for a single block on server,
        # according to superset. surely averages down with multiple blocks
        content_query = f"""
        SELECT *
        from     parquet_scan({table_files},HIVE_PARTITIONING=1) WHERE
        partition IN {partitions_str}
        AND tx_hash in {tx_hashes_str}
        """

        # todo prefixes are wrong for fees
        # bytearray to str
        # tx_hashes2 = [tx_hash.hex() for tx_hash in tx_hashes]
        # AND tx_hash_prefix IN {tx_hash_prefixes_str}
        # ;

        query = auth_query + content_query

        con = duckdb.connect()

        con.execute(query)
        data = con.fetchdf()
        return self.interpreter.interpret(data, "fee")

    def get_items(self, table: str, block_ids: List[int]) -> Any:
        table_path = self.get_table_path(table)
        list_str = self.iterable_to_str(block_ids)
        table_files = self.get_table_files(table_path, self.get_storage_options())
        auth_query = self.get_auth_query()

        # todo use scan_delta as soon as we get it to run
        # get all active delta_table_files
        # the following line doesnt work because it tries to access an
        # (i think) aws URL
        # from     delta_scan('{table_path}') WHERE block_id = '{block_id}';
        content_query = f"""
        SELECT *
        from     parquet_scan({table_files},HIVE_PARTITIONING=1)
        WHERE block_id IN {list_str};
        """

        query = auth_query + content_query

        try:
            con = duckdb.connect()
            con.execute(query)
            data = con.fetchdf()

            if not data.empty:
                return self.interpreter.interpret(data, table)
            else:
                raise KeyError(f"block_ids {block_ids} not found in table {table}")
        except Exception as e:
            raise KeyError(
                f"Error retrieving block_ids {block_ids} from table {table}: {e}"
            )

    def __getitem__(self, kv: Tuple[str, List[int]]):
        table, key = kv
        return self.get_items(table, key)

    def get(self, kv: Tuple[str, List[int]], default=None):
        try:
            return self[kv]
        except KeyError:
            return default
