# -*- coding: utf-8 -*-
# flake8: noqa: T201
from typing import Optional

import numpy as np
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import dict_factory
from pandas import DataFrame
from pandas import pandas as pd

from graphsenselib.utils.tron import tron_address_to_evm, evm_to_tron_address_string
from graphsenselib.utils.rest_utils import is_eth_like
import logging

logger = logging.getLogger(__name__)


def try_convert_tron_to_eth(x):
    try:
        if x.startswith("0x"):
            return x
        return eth_address_to_hex_str(tron_address_to_evm(x))
    except Exception as e:
        logger.warning(f"Can't convert address {x} to eth format; {e}")
        return None


def try_convert_to_tron(x):
    try:
        if x is None:
            return None
        else:
            return evm_to_tron_address_string(eth_address_to_hex_str(x))
    except Exception as e:
        logger.warning(f"Can't convert address {x} to tron format; {e}")
        return None


def eth_address_to_hex(address):
    if type(address) is bytes:
        return address
    return "0x" + address.hex()


def eth_address_to_hex_str(address):
    return "0x" + address.hex()


def eth_address_from_hex(address):
    # eth addresses are case insensitive
    try:
        b = bytes.fromhex(address[2:].lower())
    except Exception as e:
        logger.warning(f"can't convert to hex {address}; {e}")
        return None
    return b


_CONCURRENCY = 100


class GraphSense(object):
    def __init__(
        self,
        hosts: list,
        ks_map: dict,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.hosts = hosts
        self.ks_map = ks_map

        auth_provider = None
        if username is not None:
            auth_provider = PlainTextAuthProvider(username=username, password=password)

        self.cluster = Cluster(hosts, auth_provider=auth_provider)
        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory

    def close(self):
        self.cluster.shutdown()
        logger.info(f"Disconnected from {self.hosts}")

    def _execute_query(self, statement, parameters):
        """Generic query execution"""
        results = execute_concurrent_with_args(
            self.session, statement, parameters, concurrency=_CONCURRENCY
        )

        i = 0
        all_results = []
        for success, result in results:
            if not success:
                logger.warning("failed" + result)
            else:
                for row in result:
                    i = i + 1
                    all_results.append(row)
        return pd.DataFrame.from_dict(all_results)

    def contains_keyspace_mapping(self, network: str) -> bool:
        return network in self.ks_map

    def _check_passed_params(self, df: DataFrame, network: str, req_column: str):
        if df.empty:
            raise Exception(f"Received empty dataframe for network {network}")
        if req_column not in df.columns:
            raise Exception(f"Missing column {req_column}")
        if not self.contains_keyspace_mapping(network):
            raise Exception(f"Network {network} not in keyspace mapping")

    def _query_keyspace_config(self, keyspace: str) -> dict:
        self.session.set_keyspace(keyspace)
        query = "SELECT * FROM configuration"
        result = self.session.execute(query)
        return result[0]

    def keyspace_for_network_exists(self, network: str) -> bool:
        if self.contains_keyspace_mapping(network):
            for k, keyspace in self.ks_map[network].items():
                query = "SELECT keyspace_name FROM system_schema.keyspaces"
                result = self.session.execute(query)
                keyspaces = [row["keyspace_name"] for row in result]

                if keyspace not in keyspaces:
                    return False

            return True
        else:
            return False

    def get_address_ids(self, df: DataFrame, network: str) -> DataFrame:
        """Get address ids for all passed addresses"""
        self._check_passed_params(df, network, "address")

        keyspace = self.ks_map[network]["transformed"]
        ks_config = self._query_keyspace_config(keyspace)
        self.session.set_keyspace(keyspace)

        df_temp = df[["address"]].copy()
        df_temp = df_temp.drop_duplicates()

        if network == "TRX":
            # convert t-style to evm
            df_temp["address"] = df_temp["address"].apply(try_convert_tron_to_eth)

            # filter non convertible addresses
            df_temp = df_temp[df_temp["address"].notnull()]

            df_temp["address_prefix"] = df_temp["address"].str[
                2 : 2 + ks_config["address_prefix_length"]
            ]
            df_temp["address_prefix"] = df_temp["address_prefix"].apply(
                lambda x: x.upper()
            )
            df_temp["address"] = df_temp["address"].apply(
                lambda x: eth_address_from_hex(x)
            )

            # the last step can fail two, eg, wrongly encoded addresses
            # so we filter again filter non convertible addresses
            df_temp = df_temp[df_temp["address"].notnull()]

        elif network == "ETH":
            df_temp["address_prefix"] = df_temp["address"].str[
                2 : 2 + ks_config["address_prefix_length"]
            ]
            df_temp["address_prefix"] = df_temp["address_prefix"].apply(
                lambda x: x.upper()
            )

            df_temp["address"] = df["address"].apply(lambda x: eth_address_from_hex(x))

            # the last step can fail two, eg, wrongly encoded addresses
            # so we filter again filter non convertible addresses
            df_temp = df_temp[df_temp["address"].notnull()]
        else:
            if "bech_32_prefix" in ks_config:
                df_temp["a"] = df_temp["address"].apply(
                    lambda x: x.replace(ks_config["bech_32_prefix"], "")
                )

            df_temp["address_prefix"] = df_temp["a"].str[
                : ks_config["address_prefix_length"]
            ]

        query = (
            "SELECT address, address_id "
            + "FROM address_ids_by_address_prefix "
            + "WHERE address_prefix=? and address=?"
        )

        statement = self.session.prepare(query)
        parameters = df_temp[["address_prefix", "address"]].to_records(index=False)

        result = self._execute_query(statement, parameters)

        if len(result) > 0:
            if network == "ETH":
                result["address"] = result["address"].apply(eth_address_to_hex_str)
            elif network == "TRX":
                # convert evm to t-style address
                result["address"] = result["address"].apply(try_convert_to_tron)

        return result

    def get_cluster_ids(self, df: DataFrame, network: str) -> DataFrame:
        """Get cluster ids for all passed address ids"""
        self._check_passed_params(df, network, "address_id")

        if is_eth_like(network):
            raise Exception(f"{network} does not have clusters")

        keyspace = self.ks_map[network]["transformed"]
        ks_config = self._query_keyspace_config(keyspace)
        self.session.set_keyspace(keyspace)

        df_temp = df[["address_id"]].copy()
        df_temp = df_temp.drop_duplicates()
        df_temp["address_id_group"] = np.floor(
            df_temp["address_id"] / ks_config["bucket_size"]
        ).astype(int)

        query = (
            "SELECT address_id, cluster_id "
            + "FROM address WHERE address_id_group=? and address_id=?"
        )
        statement = self.session.prepare(query)
        parameters = df_temp[["address_id_group", "address_id"]].to_records(index=False)

        return self._execute_query(statement, parameters)

    def get_clusters(self, df: DataFrame, network: str) -> DataFrame:
        """Get clusters for all passed cluster ids"""
        self._check_passed_params(df, network, "cluster_id")

        if is_eth_like(network):
            raise Exception(f"{network} does not have clusters")

        keyspace = self.ks_map[network]["transformed"]
        ks_config = self._query_keyspace_config(keyspace)
        self.session.set_keyspace(keyspace)

        df_temp = df[["cluster_id"]].copy()
        df_temp = df_temp.drop_duplicates()
        df_temp["cluster_id_group"] = np.floor(
            df_temp["cluster_id"] / ks_config["bucket_size"]
        ).astype(int)

        query = "SELECT * FROM cluster " + "WHERE cluster_id_group=? and cluster_id=?"
        statement = self.session.prepare(query)
        parameters = df_temp[["cluster_id_group", "cluster_id"]].to_records(index=False)

        return self._execute_query(statement, parameters)

    def _get_cluster_definers(self, df: DataFrame, network: str) -> DataFrame:
        keyspace = self.ks_map[network]["transformed"]
        ks_config = self._query_keyspace_config(keyspace)
        self.session.set_keyspace(keyspace)

        df_temp = df[["cluster_id"]].copy()
        df_temp.rename(columns={"cluster_id": "address_id"}, inplace=True)
        df_temp = df_temp.drop_duplicates()
        df_temp["address_id_group"] = np.floor(
            df_temp["address_id"] / ks_config["bucket_size"]
        ).astype(int)

        query = (
            "SELECT address_id as cluster_id, "
            "address as cluster_defining_address FROM address "
            + "WHERE address_id_group=? and address_id=?"
        )
        statement = self.session.prepare(query)
        parameters = df_temp[["address_id_group", "address_id"]].to_records(index=False)

        return self._execute_query(statement, parameters)

    def get_address_clusters(self, df: DataFrame, network: str) -> DataFrame:
        self._check_passed_params(df, network, "address")

        addresses = df.copy()

        if network == "ETH":
            # tagpacks include invalid ETH addresses, ignore those
            addresses.drop(
                addresses[~addresses.address.str.startswith("0x")].index, inplace=True
            )
            addresses.rename(columns={"address": "checksum_address"}, inplace=True)
            addresses.loc[:, "address"] = addresses["checksum_address"].str.lower()
        elif network == "TRX":
            addresses.rename(columns={"address": "checksum_address"}, inplace=True)
            addresses.loc[:, "address"] = addresses["checksum_address"]

        df_address_ids = self.get_address_ids(addresses, network)
        if len(df_address_ids) == 0:
            return DataFrame()

        if is_eth_like(network):
            df_address_ids["cluster_id"] = df_address_ids["address_id"]
            df_address_ids["no_addresses"] = 1

            result = df_address_ids.merge(addresses, on="address")

            result.drop("address", axis="columns", inplace=True)
            result.rename(columns={"checksum_address": "address"}, inplace=True)
            result["cluster_defining_address"] = result["address"]

            return result

        df_cluster_ids = self.get_cluster_ids(df_address_ids, network)
        if len(df_cluster_ids) == 0:
            return DataFrame()

        df_cluster_definers = self._get_cluster_definers(df_cluster_ids, network)

        df_address_clusters = self.get_clusters(df_cluster_ids, network)
        if len(df_address_clusters) == 0:
            return DataFrame()

        result = (
            df_address_ids.merge(df_cluster_ids, on="address_id", how="left")
            .merge(df_address_clusters, on="cluster_id", how="left")
            .merge(df_cluster_definers, on="cluster_id", how="left")
        )

        return result
