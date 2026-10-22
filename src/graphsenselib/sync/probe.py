"""Cheap questions the scheduler asks before deciding what to run: how far is
the node, how far are raw and transformed, and do our newest blocks still
match the node's chain."""

import logging
from datetime import datetime
from typing import Optional

from ..config import KeyspaceConfig
from ..db import AnalyticsDb, DbFactory
from ..ingest.rpc_eth import BatchRpcClient
from ..utils import first_or_default

logger = logging.getLogger(__name__)

# The probe runs every poll; a node that does not answer within this is
# reported as a failed cycle rather than holding the cycle for minutes.
_RPC_TIMEOUT_SECONDS = 30
_RPC_MAX_RETRIES = 3


class ChainProbe:
    def __init__(self, env: str, network: str, ks_config: KeyspaceConfig):
        self.env = env
        self.network = network
        self.schema_type = ks_config.schema_type
        ic = ks_config.ingest_config
        sources = ic.all_node_references if ic is not None else []
        # Same choice as the ingest pipeline: the first http(s) reference is
        # the JSON-RPC endpoint (for trx the fullnode's eth-compatible one).
        self.provider_uri = first_or_default(sources, lambda x: x.startswith("http"))
        self._rpc: Optional[BatchRpcClient] = None
        self._db: Optional[AnalyticsDb] = None

    # -- node --------------------------------------------------------------

    def _client(self) -> BatchRpcClient:
        if self.provider_uri is None:
            raise ValueError(
                f"No http node reference configured for {self.env}/{self.network}"
            )
        if self._rpc is None:
            self._rpc = BatchRpcClient(self.provider_uri, timeout=_RPC_TIMEOUT_SECONDS)
        return self._rpc

    def node_tip(self) -> int:
        if self.schema_type == "utxo":
            return int(
                self._client().make_request(
                    "getblockcount", [], max_retries=_RPC_MAX_RETRIES
                )
            )
        result = self._client().make_request(
            "eth_blockNumber", [], max_retries=_RPC_MAX_RETRIES
        )
        return int(result, 16)

    def node_block_hash(self, height: int) -> Optional[str]:
        """Lowercase hex without 0x -- the form raw stores as bytes."""
        if self.schema_type == "utxo":
            h = self._client().make_request(
                "getblockhash", [height], max_retries=_RPC_MAX_RETRIES
            )
        else:
            block = self._client().make_request(
                "eth_getBlockByNumber",
                [hex(height), False],
                max_retries=_RPC_MAX_RETRIES,
            )
            h = block.get("hash") if block else None
        return _normalize_hash(h) if h else None

    # -- database ----------------------------------------------------------

    def db(self) -> AnalyticsDb:
        if self._db is None:
            db = DbFactory().from_config(self.env, self.network, readonly=True)
            db.open()
            self._db = db
        return self._db

    def raw_height(self) -> Optional[int]:
        raw = self.db().raw
        return raw.get_highest_block() if raw.exists() else None

    def transformed_height(self) -> Optional[int]:
        transformed = self.db().transformed
        if not transformed.exists():
            return None
        return transformed.get_highest_block_delta_updater()

    def raw_block_hash(self, height: int) -> Optional[str]:
        raw = self.db().raw
        bucket_size = raw.get_block_bucket_size()
        result = raw.select(
            "block",
            columns=["block_hash"],
            where={
                "block_id_group": raw.get_id_group(height, bucket_size),
                "block_id": height,
            },
            limit=1,
        )
        rows = result.current_rows
        if not rows or rows[0].block_hash is None:
            return None
        return bytes(rows[0].block_hash).hex()

    def raw_block_timestamp(self, height: int) -> Optional[datetime]:
        return self.db().raw.get_block_timestamp(height)

    def wal_pending(self) -> Optional[dict]:
        """Header of a pending delta-update WAL record (run_id, code_version,
        block_lo, block_hi, ...), or None, also when there is no WAL table.
        Read only: never ensure_schema/recover from here."""
        from ..deltaupdate.wal import WAL_TABLE_NAME, DeltaWal

        tdb = self.db().transformed
        if not tdb._db.has_table(tdb.get_keyspace(), WAL_TABLE_NAME):
            return None
        return DeltaWal(tdb, "sync-status", "n/a").pending_header()

    def reset(self):
        """Drop connections after an error; the next call reconnects."""
        self.close()

    def close(self):
        if self._db is not None:
            try:
                self._db.close()
            except Exception as e:
                logger.debug(f"Closing probe db for {self.network}: {e}")
            self._db = None
        self._rpc = None


def _normalize_hash(h: str) -> str:
    h = h.lower()
    return h[2:] if h.startswith("0x") else h
