from dataclasses import asdict, dataclass, fields
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

from ..config import GRAPHSENSE_DEFAULT_DATETIME_FORMAT
from ..db import DbFactory


@dataclass
class DbSummaryRecord:
    query_timestamp: str
    currency: str
    raw_keyspace: str
    raw_highest_block: int
    raw_no_blocks: Optional[int]
    raw_no_txs: Optional[int]
    raw_timestamp: Optional[int]
    transformed_keyspace: str
    transformed_highest_block: int
    transformed_no_blocks: int
    transformed_no_address_relations: int
    transformed_no_addresses: int
    transformed_no_txs: int
    transformed_timestamp: int
    transformed_no_clusters: Optional[int]
    transformed_no_cluster_relations: Optional[int]
    transformed_no_blocks_transform: Optional[int]
    transformed_timestamp_transform: Optional[int]

    @classmethod
    def get_fields(cls) -> List[str]:
        return [field.name for field in fields(cls)]

    def get_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_db_summary_record(env: str, currency: str) -> DbSummaryRecord:
    with DbFactory().from_config(env, currency) as db:
        raw_stats = db.raw.get_summary_statistics()
        t_stats = db.transformed.get_summary_statistics()

        return DbSummaryRecord(
            query_timestamp=dt.now().strftime(GRAPHSENSE_DEFAULT_DATETIME_FORMAT),
            currency=currency,
            raw_keyspace=db.raw.get_keyspace(),
            raw_highest_block=db.raw.get_highest_block(sanity_check=False),
            raw_no_blocks=raw_stats.no_blocks if raw_stats is not None else None,
            raw_no_txs=raw_stats.no_txs if raw_stats is not None else None,
            raw_timestamp=raw_stats.timestamp if raw_stats is not None else None,
            transformed_keyspace=db.transformed.get_keyspace(),
            transformed_highest_block=db.transformed.get_highest_block(),
            transformed_no_blocks=t_stats.no_blocks if t_stats is not None else None,
            transformed_no_address_relations=(
                t_stats.no_address_relations if t_stats is not None else None
            ),
            transformed_no_addresses=(
                t_stats.no_addresses if t_stats is not None else None
            ),
            transformed_no_txs=t_stats.no_transactions if t_stats is not None else None,
            transformed_timestamp=t_stats.timestamp if t_stats is not None else None,
            transformed_no_clusters=getattr(t_stats, "no_clusters", None),
            transformed_no_cluster_relations=getattr(
                t_stats, "no_cluster_relations", None
            ),
            transformed_no_blocks_transform=getattr(
                t_stats, "no_blocks_transform", None
            ),
            transformed_timestamp_transform=getattr(
                t_stats, "timestamp_transform", None
            ),
        )


def is_raw_behind_schedule(
    env: str, network: str, threshold_in_hours: int
) -> Tuple[bool, int, str]:
    with DbFactory().from_config(env, network) as db:
        hb = db.raw.get_highest_block()
        hb_ts = db.raw.get_block_timestamp(hb)

        last_ts = hb_ts  # dt.fromtimestamp(hb_ts)
        last_ts_str = last_ts.strftime("%F %H:%M:%S")
        diff_hours = (dt.now() - last_ts).total_seconds() / 3600

        # print(type(dt.datetime.now() - last_ts))
        # print(rs, last_ts_str, diff_hours)

        return (diff_hours > threshold_in_hours, hb, last_ts_str)
