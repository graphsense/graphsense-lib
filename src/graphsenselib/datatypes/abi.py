import logging

import eth_event

from ..utils import no_nones

logger = logging.getLogger(__name__)


class VersionedDict(dict):
    def __init__(self, mapping, version):
        self.v = version
        super().__init__(mapping)

    def __getitem__(self, key):
        v = super().__getitem__(key)
        return v[self.v]


log_signatures = {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": [
        {
            "name": "Transfer",
            "inputs": [
                {"name": "from", "type": "address", "indexed": True},
                {"name": "to", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": False},
            ],
        },
        {
            "name": "Transfer",
            "inputs": [
                {"name": "from", "type": "address", "indexed": True},
                {"name": "to", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": True},
            ],
        },
    ],
    "0xf285329298fd841af46eb83bbe90d1ebe2951c975a65b19a02f965f842ee69c5": [
        {
            "name": "ChangeOwner",
            "inputs": [{"name": "new_owner", "type": "address", "indexed": True}],
        }
    ],
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": [
        {
            "name": "Approval",
            "inputs": [
                {"name": "owner", "type": "address", "indexed": True},
                {"name": "spender", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": False},
            ],
        },
        {
            "name": "Approval",
            "inputs": [
                {"name": "owner", "type": "address", "indexed": True},
                {"name": "spender", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": True},
            ],
        },
    ],
}


def is_supported_log(log) -> bool:
    return len(log["topics"]) > 0 and log["topics"][0] in log_signatures


def convert_db_log(db_log) -> dict:
    data_str = db_log.data.hex()
    return {
        "topics": [f"0x{topic.hex()}" for topic in db_log.topics],
        "data": f"0x{data_str}",
        "address": f"0x{db_log.address.hex()}",
    }


def decoded_log_to_str(decoded_log) -> str:
    name = decoded_log["name"]
    addr = decoded_log["address"].lower()
    params = ",".join([f"{x['name']}={x['value']}" for x in decoded_log["data"]])
    return f"{addr}|{name}({params})"


def decode_db_logs(db_logs):
    return no_nones([decode_log(convert_db_log(log)) for log in db_logs])


def decode_log(log):
    if is_supported_log(log):
        logdef = log_signatures[log["topics"][0]]
        for i in range(0, len(logdef)):
            try:
                return eth_event.decode_log(log, VersionedDict(log_signatures, i))
            except (eth_event.EventError) as e:
                if i == len(logdef) - 1:
                    logger.info(f"Failed to decode supported log type. {e}. {log}")
    else:
        logger.debug("Can't decode log, not supported yet")
    return None
