import logging

import eth_event

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
    "0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc": [
        {
            "name": "AddedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": False}],
        },
        {
            "name": "AddedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": True}],
        },
    ],
    "0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c": [
        {
            "name": "RemovedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": False}],
        },
        {
            "name": "RemovedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": True}],
        },
    ],
    "0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a": [
        {
            "name": "Issue",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": False}],
        },
        {
            "name": "Issue",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": True}],
        },
    ],
    "0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44": [
        {
            "name": "Redeem",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": False}],
        },
        {
            "name": "Redeem",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": True}],
        },
    ],
    "0xcc358699805e9a8b7f77b522628c7cb9abd07d9efb86b6fb616af1609036a99e": [
        {
            "name": "Deprecate",
            "inputs": [{"name": "newAddress", "type": "address", "indexed": False}],
        },
        {
            "name": "Deprecate",
            "inputs": [{"name": "newAddress", "type": "address", "indexed": True}],
        },
    ],
    "0xb044a1e409eac5c48e5af22d4af52670dd1a99059537a78b31b48c6500a6354e": [
        {
            "name": "Params",
            "inputs": [
                {"name": "feeBasisPoints", "type": "uint256", "indexed": False},
                {"name": "maxFee", "type": "uint256", "indexed": False},
            ],
        }
    ],
    "0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6": [
        {
            "name": "DestroyedBlackFunds",
            "inputs": [
                {"name": "blackListedUser", "type": "address", "indexed": False},
                {"name": "balance", "type": "uint256", "indexed": False},
            ],
        }
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
    return f"{addr}|{name}({params})".replace("\n", "")


def decode_db_logs(db_logs):
    return [
        x
        for x in [(decode_log(convert_db_log(log)), log) for log in db_logs]
        if x[0] is not None
    ]


def decode_log(log):
    if is_supported_log(log):
        logdef = log_signatures[log["topics"][0]]
        for i in range(0, len(logdef)):
            try:
                return eth_event.decode_log(log, VersionedDict(log_signatures, i))
            except eth_event.EventError as e:
                if i == len(logdef) - 1:
                    logger.info(f"Failed to decode supported log type. {e}. {log}")
    else:
        logger.debug("Can't decode log, not supported yet")
    return None
