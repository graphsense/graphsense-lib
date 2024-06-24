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
    "0x9cd6008e8d4ebd34fd9d022278fec7f95d133780ecc1a0dea459fae3e9675390": [
        {
            "name": "TokensSent",
            "inputs": [
                {"name": "amount", "type": "uint256", "indexed": False},
                {"name": "recipient", "type": "bytes32", "indexed": False},
                {"name": "destinationChainId", "type": "uint256", "indexed": False},
                {"name": "receiveToken", "type": "bytes32", "indexed": False},
                {"name": "nonce", "type": "uint256", "indexed": False},
                {"name": "messenger", "type": "uint8", "indexed": False},
            ],
        }
    ],
    "0xe9d840d27ab4032a839c20760fb995af8e3ad1980b9428980ca1c7e072acd87a": [
        {
            "name": "TokensReceived",
            "inputs": [
                {"name": "amount", "type": "uint256", "indexed": False},
                {"name": "recipient", "type": "bytes32", "indexed": False},
                {"name": "nonce", "type": "uint256", "indexed": False},
                {"name": "messenger", "type": "uint8", "indexed": False},
                {"name": "message", "type": "bytes32", "indexed": False},
            ],
        }
    ],
    "0x54791b38f3859327992a1ca0590ad3c0f08feba98d1a4f56ab0dca74d203392a": [
        {
            "name": "MessageSent",
            "inputs": [{"name": "index_topic_1", "type": "bytes32", "indexed": True}],
        }
    ],
    "0xfa628b578e095243f0544bfad9255f49d79d03a5bbf6c85875d05a215e247ad2": [
        {
            "name": "SwapReleased",
            "inputs": [{"name": "encodedSwap", "type": "uint256", "indexed": True}],
        }
    ],
    "0x8d92c805c252261fcfff21ee60740eb8a38922469a7e6ee396976d57c22fc1c9": [
        {
            "name": "SwapExecuted",
            "inputs": [{"name": "encodedSwap", "type": "uint256", "indexed": True}],
        },
    ],
    "0x5ce4019f772fda6cb703b26bce3ec3006eb36b73f1d3a0eb441213317d9f5e9d": [
        {
            "name": "SwapPosted",
            "inputs": [{"name": "encodedSwap", "type": "uint256", "indexed": True}],
        },
    ],
    "0x06724742ccc8c330a39a641ef02a0b419bd09248360680bb38159b0a8c2635d6": [
        {
            "name": "LogDeposit",
            "inputs": [
                {"name": "depositorEthKey", "type": "address", "indexed": False},
                {"name": "starkKey", "type": "uint256", "indexed": False},
                {"name": "vaultId", "type": "uint256", "indexed": False},
                {"name": "assetType", "type": "uint256", "indexed": False},
                {"name": "nonQuantizedAmount", "type": "uint256", "indexed": False},
                {"name": "quantizedAmount", "type": "uint256", "indexed": False},
            ],
        },
    ],
    "0x0fcf2162832b2d6033d4d34d2f45a28d9cfee523f1899945bbdd32529cfda67b": [
        {
            "name": "LogNftDeposit",
            "inputs": [
                {"name": "depositorEthKey", "type": "address", "indexed": False},
                {"name": "starkKey", "type": "uint256", "indexed": False},
                {"name": "vaultId", "type": "uint256", "indexed": False},
                {"name": "assetType", "type": "uint256", "indexed": False},
                {"name": "tokenId", "type": "uint256", "indexed": False},
                {"name": "assetId", "type": "uint256", "indexed": False},
            ],
        },
    ],
    "0xed94dc026fa9364c53bc0af51cde7f54f3109b3f31fceb26d01396d80e20453b": [
        {
            "name": "LogDepositWithTokenId",
            "inputs": [
                {"name": "depositorEthKey", "type": "address", "indexed": False},
                {"name": "starkKey", "type": "uint256", "indexed": False},
                {"name": "vaultId", "type": "uint256", "indexed": False},
                {"name": "assetType", "type": "uint256", "indexed": False},
                {"name": "tokenId", "type": "uint256", "indexed": False},
                {"name": "assetId", "type": "uint256", "indexed": False},
                {"name": "nonQuantizedAmount", "type": "uint256", "indexed": False},
                {"name": "quantizedAmount", "type": "uint256", "indexed": False},
            ],
        },
    ],
    "0xb7477a7b93b2addc5272bbd7ad0986ef1c0d0bd265f26c3dc4bbe42727c2ac0c": [
        {
            "name": "LogWithdrawalPerformed",
            "inputs": [
                {"name": "ownerKey", "type": "uint256", "indexed": False},
                {"name": "assetType", "type": "uint256", "indexed": False},
                {"name": "nonQuantizedAmount", "type": "uint256", "indexed": False},
                {"name": "quantizedAmount", "type": "uint256", "indexed": False},
                {"name": "recipient", "type": "address", "indexed": False},
            ],
        },
    ],
    "0xa5cfa8e2199ec5b8ca319288bcab72734207d30569756ee594a74b4df7abbf41": [
        {
            "name": "LogNftWithdrawalPerformed",
            "inputs": [
                {"name": "ownerKey", "type": "uint256", "indexed": False},
                {"name": "assetType", "type": "uint256", "indexed": False},
                {"name": "tokenId", "type": "uint256", "indexed": False},
                {"name": "assetId", "type": "uint256", "indexed": False},
                {"name": "recipient", "type": "address", "indexed": False},
            ],
        },
    ],
    "0xc6ba68235f3229e53f3a95cda25543ad54c0f6df2493a06c05fb930bea7966fe": [
        {
            "name": "LogWithdrawalWithTokenIdPerformed",
            "inputs": [
                {"name": "ownerKey", "type": "uint256", "indexed": False},
                {"name": "assetType", "type": "uint256", "indexed": False},
                {"name": "tokenId", "type": "uint256", "indexed": False},
                {"name": "assetId", "type": "uint256", "indexed": False},
                {"name": "nonQuantizedAmount", "type": "uint256", "indexed": False},
                {"name": "quantizedAmount", "type": "uint256", "indexed": False},
                {"name": "recipient", "type": "address", "indexed": False},
            ],
        },
    ],
    "0x7e6e15df814c1a309a57686de672b2bedd128eacde35c5370c36d6840d4e9a92": [
        {
            "name": "LogMintWithdrawalPerformed",
            "inputs": [
                {"name": "ownerKey", "type": "uint256", "indexed": False},
                {"name": "assetType", "type": "uint256", "indexed": False},
                {"name": "nonQuantizedAmount", "type": "uint256", "indexed": False},
                {"name": "quantizedAmount", "type": "uint256", "indexed": False},
                {"name": "assetId", "type": "uint256", "indexed": False},
            ],
        },
    ],
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9": [
        {
            "name": "PairCreated",
            "inputs": [
                {"name": "token0", "type": "address", "indexed": True},
                {"name": "token1", "type": "address", "indexed": True},
                {"name": "pair", "type": "address", "indexed": False},
                {"name": "x", "type": "uint256", "indexed": False},
            ],
        },
    ],
    "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118": [
        {
            "name": "PoolCreated",
            "inputs": [
                {"name": "token0", "type": "address", "indexed": True},
                {"name": "token1", "type": "address", "indexed": True},
                {"name": "fee", "type": "uint24", "indexed": True},
                {"name": "tickSpacing", "type": "int24", "indexed": False},
                {"name": "pool", "type": "address", "indexed": False},
            ],
        },
    ],
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": [
        {
            "name": "Swap",
            "inputs": [
                {"name": "sender", "type": "address", "indexed": True},
                {"name": "amount0In", "type": "uint256", "indexed": False},
                {"name": "amount1In", "type": "uint256", "indexed": False},
                {"name": "amount0Out", "type": "uint256", "indexed": False},
                {"name": "amount1Out", "type": "uint256", "indexed": False},
                {"name": "to", "type": "address", "indexed": True},
            ],
        },
    ],
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": [
        {
            "name": "Swap",
            "inputs": [
                {"name": "sender", "type": "address", "indexed": True},
                {"name": "recipient", "type": "address", "indexed": True},
                {"name": "amount0", "type": "int256", "indexed": False},
                {"name": "amount1", "type": "int256", "indexed": False},
                {"name": "sqrtPriceX96", "type": "uint160", "indexed": False},
                {"name": "liquidity", "type": "uint128", "indexed": False},
                {"name": "tick", "type": "int24", "indexed": False},
            ],
        },
    ],
    "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65": [
        {
            "name": "Withdrawal",
            "inputs": [
                {"name": "src", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": False},
            ],
        },
    ],
}


def is_supported_log(log) -> bool:
    return len(log["topics"]) > 0 and log["topics"][0] in log_signatures


def convert_db_log(db_log) -> dict:
    data_str = db_log.data.hex()
    return {
        "topics": [f"0x{topic.hex()}" for topic in (db_log.topics or [])],
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
