import logging
import re
from typing import Any, Dict, List, Tuple

import eth_event

from ..utils.generic import dict_to_dataobject

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
            "tags": ["token", "erc20"],
        },
        {
            "name": "Transfer",
            "inputs": [
                {"name": "from", "type": "address", "indexed": True},
                {"name": "to", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": True},
            ],
            "tags": ["token", "erc20"],
        },
    ],
    "0xf285329298fd841af46eb83bbe90d1ebe2951c975a65b19a02f965f842ee69c5": [
        {
            "name": "ChangeOwner",
            "inputs": [{"name": "new_owner", "type": "address", "indexed": True}],
            "tags": ["ownership"],
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
            "tags": ["token", "erc20"],
        },
        {
            "name": "Approval",
            "inputs": [
                {"name": "owner", "type": "address", "indexed": True},
                {"name": "spender", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": True},
            ],
            "tags": ["token", "erc20"],
        },
    ],
    "0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc": [
        {
            "name": "AddedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": False}],
            "tags": ["blacklist"],
        },
        {
            "name": "AddedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": True}],
            "tags": ["blacklist"],
        },
    ],
    "0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c": [
        {
            "name": "RemovedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": False}],
            "tags": ["blacklist"],
        },
        {
            "name": "RemovedBlackList",
            "inputs": [{"name": "user", "type": "address", "indexed": True}],
            "tags": ["blacklist"],
        },
    ],
    "0xcb8241adb0c3fdb35b70c24ce35c5eb0c17af7431c99f827d44a445ca624176a": [
        {
            "name": "Issue",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": False}],
            "tags": ["supply-management"],
        },
        {
            "name": "Issue",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": True}],
            "tags": ["supply-management"],
        },
    ],
    "0x702d5967f45f6513a38ffc42d6ba9bf230bd40e8f53b16363c7eb4fd2deb9a44": [
        {
            "name": "Redeem",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": False}],
            "tags": ["supply-management"],
        },
        {
            "name": "Redeem",
            "inputs": [{"name": "amount", "type": "uint256", "indexed": True}],
            "tags": ["supply-management"],
        },
    ],
    "0xcc358699805e9a8b7f77b522628c7cb9abd07d9efb86b6fb616af1609036a99e": [
        {
            "name": "Deprecate",
            "inputs": [{"name": "newAddress", "type": "address", "indexed": False}],
            "tags": ["version-management"],
        },
        {
            "name": "Deprecate",
            "inputs": [{"name": "newAddress", "type": "address", "indexed": True}],
            "tags": ["version-management"],
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
            "tags": ["supply-management"],
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
            "tags": ["bridging"],
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
            "tags": ["bridging"],
        }
    ],
    "0x54791b38f3859327992a1ca0590ad3c0f08feba98d1a4f56ab0dca74d203392a": [
        {
            "name": "MessageSent",
            "inputs": [{"name": "index_topic_1", "type": "bytes32", "indexed": True}],
            "tags": ["bridging"],
        }
    ],
    "0xfa628b578e095243f0544bfad9255f49d79d03a5bbf6c85875d05a215e247ad2": [
        {
            "name": "SwapReleased",
            "inputs": [{"name": "encodedSwap", "type": "uint256", "indexed": True}],
            "tags": ["bridging"],
        }
    ],
    "0x8d92c805c252261fcfff21ee60740eb8a38922469a7e6ee396976d57c22fc1c9": [
        {
            "name": "SwapExecuted",
            "inputs": [{"name": "encodedSwap", "type": "uint256", "indexed": True}],
            "tags": ["bridging"],
        },
    ],
    "0x5ce4019f772fda6cb703b26bce3ec3006eb36b73f1d3a0eb441213317d9f5e9d": [
        {
            "name": "SwapPosted",
            "inputs": [{"name": "encodedSwap", "type": "uint256", "indexed": True}],
            "tags": ["bridging"],
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
            "tags": ["bridging"],
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
            "tags": ["bridging"],
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
            "tags": ["bridging"],
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
            "tags": ["bridging"],
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
            "tags": ["bridging"],
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
            "tags": ["bridging"],
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
            "tags": ["bridging"],
        },
    ],
    "0x9d42cb017eb05bd8944ab536a8b35bc68085931dd5f4356489801453923953f9": [
        {
            "name": "NewExchange",
            "inputs": [
                {"name": "token", "type": "address", "indexed": True},
                {"name": "exchange", "type": "address", "indexed": True},
            ],
            "tags": ["uniswap_v1", "dex-pair-created"],
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
            "tags": ["uniswap_v2", "dex-pair-created"],
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
            "tags": ["uniswap_v3", "dex-pair-created"],
        },
    ],
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438": [
        {
            "name": "Initialize",
            "inputs": [
                {"name": "id", "type": "bytes32", "indexed": True},
                {"name": "currency0", "type": "address", "indexed": True},
                {"name": "currency1", "type": "address", "indexed": True},
                {"name": "fee", "type": "uint24", "indexed": False},
                {"name": "tickSpacing", "type": "int24", "indexed": False},
                {"name": "hooks", "type": "address", "indexed": False},
                {"name": "sqrtPriceX96", "type": "uint160", "indexed": False},
                {"name": "tick", "type": "int24", "indexed": False},
            ],
            "tags": ["uniswap_v4", "dex-pair-created"],
        },
    ],
    "0xcd60aa75dea3072fbc07ae6d7d856b5dc5f4eee88854f5b4abf7b680ef8bc50f": [
        {
            "name": "TokenPurchase",
            "inputs": [
                {"name": "buyer", "type": "address", "indexed": True},
                {"name": "eth_sold", "type": "uint256", "indexed": True},
                {"name": "tokens_bought", "type": "uint256", "indexed": True},
            ],
            "tags": ["uniswap_v1"],
        },
    ],
    "0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705": [
        {
            "name": "EthPurchase",
            "inputs": [
                {"name": "buyer", "type": "address", "indexed": True},
                {"name": "eth_sold", "type": "uint256", "indexed": True},
                {"name": "eth_bought", "type": "uint256", "indexed": True},
            ],
            "tags": ["uniswap_v1"],
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
            "tags": ["uniswap_v2", "swap"],
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
            "tags": ["uniswap_v3", "swap"],
        },
    ],
    "0x1bb43f2da90e35f7b0cf38521ca95a49e68eb42fac49924930a5bd73cdf7576c": [
        {
            "name": "OrderRecord",
            "inputs": [
                {"name": "fromToken", "type": "address", "indexed": False},
                {"name": "toToken", "type": "address", "indexed": False},
                {"name": "sender", "type": "address", "indexed": False},
                {"name": "fromAmount", "type": "uint256", "indexed": False},
                {"name": "toAmount", "type": "uint256", "indexed": False},
            ],
            "tags": ["okx", "order-record", "swap"],
        }
    ],
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f": [
        {
            "name": "Swap",
            "inputs": [
                {"name": "id", "type": "bytes32", "indexed": True},
                {"name": "sender", "type": "address", "indexed": True},
                {"name": "amount0", "type": "int128", "indexed": False},
                {"name": "amount1", "type": "int128", "indexed": False},
                {"name": "sqrtPriceX96", "type": "uint160", "indexed": False},
                {"name": "liquidity", "type": "uint128", "indexed": False},
                {"name": "tick", "type": "int24", "indexed": False},
                {"name": "fee", "type": "uint24", "indexed": False},
            ],
            "tags": ["uniswap_v4", "swap"],
        },
    ],
    "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65": [
        {
            "name": "Withdrawal",
            "inputs": [
                {"name": "src", "type": "address", "indexed": True},
                {"name": "value", "type": "uint256", "indexed": False},
            ],
            "tags": ["bridging"],
        },
        {
            "name": "Withdrawal",
            "inputs": [
                {"name": "src", "type": "address", "indexed": True},
                {"name": "wad", "type": "uint256", "indexed": False},
            ],
            "tags": ["weth", "unwrap", "token"],
        },
    ],
    "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c": [
        {
            "name": "Deposit",
            "inputs": [
                {"name": "dst", "type": "address", "indexed": True},
                {"name": "wad", "type": "uint256", "indexed": False},
            ],
            "tags": ["weth", "wrap", "token"],
        },
    ],
    "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17": [
        {
            "name": "Trade",
            "inputs": [
                {"name": "owner", "type": "address", "indexed": True},
                {"name": "sellToken", "type": "address", "indexed": False},
                {"name": "buyToken", "type": "address", "indexed": False},
                {"name": "sellAmount", "type": "uint256", "indexed": False},
                {"name": "buyAmount", "type": "uint256", "indexed": False},
                {"name": "feeAmount", "type": "uint256", "indexed": False},
                {"name": "orderUid", "type": "bytes", "indexed": False},
            ],
            "tags": ["cow-protocol", "trade", "swap", "dex"],
        },
    ],
    "0x40338ce1a7c49204f0099533b1e9a7ee0a3d261f84974ab7af36105b8c4e9db4": [
        {
            "name": "Settlement",
            "inputs": [
                {"name": "solver", "type": "address", "indexed": True},
            ],
            "tags": ["cow-protocol", "settlement"],
        },
    ],
    "0x5844b8bbe3fd2b0354e73f27bfde28d2e6d991f14139c382876ec4360391a47b": [
        {
            "name": "ExpressExecutedWithToken",
            "inputs": [
                {"name": "commandId", "type": "bytes32", "indexed": True},
                {"name": "sourceChain", "type": "string", "indexed": False},
                {"name": "sourceAddress", "type": "string", "indexed": False},
                {"name": "payloadHash", "type": "bytes32", "indexed": False},
                {"name": "symbol", "type": "string", "indexed": False},
                {"name": "amount", "type": "uint256", "indexed": True},
                {"name": "expressExecutor", "type": "address", "indexed": True},
            ],
            "tags": ["squid", "bridging", "express-execution"],
        },
    ],
    "0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2": [
        {
            "name": "LogMessagePublished",
            "inputs": [
                {"name": "sender", "type": "address", "indexed": True},
                {"name": "sequence", "type": "uint64", "indexed": False},
                {"name": "nonce", "type": "uint32", "indexed": False},
                {"name": "payload", "type": "bytes", "indexed": False},
                {"name": "consistencyLevel", "type": "uint8", "indexed": False},
            ],
            "tags": ["wormhole", "bridging"],
        }
    ],
    "0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f": [
        {
            "name": "Swap",
            "inputs": [
                {"name": "chainId", "type": "uint16", "indexed": False},
                {"name": "dstPoolId", "type": "uint256", "indexed": False},
                {"name": "from", "type": "address", "indexed": False},
                {"name": "amountSD", "type": "uint256", "indexed": False},
                {"name": "eqReward", "type": "uint256", "indexed": False},
                {"name": "eqFee", "type": "uint256", "indexed": False},
                {"name": "protocolFee", "type": "uint256", "indexed": False},
                {"name": "lpFee", "type": "uint256", "indexed": False},
            ],
            "tags": ["stargate", "swap", "bridging"],
        }
    ],
    "0xef519b7eb82aaf6ac376a6df2d793843ebfd593de5f1a0601d3cc6ab49ebb395": [
        {
            "name": "Deposit",
            "inputs": [
                {"name": "to", "type": "address", "indexed": True},
                {"name": "asset", "type": "address", "indexed": True},
                {"name": "amount", "type": "uint256", "indexed": False},
                {"name": "memo", "type": "string", "indexed": False},
            ],
            "tags": ["thorchain", "deposit", "bridging"],
        }
    ],
    "0xa9cd03aa3c1b4515114539cd53d22085129d495cb9e9f9af77864526240f1bf7": [
        {
            "name": "TransferOut",
            "inputs": [
                {"name": "vault", "type": "address", "indexed": True},
                {"name": "to", "type": "address", "indexed": True},
                {"name": "asset", "type": "address", "indexed": False},
                {"name": "amount", "type": "uint256", "indexed": False},
                {"name": "memo", "type": "string", "indexed": False},
            ],
            "tags": ["thorchain", "withdrawal", "bridging"],
        }
    ],
    "0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129": [
        {
            "name": "Fill",
            "inputs": [
                {"name": "makerAddress", "type": "address", "indexed": True},
                {"name": "feeRecipientAddress", "type": "address", "indexed": True},
                {"name": "takerAddress", "type": "address", "indexed": False},
                {"name": "senderAddress", "type": "address", "indexed": False},
                {"name": "makerAssetFilledAmount", "type": "uint256", "indexed": False},
                {"name": "takerAssetFilledAmount", "type": "uint256", "indexed": False},
                {"name": "makerFeePaid", "type": "uint256", "indexed": False},
                {"name": "takerFeePaid", "type": "uint256", "indexed": False},
                {"name": "orderHash", "type": "bytes32", "indexed": True},
                {"name": "makerAssetData", "type": "bytes", "indexed": False},
                {"name": "takerAssetData", "type": "bytes", "indexed": False},
            ],
            "tags": ["0x", "exchange", "fill", "swap"],
        }
    ],
    "0xaeef64b7687b985665b6620c7fa271b6f051a3fbe2bfc366fb9c964602eb6d26": [  # you find this at the receiving side of the bridge
        {
            "name": "BurnCompleted",
            "inputs": [
                {"name": "id", "type": "bytes32", "indexed": True},
                {"name": "crossChainID", "type": "bytes32", "indexed": True},
                {"name": "to", "type": "address", "indexed": True},
                {"name": "amount", "type": "uint256", "indexed": False},
                {"name": "bridgingFee", "type": "uint256", "indexed": False},
                {"name": "token", "type": "address", "indexed": False},
            ],
            "tags": ["symbiosis", "bridging", "receive"],
        }
    ],
    "0x532dbb6d061eee97ab4370060f60ede10b3dc361cc1214c07ae5e34dd86e6aaf": [
        {
            "name": "OracleRequest",
            "inputs": [
                {"name": "bridge", "type": "address", "indexed": False},
                {"name": "callData", "type": "bytes", "indexed": False},
                {"name": "receiveSide", "type": "address", "indexed": False},
                {"name": "oppositeBridge", "type": "address", "indexed": False},
                {"name": "chainId", "type": "uint256", "indexed": False},
            ],
            "tags": ["symbiosis", "bridging", "send"],
        }
    ],
}


def get_filtered_log_signatures(filter: str, log_signatures_local=log_signatures):
    pattern = re.compile(filter)

    result = {}
    for k, v in log_signatures_local.items():
        vnew = [x for x in v if any(pattern.match(y) for y in x.get("tags", []))]

        if len(vnew) > 0:
            result[k] = v
    return result


def is_supported_log(log, log_signatures_local=log_signatures) -> bool:
    return len(log["topics"]) > 0 and log["topics"][0] in log_signatures_local


def convert_log_generic(db_log) -> dict:
    db_log = dict_to_dataobject(db_log)
    data_str = db_log.data.hex()  # ty: ignore[unresolved-attribute]
    return {
        "topics": [f"0x{topic.hex()}" for topic in (db_log.topics or [])],  # ty: ignore[unresolved-attribute]
        "data": f"0x{data_str}",
        "address": f"0x{db_log.address.hex()}",  # ty: ignore[unresolved-attribute]
    }


def decoded_log_to_str(decoded_log) -> str:
    name = decoded_log["name"]
    addr = decoded_log["address"].lower()
    params = ",".join([f"{x['name']}={x['value']}" for x in decoded_log["data"]])
    return f"{addr}|{name}({params})".replace("\n", "")


def decode_logs_db(db_logs, log_signatures_local=log_signatures):
    return decode_logs_dict(db_logs, log_signatures_local=log_signatures_local)


def decode_logs_dict(
    db_logs: List[Dict[str, Any]], log_signatures_local=log_signatures
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    return [
        x
        for x in [
            (
                decode_log(
                    convert_log_generic(log), log_signatures_local=log_signatures_local
                ),
                log,
            )
            for log in db_logs
        ]
        if x[0] is not None
    ]


def decode_log(log, log_signatures_local=log_signatures):
    if is_supported_log(log, log_signatures_local=log_signatures_local):
        logdef = log_signatures_local[log["topics"][0]]
        for i in range(0, len(logdef)):
            try:
                versioned_dict = VersionedDict(log_signatures_local, i)
                decoded_log = eth_event.decode_log(log, versioned_dict)
                ld = versioned_dict
                decoded_log["log_def"] = ld[log["topics"][0]]
                decoded_log["parameters"] = {
                    d["name"]: d["value"] for d in decoded_log.get("data", [])
                }
                return decoded_log
            except eth_event.EventError as e:
                if i == len(logdef) - 1:
                    logger.info(f"Failed to decode supported log type. {e}. {log}")
    else:
        logger.debug("Can't decode log, not supported yet")
    return None
