from enum import Enum
from typing import Dict, Optional
from pydantic import BaseModel


class BridgeStrategy(str, Enum):
    """Enum for different bridge detection strategies."""

    WORMHOLE_AUTOMATIC_RELAY = "wormhole"
    STARGATE = "stargate"
    THORCHAIN_SEND = "thorchain_send"
    THORCHAIN_RECEIVE = "thorchain_receive"
    SYMBIOSIS = "symbiosis"
    UNKNOWN = "unknown"


class StargateBridgeStrategy(str, Enum):
    """Enum for different stargate bridge detection strategies."""

    ETH = "eth"
    TOKEN = "token"


router_to_strategy: Dict[str, StargateBridgeStrategy] = {
    "0x8731d54E9D02c286767d56ac03e8037C07e01e98": StargateBridgeStrategy.TOKEN,
    "0x150f94b44927f078737562f0fcf3c95c01cc2376": StargateBridgeStrategy.ETH,
}


class BridgeSendTransfer(BaseModel):
    fromAddress: str
    fromAsset: str
    fromAmount: int
    fromNetwork: str
    fromPayment: str


class BridgeReceiveTransfer(BaseModel):
    toAddress: str
    toAsset: str
    toAmount: int
    toNetwork: str
    toPayment: str


class BridgeSendReference(BaseModel):
    fromTxHash: str


class BridgeReceiveReference(BaseModel):
    toAddress: str
    toNetwork: str
    fromTxHash: str
    fromTimestamp: Optional[int] = None  # Unix timestamp of the source transaction
    targetAssetCode: Optional[str] = (
        None  # THORChain asset code (e.g., "ETH.ETH" or "ETH.USDT-0x...")
    )


class Bridge(BridgeSendTransfer, BridgeReceiveTransfer):
    # todo could add bridging service name
    def to_dict(self) -> dict:
        return self.model_dump()
