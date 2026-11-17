from enum import Enum
from typing import Optional
from pydantic import BaseModel


class BridgeStrategy(str, Enum):
    """Enum for different bridge detection strategies."""

    THORCHAIN_SEND = "thorchain_send"
    THORCHAIN_RECEIVE = "thorchain_receive"
    SYMBIOSIS = "symbiosis"
    UNKNOWN = "unknown"


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
    # The protocol whose contracts prove the bridge (lowercase, e.g. "cctp"),
    # the app on top of it where a verified table names one ("stargate",
    # "usdt0"), and the protocol's own id of the transfer where the reader
    # decodes one: they become the row's protocol, protocol_app and
    # transfer_id (web/models/general.py ExternalConversion).
    protocol: Optional[str] = None
    protocolApp: Optional[str] = None
    transferId: Optional[str] = None

    def to_dict(self) -> dict:
        return self.model_dump()
