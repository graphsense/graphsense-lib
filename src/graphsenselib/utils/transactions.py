from enum import Enum
from typing import Optional

from pydantic import BaseModel

SUBTX_IDENT_SEPERATOR_CHAR = "_"


class SubTransactionType(Enum):
    InternalTx = "internalTx"
    ExternalTx = "externalTx"
    ERC20 = "erc20"
    GenericLog = "genericLog"


SUBTX_TYPE_TO_PREFIX = {
    SubTransactionType.InternalTx: "I",
    SubTransactionType.ExternalTx: "",
    SubTransactionType.ERC20: "T",
    SubTransactionType.GenericLog: "L",
}

REAL_SUBTXS = [
    SubTransactionType.InternalTx,
    SubTransactionType.ERC20,
    SubTransactionType.GenericLog,
]


class SubTransactionIdentifier(BaseModel):
    tx_hash: str
    tx_type: SubTransactionType
    sub_index: Optional[int]

    @classmethod
    def from_string(cls, stringRep: str):
        if f"{SUBTX_IDENT_SEPERATOR_CHAR}" in stringRep:
            for stt in REAL_SUBTXS:
                res = try_decode_subTx(stringRep, stt)
                if res is not None:
                    return res

            raise ValueError(f"Unknown transaction type {stringRep}")

        else:
            return cls(
                tx_hash=stringRep, tx_type=SubTransactionType.ExternalTx, sub_index=None
            )

    def to_string(self, type_overwrite: Optional[SubTransactionType] = None) -> str:
        h = self.tx_hash
        endType = type_overwrite or self.tx_type
        prefix = SUBTX_TYPE_TO_PREFIX[endType]

        if endType in REAL_SUBTXS:
            return f"{h}{SUBTX_IDENT_SEPERATOR_CHAR}{prefix}{self.sub_index}"
        elif endType == SubTransactionType.ExternalTx:
            return h
        else:
            raise Exception(f"Unknown transaction type {endType}")


def try_decode_subTx(
    stringRep: str, stt: SubTransactionType
) -> Optional[SubTransactionIdentifier]:
    prefix = SUBTX_TYPE_TO_PREFIX[stt]
    h, _, postfix = stringRep.partition(SUBTX_IDENT_SEPERATOR_CHAR)
    # The marker letter is accepted case-insensitively ("_t1" == "_T1"); the
    # case carries no information and hand-typed identifiers are often
    # lowercased. to_string keeps the canonical uppercase form.
    if postfix[:1].upper() != prefix:
        return None
    tindexS = postfix[1:]
    if not tindexS.isdigit():
        raise ValueError(f"{stt} index: {tindexS} is not an integer.")
    return SubTransactionIdentifier(tx_hash=h, tx_type=stt, sub_index=int(tindexS))
