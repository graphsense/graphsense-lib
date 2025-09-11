from typing import Any, Dict, Tuple
from eth_account.typed_transactions.typed_transaction import TypedTransaction
from eth_account._utils.signing import hash_of_signed_transaction, to_standard_v
from eth_keys.datatypes import Signature, PublicKey
from eth_account._utils.legacy_transactions import (
    Transaction,
    vrs_from,
    serializable_unsigned_transaction_from_dict,
)
# import rlp
# from eth_hash.auto import keccak


def _parseBytes(x):
    if x is None:
        return None
    if isinstance(x, bytes):
        return x
    elif isinstance(x, str):
        if x.startswith("0x"):
            x = x[2:]
        return bytes.fromhex(x)
    else:
        raise TypeError(f"Unsupported type {type(x)}")


def _parseInt(x):
    if isinstance(x, int):
        return x
    elif isinstance(x, str):
        if x.startswith("0x"):
            return int(x, 16)
        else:
            return int(x)
    else:
        raise TypeError(f"Unsupported type {type(x)}")


def _parseAccessList(x):
    if isinstance(x, list):
        return [
            {
                "address": item["address"],
                "storageKeys": [k for k in item["storageKeys"]],
            }
            for item in x
        ]
    else:
        raise TypeError(f"Unsupported type {type(x)}")


def blobListParser(x):
    if isinstance(x, list):
        return [_parseBytes(item) for item in x]
    else:
        raise TypeError(f"Unsupported type {type(x)}")


type_to_signature_fields_eth = {
    0: {
        #    'chainId',  # not included in legacy transactions, added on demand in code later
        "gas": (_parseInt, None),
        "gasPrice": (_parseInt, None),
        "nonce": (_parseInt, None),
        "to": (_parseBytes, None),
        "input": (_parseBytes, "data"),
        "value": (_parseInt, None),
        "type": (_parseInt, None),
    },
    1: {
        "to": (_parseBytes, None),
        "input": (_parseBytes, "data"),
        "nonce": (_parseInt, None),
        "value": (_parseInt, None),
        "gas": (_parseInt, None),
        "gasPrice": (_parseInt, None),
        "chainId": (_parseInt, None),
        "type": (_parseInt, None),
        "accessList": (_parseAccessList, None),
    },
    2: {
        "to": (_parseBytes, None),
        "input": (_parseBytes, "data"),
        "nonce": (_parseInt, None),
        "value": (_parseInt, None),
        "gas": (_parseInt, None),
        "chainId": (_parseInt, None),
        "maxFeePerGas": (_parseInt, None),
        "maxPriorityFeePerGas": (_parseInt, None),
        "type": (_parseInt, None),
        "accessList": (_parseAccessList, None),
    },
    3: {
        "to": (_parseBytes, None),
        "input": (_parseBytes, "data"),
        "nonce": (_parseInt, None),
        "value": (_parseInt, None),
        "gas": (_parseInt, None),
        "chainId": (_parseInt, None),
        "maxFeePerGas": (_parseInt, None),
        "maxPriorityFeePerGas": (_parseInt, None),
        "maxFeePerBlobGas": (_parseInt, None),
        "blobVersionedHashes": (blobListParser, None),
        "type": (_parseInt, None),
        "accessList": (_parseAccessList, None),
    },
}


def eth_get_vrs_from_rpc_json(raw_rpc_json: Dict[str, Any]) -> Tuple[int, int, int]:
    v = _parseInt(raw_rpc_json["v"])
    r = _parseInt(raw_rpc_json["r"])
    s = _parseInt(raw_rpc_json["s"])
    return (to_standard_v(v), r, s)


def eth_get_signature_data_from_rpc_json(raw_rpc_json: Dict[str, Any]):
    type = _parseInt(raw_rpc_json.get("type", 0))
    v = _parseInt(raw_rpc_json["v"])

    if type not in type_to_signature_fields_eth:
        raise ValueError(f"Unsupported transaction type: {type}")

    output = {
        (new_field_name if new_field_name else field): converter(raw_rpc_json[field])
        for field, (converter, new_field_name) in type_to_signature_fields_eth[
            type
        ].items()
    }

    if type == 0 and v != 27 and v != 28:
        # EIP155 unaffected values when y parity {0,1} + 27
        # i.e., if v = 27 or v = 28
        # Tx with these values do not include a dedicated "chainId"
        # Therefore, it has to be set manually on those tx
        # https://eips.ethereum.org/EIPS/eip-155
        output["chainId"] = 1

    if type == 0:
        # remove type field for legacy transactions (expected by serializable_unsigned_transaction_from_dict)
        output.pop("type")

    return output


def eth_get_msg_hash_from_signature_data(signature_data: Dict[str, Any]) -> bytes:
    # if "accessList" in signature_data and len(signature_data["accessList"]) > 0:
    #     # non-empty access list:
    #     # The access lists are currently not handled correctly
    #     # by serializable_unsigned_transaction_from_dict so this code
    #     # created the unsigned transaction hash for these tx instead
    #     access_list = list()
    #     for j in signature_data["accessList"]:
    #         storage_keys = [bytes.fromhex(k[2:]) for k in j["storageKeys"]]
    #         access_tuple = [bytes.fromhex(j["address"][2:]), storage_keys]
    #         access_list.append(access_tuple)

    #     if signature_data["to"] is None:
    #         # This fixes an error for some tx
    #         # where the returned to addr is None
    #         # e.g., 0x9fa4387b9a9e4de91f11f1cdf1c35eef4fe4d38e4548d581183743a2f6dee23a
    #         # in block 0x200100
    #         tx_to = ""
    #     else:
    #         tx_to = signature_data["to"]

    #     if signature_data["type"] == 0x01:
    #         tx_payload = [
    #             signature_data["chainId"],
    #             signature_data["nonce"],
    #             signature_data["gasPrice"],
    #             signature_data["gas"],
    #             tx_to,
    #             signature_data["value"],
    #             signature_data["data"],
    #             access_list,
    #         ]
    #     elif signature_data["type"] == 0x02:
    #         tx_payload = [
    #             signature_data["chainId"],
    #             signature_data["nonce"],
    #             signature_data["maxPriorityFeePerGas"],
    #             signature_data["maxFeePerGas"],
    #             signature_data["gas"],
    #             tx_to,
    #             signature_data["value"],
    #             signature_data.get("data", ""),
    #             access_list,
    #         ]

    #     elif signature_data["type"] == 0x03:
    #         tx_payload = [
    #             signature_data["chainId"],
    #             signature_data["nonce"],
    #             signature_data["maxPriorityFeePerGas"],
    #             signature_data["maxFeePerGas"],
    #             signature_data["gas"],
    #             tx_to,
    #             signature_data["value"],
    #             signature_data["data"],
    #             access_list,
    #             signature_data["maxFeePerBlobGas"],
    #             signature_data["blobVersionedHashes"],
    #         ]
    #     else:
    #         raise ValueError(f"Unsupported transaction type: {signature_data['type']}")

    #     return keccak(signature_data["type"].to_bytes(1,byteorder="big") + rlp.encode(tx_payload))
    # else:
    # empty accessList:
    """
    tx_payload.extend((
        tx["chainId"],
        tx["nonce"],
        tx["gasPrice"],
        tx["gas"],
        bytes.fromhex(tx["to"][2:]),
        tx["value"],
        tx["input"],
        tx["accessList"],
        #bytes.fromhex(tx["input"].hex()[2:]),
    ))]
    """
    ut = serializable_unsigned_transaction_from_dict(signature_data)
    return ut.hash()


def eth_get_msg_hash_and_vrs_from_raw(txn_bytes) -> Tuple[bytes, Tuple[int, int, int]]:
    if len(txn_bytes) > 0 and txn_bytes[0] <= 0x7F:
        # We are dealing with a typed transaction.
        typed_transaction = TypedTransaction.from_bytes(txn_bytes)
        msg_hash = typed_transaction.hash()
        v, r, s = typed_transaction.vrs()
        txn = typed_transaction
    else:
        txn = Transaction.from_bytes(txn_bytes)
        msg_hash = hash_of_signed_transaction(txn)
        v, r, s = vrs_from(txn)

    return msg_hash, (to_standard_v(v), r, s)


def eth_recover_pubkey(sig: Tuple[int, int, int], msg_hash: bytes) -> PublicKey:
    v_standard = to_standard_v(sig[0])
    signature = Signature(vrs=(v_standard, sig[1], sig[2]))
    return signature.recover_public_key_from_msg_hash(msg_hash)
