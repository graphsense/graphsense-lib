# flake8: noqa: E501
import json
import re

from graphsenselib.utils import dict_to_dataobject, dict_with_snake_keys


def preprocess_inputs(text: str, drop=[]):
    tnl = ",\n".join([line for line in text.strip().split("\n")])
    items = json.loads(f"[{tnl}]")
    for item in items:
        keys_to_drop = [k for k in item.keys() if any([re.match(dp, k) for dp in drop])]
        for k in keys_to_drop:
            item.pop(k)
    return [dict_with_snake_keys(item) for item in items]


# These are copies for the tests in the graphsense-transform repo
# src/test/resources/test_txs.json
txs = preprocess_inputs(
    """
{"txIdGroup": 0, "txId": 1, "txHash": "ab100004ba5dd70a143ca2359693c19fc879bb86b259870eed0de573f51d34e6", "coinbase": true, "coinjoin": false, "blockId": 1, "inputs": [], "outputs": [{"address": ["138cWsiAGpW9yqfjMVCCsFcnaiSHyoWMnJ"], "value": 2500000, "txType": 3}], "timestamp": 1230948000, "totalInput": 0, "totalOutput": 2500000}
{"txIdGroup": 0, "txId": 2, "txHash": "ab200013f626405ddebf1a7b2e0af34253d09e80f9ef7f981ec1ec59d6200c1f", "coinbase": true, "coinjoin": false, "blockId": 2, "inputs": [], "outputs": [{"address": ["1HxbaBMF2jXBVnagoHvaA6rLxmaYL8gb8T"], "value": 2500000, "txType": 3}], "timestamp": 1231466400, "totalInput": 0, "totalOutput": 2500000}
{"txIdGroup": 0, "txId": 3, "txHash": "ab20101c4ca509cd1bbb31bef15b96cc5f987e6438f770e105e41d012057930a", "coinbase": false, "coinjoin": false, "blockId": 2, "inputs": [{"address": ["138cWsiAGpW9yqfjMVCCsFcnaiSHyoWMnJ"], "value": 2500000, "txType": 5}], "outputs": [{"address": ["1FDCgJ8m2xDyVmYuankk13XReVC2Zvs5cz"], "value": 1480000, "txType": 5}, {"address": ["1FAkhqm95YnV5Mi7Q5j2Wb8CkbK7Z9zpyB"], "value": 495000, "txType": 5}, {"address": ["3Kawbc5pkpQPfmaoGCBiaw5t2uK6WBoVVe"], "value": 495000, "txType": 5}], "timestamp": 1231466400, "totalInput": 2500000, "totalOutput": 2470000}
{"txIdGroup": 0, "txId": 4, "txHash": "ab300013f626405ddebf1a7b2e0af34253d09e80f9ef7f981ec1ec59d6200c1f", "coinbase": true, "coinjoin": false, "blockId": 3, "inputs": [], "outputs": [{"address": ["12751KvnaTTPMJbHbbercdTV48kL81BjX8"], "value": 2500000, "txType": 3}], "timestamp": 1231552800, "totalInput": 0, "totalOutput": 2500000}
{"txIdGroup": 0, "txId": 5, "txHash": "ab30109910f327700e0f199972eed8ea7c6b1920e965f9cb48a92973e7325046", "coinbase": false, "coinjoin": true, "blockId": 3, "inputs": [{"address": ["1FDCgJ8m2xDyVmYuankk13XReVC2Zvs5cz"], "value": 1480000, "txType": 5}, {"address": ["1HxbaBMF2jXBVnagoHvaA6rLxmaYL8gb8T"], "value": 2500000, "txType": 3}], "outputs": [{"address": ["1E5UPs8bXBq7v7D2b7BYNU19ZviX2LMgEe", "1KLWBUTPXtRB7wvTYq9TYdeo93fAKvWPKs"], "value": 150000, "txType": 6}, {"address": ["1231PgW8KbpwKkvACPhp13fcL6fM5sKGvy"], "value": 1325000, "txType": 3}, {"address": ["1Fufjpf9RM2aQsGedhSpbSCGRHrmLMJ7yY"], "value": 100000, "txType": 3}, {"address": ["1iYSYHTpr2wMShaXTTNUzMohkpuV5p5ep"], "value": 2350000, "txType": 3}], "timestamp": 1231552800, "totalInput": 3980000, "totalOutput": 3925000}
{"txIdGroup": 0, "txId": 6, "txHash": "ab3020c34bed7a86eef78534d72fd38c3c247c25d58be632201cb2027c9d2197", "coinbase": false, "coinjoin": false, "blockId": 3, "inputs": [{"address": ["1FAkhqm95YnV5Mi7Q5j2Wb8CkbK7Z9zpyB"], "value": 495000, "txType": 5}], "outputs": [{"address": ["3Fkx2TFdcHoab4xGgSjhAVh5YBPvbBWjNL"], "value": 140000, "txType": 5}, {"address": ["1FAkhqm95YnV5Mi7Q5j2Wb8CkbK7Z9zpyB"], "value": 345000, "txType": 3}], "timestamp": 1231552800, "totalInput": 495000, "totalOutput": 485000}
{"txIdGroup": 0, "txId": 7, "txHash": "ab4006c39b99683ac8f456721b270786c627ecb246700888315991877024b983", "coinbase": true, "coinjoin": false, "blockId": 4, "inputs": [], "outputs": [{"address": ["1CjPR7Z5ZSyWk6WtXvSFgkptmpoi4UM9BC"], "value": 2500000, "txType": 3}], "timestamp": 1231639200, "totalInput": 0, "totalOutput": 2500000}
{"txIdGroup": 0, "txId": 8, "txHash": "ab40158fdb719f333927a92f72727b996f1ebf565fce2cc83aea04b0f6902c6f", "coinbase": false, "coinjoin": false, "blockId": 4, "inputs": [{"address": ["1E5UPs8bXBq7v7D2b7BYNU19ZviX2LMgEe", "1KLWBUTPXtRB7wvTYq9TYdeo93fAKvWPKs"], "value": 1500000, "txType": 6}, {"address": ["3Kawbc5pkpQPfmaoGCBiaw5t2uK6WBoVVe"], "value": 495000, "txType": 5}, {"address": ["12751KvnaTTPMJbHbbercdTV48kL81BjX8"], "value": 2500000, "txType": 3}], "outputs": [{"address": ["1AGMAXWELLayCyS1vkLXEszESHEcB3LWqa"], "value": 4300000, "txType": 3}, {"address": ["3JX79i9xSSmLEDZ8WrUFsy3WqrpvPoQmPv"], "value": 149000, "txType": 5}], "timestamp": 1231639200, "totalInput": 4495000, "totalOutput": 4449000}
{"txIdGroup": 0, "txId": 9, "txHash": "ab40231bd240c232eb8a8ed1b2267fe9f76d46c892415ab74427195cce5c6cff", "coinbase": false, "coinjoin": false, "blockId": 4, "inputs": [{"address": ["1FAkhqm95YnV5Mi7Q5j2Wb8CkbK7Z9zpyB"], "value": 345000, "txType": 3}], "outputs": [{"address": ["3Fkx2TFdcHoab4xGgSjhAVh5YBPvbBWjNL"], "value": 340000, "txType": 5}], "timestamp": 1231639200, "totalInput": 345000, "totalOutput": 340000}
{"txIdGroup": 0, "txId": 10, "txHash": "ab403d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d", "coinbase": false, "coinjoin": false, "blockId": 4, "inputs": [{"address": ["1Fufjpf9RM2aQsGedhSpbSCGRHrmLMJ7yY"], "value": 100000, "txType": 3}, {"address": ["1231PgW8KbpwKkvACPhp13fcL6fM5sKGvy"], "value": 1325000, "txType": 3}], "outputs": [{"address": ["1Fm1rJe1CtGuTKxWjQ4MyU7GEwrgkHYn5k"], "value": 1400000, "txType": 3}], "timestamp": 1231639200, "totalInput": 1425000, "totalOutput": 1400000}
{"txIdGroup": 0, "txId": 11, "txHash": "ab403d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d", "coinbase": false, "coinjoin": false, "blockId": 4, "inputs": [{"address": ["ltc1qd42h5rlss8d54hpzvjpy408z2d8gpnj668wnt0"], "value": 100000, "txType": 8}, {"address": ["ltc1qq8gtfz5dvd838na8v86ehd3l98qjatpz47mayk"], "value": 1325000, "txType": 8}], "outputs": [{"address": ["ltc1qq8gtfz5dvd838na8v86ehd3l98qjatpz47mayk"], "value": 1400000, "txType": 3}], "timestamp": 1231639200, "totalInput": 1425000, "totalOutput": 1400000}
""",
    drop=[
        r".*Group",
    ],
)

exchange_rates = preprocess_inputs(
    """
{"date": "2009-01-03", "fiatValues": {"EUR": 150, "USD": 149.55}, "blockId": 1}
{"date": "2009-01-09", "fiatValues": {"EUR": 300, "USD": 302.67}, "blockId": 2}
{"date": "2009-01-10", "fiatValues": {"EUR": 600, "USD": 598.23}, "blockId": 3}
{"date": "2009-01-11", "fiatValues": {"EUR": 500, "USD": 498.18}, "blockId": 4}
{"date": "2015-05-22", "fiatValues": {"EUR": 500, "USD": 498.18}, "blockId": 357512}
"""
)


# src/test/resources/reference/address_txs.json
address_txs = preprocess_inputs(
    """
{"value": 2500000, "blockId": 1, "txId": 1, "addressIdGroup": 0, "addressId": 0, "isOutgoing": false}
{"value": -2500000, "blockId": 2, "txId": 3, "addressIdGroup": 0, "addressId": 0, "isOutgoing": true}
{"value": 2500000, "blockId": 2, "txId": 2, "addressIdGroup": 0, "addressId": 1, "isOutgoing": false}
{"value": -2500000, "blockId": 3, "txId": 5, "addressIdGroup": 0, "addressId": 1, "isOutgoing": true}
{"value": 1480000, "blockId": 2, "txId": 3, "addressIdGroup": 1, "addressId": 2, "isOutgoing": false}
{"value": -1480000, "blockId": 3, "txId": 5, "addressIdGroup": 1, "addressId": 2, "isOutgoing": true}
{"value": 495000, "blockId": 2, "txId": 3, "addressIdGroup": 1, "addressId": 3, "isOutgoing": false}
{"value": -150000, "blockId": 3, "txId": 6, "addressIdGroup": 1, "addressId": 3, "isOutgoing": true}
{"value": -345000, "blockId": 4, "txId": 9, "addressIdGroup": 1, "addressId": 3, "isOutgoing": true}
{"value": 495000, "blockId": 2, "txId": 3, "addressIdGroup": 2, "addressId": 4, "isOutgoing": false}
{"value": -495000, "blockId": 4, "txId": 8, "addressIdGroup": 2, "addressId": 4, "isOutgoing": true}
{"value": 2500000, "blockId": 3, "txId": 4, "addressIdGroup": 2, "addressId": 5, "isOutgoing": false}
{"value": -2500000, "blockId": 4, "txId": 8, "addressIdGroup": 2, "addressId": 5, "isOutgoing": true}
{"value": 1325000, "blockId": 3, "txId": 5, "addressIdGroup": 3, "addressId": 6, "isOutgoing": false}
{"value": -1325000, "blockId": 4, "txId": 10, "addressIdGroup": 3, "addressId": 6, "isOutgoing": true}
{"value": 100000, "blockId": 3, "txId": 5, "addressIdGroup": 3, "addressId": 7, "isOutgoing": false}
{"value": -100000, "blockId": 4, "txId": 10, "addressIdGroup": 3, "addressId": 7, "isOutgoing": true}
{"value": 2350000, "blockId": 3, "txId": 5, "addressIdGroup": 4, "addressId": 8, "isOutgoing": false}
{"value": 140000, "blockId": 3, "txId": 6, "addressIdGroup": 4, "addressId": 9, "isOutgoing": false}
{"value": 340000, "blockId": 4, "txId": 9, "addressIdGroup": 4, "addressId": 9, "isOutgoing": false}
{"value": 2500000, "blockId": 4, "txId": 7, "addressIdGroup": 5, "addressId": 10, "isOutgoing": false}
{"value": 4300000, "blockId": 4, "txId": 8, "addressIdGroup": 5, "addressId": 11, "isOutgoing": false}
{"value": 149000, "blockId": 4, "txId": 8, "addressIdGroup": 6, "addressId": 12, "isOutgoing": false}
{"value": 1400000, "blockId": 4, "txId": 10, "addressIdGroup": 6, "addressId": 13, "isOutgoing": false}
{"value": -100000, "blockId": 4, "txId": 11, "addressIdGroup": 6, "addressId": 14, "isOutgoing": true}
{"value": 75000, "blockId": 4, "txId": 11, "addressIdGroup": 6, "addressId": 15, "isOutgoing": false}
""",
    drop=[r".*Group", "blockId"],
)

# src/test/resources/reference/address_ids.json
address_ids = preprocess_inputs(
    """
{"addressId": 0, "address": "138cWsiAGpW9yqfjMVCCsFcnaiSHyoWMnJ"}
{"addressId": 1, "address": "1HxbaBMF2jXBVnagoHvaA6rLxmaYL8gb8T"}
{"addressId": 2, "address": "1FDCgJ8m2xDyVmYuankk13XReVC2Zvs5cz"}
{"addressId": 3, "address": "1FAkhqm95YnV5Mi7Q5j2Wb8CkbK7Z9zpyB"}
{"addressId": 4, "address": "3Kawbc5pkpQPfmaoGCBiaw5t2uK6WBoVVe"}
{"addressId": 5, "address": "12751KvnaTTPMJbHbbercdTV48kL81BjX8"}
{"addressId": 6, "address": "1231PgW8KbpwKkvACPhp13fcL6fM5sKGvy"}
{"addressId": 7, "address": "1Fufjpf9RM2aQsGedhSpbSCGRHrmLMJ7yY"}
{"addressId": 8, "address": "1iYSYHTpr2wMShaXTTNUzMohkpuV5p5ep"}
{"addressId": 9, "address": "3Fkx2TFdcHoab4xGgSjhAVh5YBPvbBWjNL"}
{"addressId": 10, "address": "1CjPR7Z5ZSyWk6WtXvSFgkptmpoi4UM9BC"}
{"addressId": 11, "address": "1AGMAXWELLayCyS1vkLXEszESHEcB3LWqa"}
{"addressId": 12, "address": "3JX79i9xSSmLEDZ8WrUFsy3WqrpvPoQmPv"}
{"addressId": 13, "address": "1Fm1rJe1CtGuTKxWjQ4MyU7GEwrgkHYn5k"}
{"addressId": 14, "address": "ltc1qd42h5rlss8d54hpzvjpy408z2d8gpnj668wnt0"}
{"addressId": 15, "address": "ltc1qq8gtfz5dvd838na8v86ehd3l98qjatpz47mayk"}
"""
)

# src/test/resources/reference/address_relations.json
address_relations = preprocess_inputs(
    """
{"srcAddressIdGroup": 0, "srcAddressId": 0, "dstAddressIdGroup": 1, "dstAddressId": 2, "noTransactions": 1, "estimatedValue": {"value": 1480000, "fiatValues": [4.44, 4.48]}, "txList": [3]}
{"srcAddressIdGroup": 0, "srcAddressId": 0, "dstAddressIdGroup": 1, "dstAddressId": 3, "noTransactions": 1, "estimatedValue": {"value": 495000, "fiatValues": [1.49, 1.5]}, "txList": [3]}
{"srcAddressIdGroup": 0, "srcAddressId": 0, "dstAddressIdGroup": 2, "dstAddressId": 4, "noTransactions": 1, "estimatedValue": {"value": 495000, "fiatValues": [1.49, 1.5]}, "txList": [3]}
{"srcAddressIdGroup": 0, "srcAddressId": 1, "dstAddressIdGroup": 3, "dstAddressId": 6, "noTransactions": 1, "estimatedValue": {"value": 832286, "fiatValues": [4.99, 4.98]}, "txList": [5]}
{"srcAddressIdGroup": 1, "srcAddressId": 2, "dstAddressIdGroup": 3, "dstAddressId": 6, "noTransactions": 1, "estimatedValue": {"value": 492714, "fiatValues": [2.96, 2.95]}, "txList": [5]}
{"srcAddressIdGroup": 0, "srcAddressId": 1, "dstAddressIdGroup": 3, "dstAddressId": 7, "noTransactions": 1, "estimatedValue": {"value": 62814, "fiatValues": [0.38, 0.38]}, "txList": [5]}
{"srcAddressIdGroup": 1, "srcAddressId": 2, "dstAddressIdGroup": 3, "dstAddressId": 7, "noTransactions": 1, "estimatedValue": {"value": 37186, "fiatValues": [0.22, 0.22]}, "txList": [5]}
{"srcAddressIdGroup": 0, "srcAddressId": 1, "dstAddressIdGroup": 4, "dstAddressId": 8, "noTransactions": 1, "estimatedValue": {"value": 1476131, "fiatValues": [8.86, 8.83]}, "txList": [5]}
{"srcAddressIdGroup": 1, "srcAddressId": 2, "dstAddressIdGroup": 4, "dstAddressId": 8, "noTransactions": 1, "estimatedValue": {"value": 873869, "fiatValues": [5.24, 5.23]}, "txList": [5]}
{"srcAddressIdGroup": 1, "srcAddressId": 3, "dstAddressIdGroup": 4, "dstAddressId": 9, "noTransactions": 2, "estimatedValue": {"value": 480000, "fiatValues": [2.54, 2.53]}, "txList": [9, 6]}
{"srcAddressIdGroup": 2, "srcAddressId": 4, "dstAddressIdGroup": 5, "dstAddressId": 11, "noTransactions": 1, "estimatedValue": {"value": 473526, "fiatValues": [2.37, 2.36]}, "txList": [8]}
{"srcAddressIdGroup": 2, "srcAddressId": 5, "dstAddressIdGroup": 5, "dstAddressId": 11, "noTransactions": 1, "estimatedValue": {"value": 2391546, "fiatValues": [11.96, 11.91]}, "txList": [8]}
{"srcAddressIdGroup": 2, "srcAddressId": 4, "dstAddressIdGroup": 6, "dstAddressId": 12, "noTransactions": 1, "estimatedValue": {"value": 16408, "fiatValues": [0.08, 0.08]}, "txList": [8]}
{"srcAddressIdGroup": 2, "srcAddressId": 5, "dstAddressIdGroup": 6, "dstAddressId": 12, "noTransactions": 1, "estimatedValue": {"value": 82870, "fiatValues": [0.41, 0.41]}, "txList": [8]}
{"srcAddressIdGroup": 3, "srcAddressId": 6, "dstAddressIdGroup": 6, "dstAddressId": 13, "noTransactions": 1, "estimatedValue": {"value": 1301754, "fiatValues": [6.51, 6.49]}, "txList": [10]}
{"srcAddressIdGroup": 3, "srcAddressId": 7, "dstAddressIdGroup": 6, "dstAddressId": 13, "noTransactions": 1, "estimatedValue": {"value": 98246, "fiatValues": [0.49, 0.49]}, "txList": [10]}
{"srcAddressIdGroup": 3, "srcAddressId": 14, "dstAddressIdGroup": 6, "dstAddressId": 15, "noTransactions": 1, "estimatedValue": {"value": 75000, "fiatValues": [0.38, 0.37]}, "txList": [11]}
""",
    drop=[r".*Group"],
)


def resolve_address_id(item, addr_id_lookup):
    prefix = "address_id"
    candidates = [x for x in item.keys() if x.endswith("address_id")]
    for c in candidates:
        item[c.replace(prefix, "address")] = addr_id_lookup[item.pop(c)]
    return item.copy()


def get_address_id_lookup():
    return {x["address_id"]: x["address"] for x in address_ids}


def rename_address_to_identifier(d):
    d["identifier"] = d.pop("address")
    return d


def get_txs():
    return [dict_to_dataobject(tx) for tx in txs]


def get_flow_test_tx():
    x = preprocess_inputs(
        """
            {"tx_id_group": 2777, "tx_id": 69431178, "block_id": 357512, "coinbase": false, "coinjoin": false, "inputs": [{"address": ["19doKeV52qcoR881Wk7AxZdjXXA5sSrPkf"], "value": 579551, "address_type": 3}, {"address": ["1KhgLiQCnaobb5Q5Ly9PKjagSc7hx48hhY"], "value": 17245552064, "address_type": 3}], "outputs": [{"address": ["17Buk9zw9TjzxFhrWpMEvLBbN6iiNMkwmd"], "value": 7330000, "address_type": 3}, {"address": ["1KhgLiQCnaobb5Q5Ly9PKjagSc7hx48hhY"], "value": 17238791615, "address_type": 3}], "timestamp": 1432266361, "total_input": 17246131615, "total_output": 17246121615, "tx_hash": "0xd88be10da34a270ff3c6362a9022eeafec30675a65cccb4a8f95d1caad9efac7"}
        """
    )
    return dict_to_dataobject(x)


def get_atxs():
    id_to_adr = get_address_id_lookup()
    return [
        dict_to_dataobject(
            rename_address_to_identifier(resolve_address_id(adr_tx, id_to_adr))
        )
        for adr_tx in address_txs
    ]


def get_exchange_rates_per_block():
    return {x["block_id"]: x["fiat_values"].values() for x in exchange_rates}


def get_arel():
    id_to_adr = get_address_id_lookup()
    return [
        dict_to_dataobject(resolve_address_id(adr_rel, id_to_adr))
        for adr_rel in address_relations
    ]
