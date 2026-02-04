import copy
import itertools

from graphsenselib.utils.rest_utils import omit
from graphsenselib.utils.tron import evm_to_tron_address_string

import graphsenselib.web.test.tags_service as ts
from graphsenselib.web.models import (
    Address,
    AddressTag,
    AddressTxUtxo,
    AddressTxs,
    Entity,
    LabeledItemRef,
    LinkUtxo,
    Links,
    NeighborAddress,
    NeighborAddresses,
    TxAccount,
    TxSummary,
)
from graphsenselib.web.service.rates_service import list_rates
from graphsenselib.web.test.txs_service import tx1_eth, tx2_eth, tx4_eth, tx22_eth
from graphsenselib.web.util.values_legacy import convert_value, make_values

address = Address(
    currency="btc",
    first_tx=TxSummary(
        tx_hash="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
        height=1,
        timestamp=1361497172,
    ),
    total_spent=make_values(usd=2541183.0, value=40296873552, eur=2118309.0),
    out_degree=284,
    no_incoming_txs=3981,
    no_outgoing_txs=267,
    total_received=make_values(usd=2543214.5, value=40412296129, eur=2130676.5),
    last_tx=TxSummary(
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
        timestamp=1361497172,
    ),
    address="addressA",
    entity=17642138,
    in_degree=5013,
    balance=make_values(eur=1.15, usd=2.31, value=115422577),
    status="clean",
)

addressWithTags = Address(**address.to_dict())
addressWithTags.tags = [ts.tag1, ts.tag7, ts.tag2, ts.tag3]

address2 = Address(
    out_degree=1,
    currency="btc",
    no_incoming_txs=1,
    total_spent=make_values(value=1260000, usd=103.8, eur=88.46),
    first_tx=TxSummary(
        timestamp=1361497172,
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
    ),
    total_received=make_values(eur=70.96, usd=82.79, value=1260000),
    in_degree=1,
    last_tx=TxSummary(
        tx_hash="6e7456a7a0e4cc2c4ade617e4e950ece015c00add338be345ce2b544e5a86322",
        timestamp=1510347493,
        height=2,
    ),
    address="bc1xyz123456789",
    entity=325790641,
    no_outgoing_txs=1,
    balance=make_values(eur=0.0, usd=0.0, value=0),
    status="clean",
)

addressWithoutTags = Address(**address2.to_dict())
addressWithoutTags.tags = []

address3 = Address(
    first_tx=TxSummary(
        timestamp=1361497172,
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
    ),
    out_degree=1,
    total_received=make_values(usd=0.45, eur=0.39, value=6896),
    currency="btc",
    address="addressJ",
    entity=442606576,
    no_incoming_txs=1,
    in_degree=1,
    no_outgoing_txs=1,
    total_spent=make_values(value=6896, usd=0.45, eur=0.39),
    last_tx=TxSummary(
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        timestamp=1361497172,
        height=1,
    ),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    status="clean",
)

addressE = Address(
    address="addressE",
    currency="btc",
    entity=17642138,
    last_tx=TxSummary(
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
        timestamp=1361497172,
    ),
    no_outgoing_txs=3,
    balance=make_values(value=0, eur=0.0, usd=0.0),
    out_degree=7,
    first_tx=TxSummary(
        timestamp=1361497172,
        height=1,
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
    ),
    total_received=make_values(value=87789282, eur=114.86, usd=142.18),
    total_spent=make_values(value=87789282, eur=114.86, usd=142.18),
    no_incoming_txs=3,
    in_degree=3,
    actors=[LabeledItemRef(id="actorY", label="Actor Y")],
    status="clean",
)

addressF = Address(
    address="addressF",
    currency="btc",
    entity=10164852,
    last_tx=TxSummary(
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
        timestamp=1361497172,
    ),
    first_tx=TxSummary(
        tx_hash="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
        height=1,
        timestamp=1361497172,
    ),
    total_spent=make_values(usd=2541183.0, value=40296873552, eur=2118309.0),
    out_degree=284,
    no_incoming_txs=3981,
    no_outgoing_txs=267,
    total_received=make_values(usd=2543214.5, value=40412296129, eur=2130676.5),
    in_degree=5013,
    balance=make_values(eur=1.15, usd=2.31, value=115422577),
    status="clean",
)

addressB = Address(
    address="addressB",
    currency="btc",
    entity=67065,
    first_tx=TxSummary(
        tx_hash="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
        height=1,
        timestamp=1361497172,
    ),
    last_tx=TxSummary(
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
        timestamp=1361497172,
    ),
    total_spent=make_values(usd=2541183.0, value=40296873552, eur=2118309.0),
    out_degree=284,
    no_incoming_txs=3981,
    no_outgoing_txs=267,
    total_received=make_values(usd=2543214.5, value=40412296129, eur=2130676.5),
    in_degree=5013,
    balance=make_values(eur=1.15, usd=2.31, value=115422577),
    status="clean",
)

addressD = Address(
    address="addressD",
    currency="btc",
    entity=17642138,
    first_tx=TxSummary(
        tx_hash="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
        height=1,
        timestamp=1361497172,
    ),
    last_tx=TxSummary(
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
        timestamp=1361497172,
    ),
    total_spent=make_values(usd=2541183.0, value=40296873552, eur=2118309.0),
    out_degree=284,
    no_incoming_txs=3981,
    no_outgoing_txs=267,
    total_received=make_values(usd=2543214.5, value=40412296129, eur=2130676.5),
    in_degree=5013,
    balance=make_values(eur=1.15, usd=2.31, value=115422577),
    status="clean",
)

addressWithTotalSpent0 = Address(
    first_tx=TxSummary(
        tx_hash="04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd",
        height=1,
        timestamp=1361497172,
    ),
    currency="btc",
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    out_degree=284,
    no_incoming_txs=3981,
    no_outgoing_txs=267,
    total_received=make_values(usd=0.11, value=18099, eur=0.1),
    last_tx=TxSummary(
        tx_hash="bd01b57a50bdee0fb34ce77f5c62a664cea5b94b304d438a8225850f05b45ae5",
        height=1,
        timestamp=1361497172,
    ),
    address="addressC",
    entity=17642139,
    in_degree=5013,
    balance=make_values(eur=0.0, usd=0.0, value=18099),
    status="clean",
)

addressWithTagsOutNeighbors = NeighborAddresses(
    next_page=None,
    neighbors=[
        NeighborAddress(
            labels=["labelX", "labelY"],
            no_txs=10,
            value=make_values(value=27789282, usd=87.24, eur=72.08),
            address=addressE,
        ),
        NeighborAddress(
            labels=[],
            no_txs=1,
            value=make_values(value=27789282, usd=87.24, eur=72.08),
            address=addressF,
        ),
    ],
)

addressWithTagsInNeighbors = NeighborAddresses(
    next_page=None,
    neighbors=[
        NeighborAddress(
            labels=["coinbase"],
            no_txs=1,
            value=make_values(value=1091, usd=0.01, eur=0.0),
            address=addressB,
        ),
        NeighborAddress(
            labels=[],
            no_txs=1,
            value=make_values(value=50000000, usd=404.02, eur=295.7),
            address=addressD,
        ),
    ],
)

entityWithTags = Entity(
    currency="btc",
    no_outgoing_txs=280,
    last_tx=TxSummary(height=1, tx_hash="5678", timestamp=1434554207),
    total_spent=make_values(eur=2291256.5, value=138942266867, usd=2762256.25),
    in_degree=4358,
    no_addresses=110,
    no_address_tags=4,
    total_received=make_values(usd=2583655.0, eur=2162085.5, value=139057689444),
    no_incoming_txs=4859,
    entity=17642138,
    root_address="addressA",
    out_degree=176,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(value=115422577, usd=2.31, eur=1.15),
    best_address_tag=ts.tag1,
)


entity2 = Entity(
    currency="btc",
    no_address_tags=2,
    no_outgoing_txs=280,
    last_tx=TxSummary(height=1, tx_hash="5678", timestamp=1434554207),
    total_spent=make_values(eur=2291256.5, value=138942266867, usd=2762256.25),
    in_degree=123,
    no_addresses=110,
    total_received=make_values(usd=2583655.0, eur=2162085.5, value=139057689444),
    no_incoming_txs=234,
    entity=2818641,
    root_address="address2818641",
    out_degree=176,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(value=115422577, usd=2.31, eur=1.15),
    best_address_tag=AddressTag(**ts.tag8.to_dict(), inherited_from="cluster"),
)

entity3 = Entity(**entity2.to_dict())
entity3.entity = 8361735
entity3.best_address_tag = None
entity3.no_address_tags = 0
entity3.root_address = "address8361735"

entity4 = Entity(
    currency="btc",
    no_address_tags=0,
    no_outgoing_txs=280,
    last_tx=TxSummary(height=1, tx_hash="5678", timestamp=1434554207),
    total_spent=make_values(usd=100.0, value=5, eur=50.0),
    in_degree=123,
    no_addresses=110,
    total_received=make_values(usd=200.0, value=10, eur=100.0),
    no_incoming_txs=234,
    entity=67065,
    root_address="addressB",
    out_degree=176,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=5),
    best_address_tag=None,
)

entity5 = Entity(
    currency="btc",
    no_address_tags=0,
    no_outgoing_txs=1,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=40402.43, value=5000000000, eur=29569.65),
    in_degree=0,
    no_addresses=1,
    total_received=make_values(usd=13.41, value=5000000000, eur=9.87),
    no_incoming_txs=1,
    entity=144534,
    root_address="addressD",
    out_degree=2,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=None,
)

eth_address = Address(
    currency="eth",
    first_tx=TxSummary(tx_hash="af6e0000", height=1, timestamp=15),
    total_spent=make_values(eur=30.33, value=123000000000000000000, usd=40.44),
    out_degree=6,
    no_incoming_txs=5,
    no_outgoing_txs=10,
    total_received=make_values(eur=10.11, value=234000000000000000000, usd=20.22),
    last_tx=TxSummary(tx_hash="af6e0003", height=2, timestamp=16),
    address="0xabcdef",
    entity=107925000,
    is_contract=True,
    in_degree=5,
    balance=make_values(eur=111.0, usd=222.0, value=111000000000000000000),
    status="clean",
)

eth_addressWithTags = Address(**eth_address.to_dict())
eth_addressWithTags.tags = [ts.eth_tag1, ts.eth_tag2]

eth_address2 = Address(
    currency="eth",
    last_tx=TxSummary(tx_hash="af6e0003", height=2, timestamp=16),
    in_degree=1,
    no_incoming_txs=1,
    out_degree=2,
    total_received=make_values(value=456000000000000000000, eur=40.44, usd=50.56),
    balance=make_values(value=111000000000000000000, usd=222.0, eur=111.0),
    no_outgoing_txs=2,
    total_spent=make_values(value=345000000000000000000, eur=50.56, usd=60.67),
    first_tx=TxSummary(timestamp=15, tx_hash="af6e0000", height=1),
    address="0x123456",
    entity=107925001,
    is_contract=False,
    total_tokens_received={
        "usdt": make_values(eur=450.0, usd=500.0, value=450),
        "weth": make_values(eur=50.56, usd=60.67, value=345000000000000000000),
    },
    actors=[
        LabeledItemRef(id="actorX", label="Actor X"),
        LabeledItemRef(id="actorY", label="Actor Y"),
    ],
    status="clean",
)

eth_address3 = Address(
    currency="eth",
    last_tx=TxSummary(tx_hash="af6e0003", height=2, timestamp=16),
    in_degree=1,
    no_incoming_txs=1,
    out_degree=2,
    total_received=make_values(value=456000000000000000000, eur=40.44, usd=50.56),
    balance=make_values(value=111000000000000000000, usd=222.0, eur=111.0),
    no_outgoing_txs=2,
    total_spent=make_values(value=345000000000000000000, eur=50.0, usd=100.0),
    first_tx=TxSummary(timestamp=15, tx_hash="af6e0000", height=1),
    address="0x234567",
    entity=107925002,
    is_contract=False,
    total_tokens_spent={
        "usdt": make_values(eur=450, usd=900.0, value=450),
        "weth": make_values(eur=50.0, usd=100.0, value=345000000000000000000),
    },
    status="clean",
    token_balances={
        "usdt": make_values(value=450000000, eur=225.0, usd=450.0),
        "weth": make_values(value=345000000000000000000, eur=345.0, usd=690.0),
    },
)

eth_addressWithTagsOutNeighbors = NeighborAddresses(
    next_page=None,
    neighbors=[
        NeighborAddress(
            labels=["TagA", "TagB"],
            no_txs=4,
            value=make_values(value=10000000000000000000, usd=20.0, eur=10.0),
            address=eth_address,
        ),
        NeighborAddress(
            labels=["LabelX", "LabelY"],
            no_txs=4,
            value=make_values(value=10000000000000000000, usd=20.0, eur=10.0),
            address=eth_address2,
        ),
    ],
)

eth_address2WithTokenFlows = NeighborAddresses(
    next_page=None,
    neighbors=[
        NeighborAddress(
            labels=[],
            no_txs=2,
            value=make_values(value=0, usd=0.0, eur=0.0),
            token_values={
                "usdt": make_values(value=450, eur=450.0, usd=900.0),
                "weth": make_values(value=345000000000000000000, eur=50.0, usd=100.0),
            },
            address=eth_address3,
        )
    ],
)

eth_entityWithTags = Entity(
    currency="eth",
    no_outgoing_txs=eth_address.no_outgoing_txs,
    last_tx=eth_address.last_tx,
    total_spent=eth_address.total_spent,
    in_degree=eth_address.in_degree,
    no_addresses=1,
    no_address_tags=2,
    total_received=eth_address.total_received,
    no_incoming_txs=eth_address.no_incoming_txs,
    entity=107925000,
    root_address=eth_address.address,
    out_degree=eth_address.out_degree,
    first_tx=eth_address.first_tx,
    balance=eth_address.balance,
    best_address_tag=ts.eth_tag1,
)

eth_entityWithTokens = Entity(
    currency="eth",
    no_outgoing_txs=eth_address2.no_outgoing_txs,
    last_tx=eth_address2.last_tx,
    total_spent=eth_address2.total_spent,
    in_degree=eth_address2.in_degree,
    no_addresses=1,
    no_address_tags=2,
    total_received=eth_address2.total_received,
    no_incoming_txs=eth_address2.no_incoming_txs,
    entity=107925001,
    root_address=eth_address2.address,
    out_degree=eth_address2.out_degree,
    first_tx=eth_address2.first_tx,
    balance=eth_address2.balance,
    best_address_tag=ts.eth_tag1,
)


async def get_tag_summary_by_address(test_case):
    pass


async def get_address(test_case):
    """Test case for get_address"""
    basepath = "/{currency}/addresses/{address}"
    path = basepath + "?include_tags={include_tags}"
    result = await test_case.request(
        path, currency="btc", address=addressWithoutTags.address, include_tags=True
    )

    assert addressWithoutTags.to_dict() == result

    result = await test_case.request(
        path, currency="btc", address=addressWithTags.address, include_tags=True
    )
    awt = addressWithTags.to_dict()
    assert awt == result
    awt_public = Address(**awt)
    awt_public.tags = [ts.tag1, ts.tag3]
    result = await test_case.request(
        path,
        currency="btc",
        auth="unauthorized",
        address=addressWithTags.address,
        include_tags=True,
    )
    test_case.assertEqual(awt_public.to_dict(), result)

    result = await test_case.request(
        basepath, currency="btc", address=addressWithTotalSpent0.address
    )
    test_case.assertEqual(addressWithTotalSpent0.to_dict(), result)

    # ETH
    result = await test_case.request(
        basepath, currency="eth", address=eth_addressWithTags.address
    )
    test_case.assertEqual(eth_address.to_dict(), result)

    result, body = await test_case.requestOnly(
        "/{currency}/addresses/{address}",
        None,
        currency="eth",
        address="1Archi6M1r5b41Rvn1SY2FfJAzsrEUT7aT",
    )
    assert result.status == 400
    assert "The address provided does not look like a ETH address" in body

    # non supported currency
    result, body = await test_case.requestOnly(
        "/{currency}/addresses/{address}",
        None,
        currency="DOGE",
        address="DBgS3X3hveRppkeywm9C6HMJKZb2CG8nGV",
    )
    assert result.status == 404
    assert "Network doge not supported" in body

    result = await test_case.request(
        path,
        currency="trx",
        address=evm_to_tron_address_string("0xabcdef"),
        include_tags=False,
    )

    assert result["address"] == evm_to_tron_address_string("0xabcdef")

    result = await test_case.request(
        path,
        currency="trx",
        address=evm_to_tron_address_string("0x123456"),
        include_tags=False,
    )

    assert result["address"] == evm_to_tron_address_string("0x123456")


async def list_address_txs(test_case):
    """Test case for list_address_txs

    Get all transactions an address has been involved in
    """
    path = "/{currency}/addresses/{address}/txs"
    path_with_pagesize = path + "?pagesize={pagesize}&page={page}"
    rates = await list_rates(test_case, currency="btc", heights=[2])
    txs = [
        AddressTxUtxo(
            tx_hash="123456",
            currency="btc",
            value=convert_value("btc", 1260000, rates[2]),
            height=3,
            coinbase=False,
            timestamp=1510347493,
        ),
        AddressTxUtxo(
            tx_hash="abcdef",
            currency="btc",
            value=convert_value("btc", -1260000, rates[2]),
            height=2,
            coinbase=False,
            timestamp=1511153263,
        ),
        AddressTxUtxo(
            tx_hash="ab1880",
            currency="btc",
            value=convert_value("btc", -1, rates[2]),
            height=1,
            coinbase=False,
            timestamp=1434554207,
        ),
    ]
    address_txs = AddressTxs(next_page=None, address_txs=txs)
    result = await test_case.request(
        path_with_pagesize,
        currency="btc",
        address=address2.address,
        pagesize=2,
        page="",
    )
    test_case.assertEqual(
        address_txs.to_dict()["address_txs"][0:2], result["address_txs"]
    )
    test_case.assertNotEqual(result.get("next_page", None), None)

    result = await test_case.request(
        path_with_pagesize,
        currency="btc",
        address=address2.address,
        pagesize=2,
        page=result["next_page"],
    )

    test_case.assertEqual(
        address_txs.to_dict()["address_txs"][2:3], result["address_txs"]
    )
    test_case.assertEqual(None, result.get("next_page", None))

    path_with_order = path + "?order={order}"
    _reversed = list(reversed(address_txs.to_dict()["address_txs"]))
    result = await test_case.request(
        path_with_order, currency="btc", address=address2.address, order="asc"
    )
    test_case.assertEqual(_reversed, result["address_txs"])

    path_with_order_and_page = path_with_order + "&pagesize={pagesize}&page={page}"
    result = await test_case.request(
        path_with_order_and_page,
        currency="btc",
        address=address2.address,
        order="asc",
        pagesize=2,
        page="",
    )
    test_case.assertEqual(_reversed[0:2], result["address_txs"])
    test_case.assertNotEqual(result.get("next_page", None), None)

    result = await test_case.request(
        path_with_order_and_page,
        currency="btc",
        address=address2.address,
        order="asc",
        pagesize=2,
        page=result["next_page"],
    )

    test_case.assertEqual(_reversed[2:3], result["address_txs"])
    test_case.assertEqual(result.get("next_page", None), None)

    path_with_direction = path + "?direction={direction}"
    result = await test_case.request(
        path_with_direction, currency="btc", address=address2.address, direction="out"
    )
    address_txs.address_txs = txs[1:3]
    test_case.assertEqual(address_txs.to_dict()["address_txs"], result["address_txs"])
    path_with_range = (
        path_with_direction + "&min_height={min_height}&max_height={max_height}"
    )
    result = await test_case.request(
        path_with_range,
        currency="btc",
        address=address2.address,
        direction="out",
        min_height=1,
        max_height=1,
    )
    address_txs.address_txs = txs[2:3]
    test_case.assertEqual(address_txs.to_dict()["address_txs"], result["address_txs"])

    result = await test_case.request(
        path_with_range,
        currency="btc",
        address=address2.address,
        direction="out",
        min_height=2,
        max_height=2,
    )
    address_txs.address_txs = txs[1:2]
    test_case.assertEqual(address_txs.to_dict()["address_txs"], result["address_txs"])

    result = await test_case.request(
        path_with_direction, currency="btc", address=address2.address, direction="in"
    )
    address_txs.address_txs = txs[0:1]
    test_case.assertEqual(address_txs.to_dict()["address_txs"], result["address_txs"])

    def reverse(tx):
        tx_r = TxAccount.from_dict(copy.deepcopy(tx.to_dict()))
        tx_r.value.value = -tx_r.value.value
        for v in tx_r.value.fiat_values:
            v.value = -v.value
        return tx_r

    tx2_eth_r = reverse(tx2_eth)
    tx22_eth_r = reverse(tx22_eth)
    txs = AddressTxs(address_txs=[tx4_eth, tx22_eth_r, tx2_eth_r, tx1_eth])
    result = await test_case.request(path, currency="eth", address=eth_address.address)

    test_case.assertEqual(txs.to_dict()["address_txs"], result["address_txs"])
    result = await test_case.request(
        path_with_direction,
        currency="eth",
        address=eth_address.address,
        direction="out",
    )
    test_case.assertEqual(txs.to_dict()["address_txs"][1:3], result["address_txs"])

    path_with_range_and_tc = path_with_range + "&token_currency={token_currency}"
    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        address=eth_address.address,
        direction="",
        min_height=3,
        max_height="",
        token_currency="",
    )
    test_case.assertEqual(txs.to_dict()["address_txs"][0:2], result["address_txs"])

    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        address=eth_address.address,
        direction="",
        min_height=1,
        max_height=2,
        token_currency="",
    )

    test_case.assertEqual(txs.to_dict()["address_txs"][2:4], result["address_txs"])

    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        address=eth_address.address,
        direction="",
        min_height="",
        max_height=3,
        token_currency="",
    )
    test_case.assertEqual(txs.to_dict()["address_txs"][1:4], result["address_txs"])

    result = await test_case.request(path, currency="eth", address=eth_address2.address)
    assert len(result["address_txs"]) == 5

    assert [x["currency"] for x in result["address_txs"]] == [
        "eth",
        "eth",
        "weth",
        "usdt",
        "eth",
    ]
    assert [x["value"]["value"] for x in result["address_txs"]] == [
        124000000000000000000,
        123000000000000000000,
        -6818627949560085517,
        -3360488227,
        -123000000000000000000,
    ]
    assert [x["height"] for x in result["address_txs"]] == [3, 2, 2, 2, 1]

    path_tc = path + "?token_currency={token_currency}"
    result = await test_case.request(
        path_tc, currency="eth", address=eth_address2.address, token_currency="weth"
    )

    assert len(result["address_txs"]) == 1
    assert [x["currency"] for x in result["address_txs"]] == ["weth"]

    result = await test_case.request(
        path_tc, currency="eth", address=eth_address2.address, token_currency="eth"
    )

    assert len(result["address_txs"]) == 3
    assert [x["currency"] for x in result["address_txs"]] == ["eth", "eth", "eth"]
    assert [x["timestamp"] for x in result["address_txs"]] == [17, 16, 15]

    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        address=eth_address2.address,
        direction="",
        min_height=2,
        max_height=2,
        token_currency="",
    )

    assert len(result["address_txs"]) == 3
    assert [x["currency"] for x in result["address_txs"]] == ["eth", "weth", "usdt"]
    assert [x["height"] for x in result["address_txs"]] == [2, 2, 2]

    result = await test_case.request(
        path_with_range_and_tc,
        currency="eth",
        address=eth_address2.address,
        direction="",
        min_height=2,
        max_height=2,
        token_currency="weth",
    )

    assert len(result["address_txs"]) == 1
    assert [x["currency"] for x in result["address_txs"]] == ["weth"]
    assert [x["height"] for x in result["address_txs"]] == [2]

    path_with_pagesize = path + "?pagesize={pagesize}"
    result1 = await test_case.request(
        path_with_pagesize, currency="btc", address="addressE", pagesize=1
    )

    assert len(result1["address_txs"]) == 1
    assert result1["next_page"] == "13168304:1"

    path_with_pagesize = path + "?pagesize={pagesize}&page={page}"
    result2 = await test_case.request(
        path_with_pagesize,
        currency="btc",
        address="addressE",
        pagesize=1,
        page=result1["next_page"],
    )

    assert result1 != result2
    assert result2["next_page"] == "13168303:1"

    result3 = await test_case.request(
        path_with_pagesize,
        currency="btc",
        address="addressE",
        pagesize=1,
        page=result2["next_page"],
    )

    assert result2 != result3
    assert result3["next_page"] == "13168302:1"

    result4 = await test_case.request(
        path_with_pagesize,
        currency="btc",
        address="addressE",
        pagesize=1,
        page=result3["next_page"],
    )

    assert result3 != result4
    assert result4.get("next_page", None) is None

    addr_txs_total = (
        result1["address_txs"]
        + result2["address_txs"]
        + result3["address_txs"]
        + result4["address_txs"]
    )

    path_with_pagesize = path + "?pagesize={pagesize}"
    result_total = await test_case.request(
        path_with_pagesize, currency="btc", address="addressE", pagesize=4
    )

    assert result_total["address_txs"] == addr_txs_total

    path_with_pagesize_direction = path + ("?pagesize={pagesize}&direction={direction}")
    result = await test_case.request(
        path_with_pagesize_direction,
        currency="btc",
        address="addressE",
        direction="",
        pagesize=1,
    )

    assert len(result["address_txs"]) == 1
    assert result.get("next_page", None) is not None

    result1 = await test_case.request(
        path_with_pagesize_direction,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=1,
    )

    assert len(result1["address_txs"]) == 1
    assert result1.get("next_page", None) is not None

    path_with_pagesize_and_page = path + (
        "?pagesize={pagesize}&page={page}&direction={direction}"
    )
    result2 = await test_case.request(
        path_with_pagesize_and_page,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=1,
        page=result1["next_page"],
    )

    assert len(result2["address_txs"]) == 1
    assert result2.get("next_page", None) is not None

    result3 = await test_case.request(
        path_with_pagesize_and_page,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=1,
        page=result2["next_page"],
    )

    assert len(result3["address_txs"]) == 0
    assert result3.get("next_page", None) is None

    addr_txs_total = (
        result1["address_txs"] + result2["address_txs"] + result3["address_txs"]
    )

    result4 = await test_case.request(
        path_with_pagesize_direction,
        currency="btc",
        address="addressE",
        direction="in",
        pagesize=3,
    )

    assert addr_txs_total == result4["address_txs"]


async def list_tags_by_address(test_case):
    path = "/{currency}/addresses/{address}/tags"
    result = await test_case.request(
        path, currency="btc", address=addressWithTags.address
    )
    tags = [tag.to_dict() for tag in addressWithTags.tags]
    assert tags == result["address_tags"]

    result = await test_case.request(
        path, auth="unauthorized", currency="btc", address=addressWithTags.address
    )
    tags = [tag for tag in tags if tag["tagpack_is_public"]]
    assert tags == result["address_tags"]

    result = await test_case.request(
        path, currency="eth", address=eth_addressWithTags.address
    )

    expected = [
        omit(tag.to_dict(), {"inherited_from"}) for tag in eth_addressWithTags.tags
    ]

    assert expected == result["address_tags"]

    # Casing of the address does not matter for ethereum
    result = await test_case.request(
        path, currency="eth", address=eth_addressWithTags.address.upper()
    )
    assert expected == result["address_tags"]

    # Adding trailing whitespace is handled gracefully
    result = await test_case.request(
        path, currency="eth", address=eth_addressWithTags.address.upper() + "   "
    )
    assert expected == result["address_tags"]

    result = await test_case.request(
        path, currency="abcd", address=eth_addressWithTags.address
    )
    assert len(result["address_tags"]) == 2


async def list_address_neighbors(test_case):
    path = (
        "/{currency}/addresses/{address}/neighbors"
        "?include_labels={include_labels}&direction={direction}"
    )

    result = await test_case.request(
        path,
        currency="btc",
        address=address.address,
        include_labels=True,
        direction="out",
    )
    awton = addressWithTagsOutNeighbors.to_dict()
    assert awton == result

    result = await test_case.request(
        path + "&only_ids={only_ids}",
        currency="btc",
        address=address.address,
        include_labels=True,
        only_ids=addressF.address,
        direction="out",
    )
    awton2 = addressWithTagsOutNeighbors.to_dict()
    awton2["neighbors"] = awton2["neighbors"][1:2]
    test_case.assertEqual(awton2, result)

    result = await test_case.request(
        path,
        currency="btc",
        auth="unauthorized",
        address=address.address,
        include_labels=True,
        direction="out",
    )
    awton["neighbors"][0]["labels"] = ["labelX"]
    test_case.assertEqual(awton, result)

    result = await test_case.request(
        path,
        currency="btc",
        address=address.address,
        include_labels=True,
        direction="in",
    )
    assert addressWithTagsInNeighbors.to_dict() == result

    result = await test_case.request(
        path,
        currency="eth",
        address=eth_address.address,
        include_labels=True,
        direction="out",
    )
    assert ["0xabcdef", "0x123456"] == [
        n.address.address for n in eth_addressWithTagsOutNeighbors.neighbors
    ]

    result = await test_case.request(
        path,
        currency="eth",
        address=eth_address2.address,
        include_labels=True,
        direction="out",
    )
    test_case.assertEqual(eth_address2WithTokenFlows.to_dict(), result)

    # correct handling pages in wrong format.
    result, body = await test_case.requestOnly(
        (
            "/{currency}/addresses/{address}/neighbors?"
            "include_labels={include_labels}&direction={direction}&page={page}"
        ),
        None,
        currency="eth",
        include_labels=False,
        direction="in",
        page="PAGE2",
        address="0x123456",
    )
    assert result.status == 400
    assert "is not formatted correctly" in body


async def get_address_entity(test_case):
    path = "/{currency}/addresses/{address}/entity"
    "?include_tags={include_tags}"
    result = await test_case.request(
        path, currency="btc", address=address.address, include_tags=True
    )
    assert entityWithTags.to_dict() == result

    result = await test_case.request(
        path, currency="eth", address=eth_address.address, include_tags=True
    )

    ewt = eth_entityWithTags.to_dict()
    ewt["best_address_tag"].pop("inherited_from")
    assert ewt == result

    non_existent_address = "0x40a197b01CDeF4C77196045EaFFaC80F25Be00FE"
    result, body = await test_case.requestOnly(
        path, None, currency="eth", address=non_existent_address, include_tags=True
    )
    assert result.status == 404
    assert ("Address 0x40a197b01cdef4c77196045eaffac80f25be00fe not found") in body


async def list_address_links(test_case):
    path = "/{currency}/addresses/{address}/links?neighbor={neighbor}"
    result = await test_case.request(
        path, currency="btc", address=address.address, neighbor="addressE"
    )

    link = Links(
        links=[
            LinkUtxo(
                tx_hash="123456",
                currency="btc",
                input_value=make_values(eur=-0.1, usd=-0.2, value=-10000000),
                output_value=make_values(eur=0.1, usd=0.2, value=10000000),
                timestamp=1361497172,
                height=2,
            )
        ]
    )

    test_case.assertEqualWithList(link.to_dict(), result, "links", "tx_hash")

    txs = Links(links=[tx2_eth, tx22_eth])
    result = await test_case.request(
        path, currency="eth", address=eth_address.address, neighbor="0x123456"
    )

    # remember here in which order eth links are stored in db (might change
    # from ingest to ingest:
    first = result["links"][0]
    second = result["links"][1]

    test_case.assertEqualWithList(txs.to_dict(), result, "links", "tx_hash")

    path += "&pagesize={pagesize}"
    result = await test_case.request(
        path,
        currency="eth",
        address=eth_address.address,
        neighbor="0x123456",
        pagesize=1,
    )
    txs = Links(links=[tx2_eth if first["tx_hash"] == tx2_eth.tx_hash else tx22_eth])
    test_case.assertEqual([li.to_dict() for li in txs.links], result["links"])
    test_case.assertNotEqual(None, result.get("next_page", None))

    path += "&page={page}"
    result = await test_case.request(
        path,
        currency="eth",
        address=eth_address.address,
        neighbor="0x123456",
        page=result["next_page"],
        pagesize=1,
    )
    txs = Links(links=[tx22_eth if second["tx_hash"] == tx22_eth.tx_hash else tx2_eth])
    test_case.assertEqual([li.to_dict() for li in txs.links], result["links"])
    test_case.assertNotEqual(None, result.get("next_page", None))

    result = await test_case.request(
        path,
        currency="eth",
        address=eth_address.address,
        neighbor="0x123456",
        page=result["next_page"],
        pagesize=1,
    )
    test_case.assertEqual(Links(links=[]).to_dict(), result)

    # test order parameter
    path = "/{currency}/addresses/{address}/links?neighbor={neighbor}&order={order}"
    result = await test_case.request(
        path,
        currency="eth",
        address=eth_address.address,
        neighbor="0x123456",
        order="desc",
    )

    test_case.assertEqual(
        ["af6e0004", "af6e0003"], [x["tx_hash"] for x in result["links"]]
    )
    test_case.assertEqual(None, result.get("next_page", None))

    result = await test_case.request(
        path,
        currency="eth",
        address=eth_address.address,
        neighbor="0x123456",
        order="asc",
    )

    test_case.assertEqual(
        list(reversed(["af6e0004", "af6e0003"])),
        [x["tx_hash"] for x in result["links"]],
    )
    test_case.assertEqual(None, result.get("next_page", None))

    for mh, ex, exv in [
        (
            2,
            2,
            ["af6e0004", "af6e0003"],
        ),
        (
            3,
            1,
            ["af6e0004"],
        ),
        (
            4,
            0,
            [],
        ),
    ]:
        result = await test_case.request(
            path + "&min_height={min_height}",
            currency="eth",
            address=eth_address.address,
            neighbor="0x123456",
            min_height=mh,
            order="desc",
        )

        test_case.assertEqual(ex, len(result["links"]))
        test_case.assertEqual(exv, [x["tx_hash"] for x in result["links"]])

    rel = itertools.permutations(["A", "B", "C", "D", "E"], r=2)
    er = {("A", "E"): 1}
    queries = [(x, y, er.get((x, y), 0)) for x, y in rel]

    for o in ["desc", "asc"]:
        for a, b, n in queries:
            result = await test_case.request(
                path,
                currency="btc",
                address=f"address{a}",
                neighbor=f"address{b}",
                order=o,
            )
            test_case.assertEqual(n, len(result["links"]))

    for o in ["desc", "asc"]:
        for a, b, n in queries:
            result = await test_case.request(
                path + "&min_height=2",
                currency="btc",
                address=f"address{a}",
                neighbor=f"address{b}",
                order=o,
            )
            test_case.assertEqual(n, len(result["links"]))

    for o in ["desc", "asc"]:
        for a, b, n in queries:
            result = await test_case.request(
                path + "&max_height=3",
                currency="btc",
                address=f"address{a}",
                neighbor=f"address{b}",
                order=o,
            )
            test_case.assertEqual(n, len(result["links"]))

    er = {}
    queries = [(x, y, er.get((x, y), 0)) for x, y in rel]
    for o in ["desc", "asc"]:
        for a, b, n in queries:
            result = await test_case.request(
                path + "&min_height=3",
                currency="btc",
                address=f"address{a}",
                neighbor=f"address{b}",
                order=o,
            )
            test_case.assertEqual(n, len(result["links"]))

    for o in ["desc", "asc"]:
        for a, b, n in queries:
            result = await test_case.request(
                path + "&max_height=2",
                currency="btc",
                address=f"address{a}",
                neighbor=f"address{b}",
                order=o,
            )
            test_case.assertEqual(n, len(result["links"]))
