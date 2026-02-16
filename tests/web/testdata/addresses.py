import tests.web.testdata.tags as ts
from tests.web.testdata.txs import tx1_eth, tx2_eth, tx4_eth, tx22_eth
from graphsenselib.web.models import (
    Address,
    AddressTag,
    Entity,
    LabeledItemRef,
    LinkUtxo,
    Links,
    NeighborAddress,
    NeighborAddresses,
    TxSummary,
)
from graphsenselib.web.util.values_legacy import make_values

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
