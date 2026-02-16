import tests.web.testdata.tags as ts
from tests.web.testdata.addresses import (
    addressD,
    addressE,
    entity2,
    entity3,
    entity4,
    entity5,
    entityWithTags,
    eth_address,
    eth_address2,
    eth_addressWithTagsOutNeighbors,
    eth_entityWithTags,
    eth_entityWithTokens,
)
from graphsenselib.web.models import (
    AddressTag,
    Entity,
    EntityAddresses,
    LabeledItemRef,
    NeighborEntities,
    NeighborEntity,
    TxSummary,
)
from graphsenselib.web.util.values_legacy import make_values

tagstore_public_tp_uri = "tagpack_public.yaml"

eth_entity = Entity(
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

eth_entity2 = Entity(
    currency="eth",
    actors=[
        LabeledItemRef(id="actorX", label="Actor X"),
        LabeledItemRef(id="actorY", label="Actor Y"),
    ],
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
    total_tokens_received=eth_address2.total_tokens_received,
)

eth_neighbors = []
for n in eth_addressWithTagsOutNeighbors.neighbors:
    d = n.to_dict()
    d.pop("address")
    nn = NeighborEntity(**d)
    eth_neighbors.append(nn)

eth_neighbors[0].entity = eth_entity
eth_neighbors[0].entity.best_address_tag.inherited_from = "cluster"
eth_neighbors[1].entity = eth_entity2

eth_entityWithTagsOutNeighbors = NeighborEntities(
    next_page=None, neighbors=eth_neighbors
)

entityWithTagsOutNeighbors = NeighborEntities(
    next_page=None,
    neighbors=[
        NeighborEntity(
            entity=entity2,
            value=make_values(eur=2411.06, usd=3074.92, value=48610000000),
            labels=["labelX", "labelY"],
            no_txs=1,
        ),
        NeighborEntity(
            entity=entity3,
            value=make_values(eur=1078.04, usd=1397.54, value=3375700000),
            labels=[],
            no_txs=3,
        ),
    ],
)

entityWithTagsInNeighbors = NeighborEntities(
    next_page=None,
    neighbors=[
        NeighborEntity(
            entity=entity4,
            value=make_values(usd=0.96, eur=0.72, value=190000),
            labels=[],
            no_txs=10,
        ),
        NeighborEntity(
            entity=entity5,
            value=make_values(eur=295.7, usd=404.02, value=50000000),
            labels=[],
            no_txs=1,
        ),
    ],
)

entityWithTagsAddresses = EntityAddresses(
    next_page=None, addresses=[addressD, addressE]
)

tag_entityA = Entity(
    currency="btc",
    no_address_tags=2,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=2,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=12,
    root_address="tag_addressA",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=None,
)

tag_entityB = Entity(
    currency="btc",
    no_address_tags=2,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=2,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=14,
    root_address="tag_addressC",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressC",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=True,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        inherited_from="cluster",
        tagpack_uri=tagstore_public_tp_uri,
        concepts=[],
        entity=14,
        tag_type="actor",
    ),
)

tag_entityC = Entity(
    currency="btc",
    no_address_tags=3,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=3,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=16,
    root_address="tag_addressE",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressE",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=True,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        tagpack_uri=tagstore_public_tp_uri,
        inherited_from="cluster",
        concepts=[],
        entity=16,
        tag_type="actor",
    ),
)

tag_entityD = Entity(
    currency="btc",
    no_address_tags=1,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=1,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=19,
    root_address="tag_addressH",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressH",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=False,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        tagpack_uri=tagstore_public_tp_uri,
        concepts=[],
        inherited_from="cluster",
        entity=19,
        tag_type="actor",
    ),
)

tag_entityE = Entity(
    currency="btc",
    no_address_tags=3,
    no_outgoing_txs=0,
    last_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    total_spent=make_values(usd=0.0, value=0, eur=0.0),
    in_degree=0,
    no_addresses=1,
    total_received=make_values(usd=0.0, value=0, eur=0.0),
    no_incoming_txs=0,
    entity=20,
    root_address="tag_addressI",
    out_degree=0,
    first_tx=TxSummary(timestamp=1434554207, height=1, tx_hash="4567"),
    balance=make_values(eur=0.0, usd=0.0, value=0),
    best_address_tag=AddressTag(
        category="organization",
        label="x",
        abuse=None,
        lastmod=1562112000,
        source="Unspecified",
        address="tag_addressI",
        currency="BTC",
        tagpack_is_public=True,
        is_cluster_definer=False,
        confidence="ownership",
        confidence_level=100,
        tagpack_creator="GraphSense Core Team",
        tagpack_title="GraphSense",
        tagpack_uri=tagstore_public_tp_uri,
        inherited_from="cluster",
        concepts=[],
        entity=20,
        tag_type="actor",
    ),
)
