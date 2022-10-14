CREATE KEYSPACE IF NOT EXISTS btc_transformed
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE btc_transformed;

CREATE TYPE currency (
    value bigint,
    fiat_values list<float>
);

CREATE TABLE exchange_rates (
    block_id int PRIMARY KEY,
    fiat_values list<float>
);

CREATE TABLE address_transactions (
    address_id_group int,
    address_id int,
    tx_id bigint,
    value bigint,
    is_outgoing boolean,
    PRIMARY KEY (address_id_group, address_id, is_outgoing, tx_id)
) WITH CLUSTERING ORDER BY (address_id DESC, is_outgoing DESC, tx_id DESC);

CREATE TABLE address (
    address_id_group int,
    address_id int,
    address text,
    cluster_id int,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx_id bigint,
    last_tx_id bigint,
    total_received FROZEN <currency>,
    total_spent FROZEN <currency>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE address_ids_by_address_prefix (
    address_prefix text,
    address text,
    address_id int,
    PRIMARY KEY (address_prefix, address)
);

CREATE TABLE address_incoming_relations (
    dst_address_id_group int,
    dst_address_id int,
    src_address_id int,
    no_transactions int,
    estimated_value FROZEN <currency>,
    PRIMARY KEY (dst_address_id_group, dst_address_id, src_address_id)
);

CREATE TABLE address_outgoing_relations (
    src_address_id_group int,
    src_address_id int,
    dst_address_id int,
    no_transactions int,
    estimated_value FROZEN <currency>,
    PRIMARY KEY (src_address_id_group, src_address_id, dst_address_id)
);

CREATE TABLE cluster_transactions (
    cluster_id_group int,
    cluster_id int,
    tx_id bigint,
    value bigint,
    is_outgoing boolean,
    PRIMARY KEY (cluster_id_group, cluster_id, is_outgoing, tx_id)
) WITH CLUSTERING ORDER BY (cluster_id DESC, is_outgoing DESC, tx_id DESC);

CREATE TABLE cluster (
    cluster_id_group int,
    cluster_id int,
    no_addresses int,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx_id bigint,
    last_tx_id bigint,
    total_received FROZEN <currency>,
    total_spent FROZEN <currency>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (cluster_id_group, cluster_id)
);

CREATE TABLE cluster_addresses (
    cluster_id_group int,
    cluster_id int,
    address_id int,
    PRIMARY KEY (cluster_id_group, cluster_id, address_id)
);

CREATE TABLE cluster_incoming_relations (
    dst_cluster_id_group int,
    dst_cluster_id int,
    src_cluster_id int,
    no_transactions int,
    estimated_value FROZEN <currency>,
    PRIMARY KEY (dst_cluster_id_group, dst_cluster_id, src_cluster_id)
);

CREATE TABLE cluster_outgoing_relations (
    src_cluster_id_group int,
    src_cluster_id int,
    dst_cluster_id int,
    no_transactions int,
    estimated_value FROZEN <currency>,
    PRIMARY KEY (src_cluster_id_group, src_cluster_id, dst_cluster_id)
);

CREATE TABLE summary_statistics (
    timestamp int,
    no_blocks bigint PRIMARY KEY,
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint,
    no_clusters bigint,
    no_cluster_relations bigint
);

CREATE TABLE configuration (
    keyspace_name text PRIMARY KEY,
    bucket_size int,
    address_prefix_length int,
    bech_32_prefix text,
    coinjoin_filtering boolean,
    fiat_currencies list<text>
);
