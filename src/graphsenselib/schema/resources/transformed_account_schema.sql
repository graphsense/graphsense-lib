CREATE KEYSPACE IF NOT EXISTS eth_transformed
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE eth_transformed;

// custom data types

CREATE TYPE currency (
    value varint,
    fiat_values list<float>
);

CREATE TYPE address_summary (
    total_received FROZEN <currency>,
    total_spent FROZEN <currency>
);

// transformed schema

CREATE TABLE exchange_rates (
    block_id int PRIMARY KEY,
    fiat_values list<float>
);

CREATE TABLE transaction_ids_by_transaction_id_group (
    transaction_id_group int,
    transaction_id int,
    transaction blob,
    PRIMARY KEY (transaction_id_group, transaction_id)
);

CREATE TABLE transaction_ids_by_transaction_prefix (
    transaction_prefix text,
    transaction blob,
    transaction_id int,
    PRIMARY KEY (transaction_prefix, transaction)
);

CREATE TABLE address_ids_by_address_prefix (
    address_prefix text,
    address blob,
    address_id int,
    PRIMARY KEY (address_prefix, address)
);

CREATE TABLE block_transactions(
    block_id_group int,
    block_id int,
    txs list<int>,
    PRIMARY KEY (block_id_group, block_id)
);

CREATE TABLE address_transactions (
    address_id_group int,
    address_id_secondary_group int,
    address_id int,
    transaction_id int,
    is_outgoing boolean,
    PRIMARY KEY ((address_id_group, address_id_secondary_group), address_id, is_outgoing, transaction_id)
) WITH CLUSTERING ORDER BY (address_id DESC, is_outgoing DESC, transaction_id DESC);

CREATE TABLE address_transactions_secondary_ids (
    address_id_group int PRIMARY KEY,
    max_secondary_id int
);

CREATE TABLE address (
    address_id_group int,
    address_id int,
    address blob,
    no_incoming_txs int,
    no_outgoing_txs int,
    first_tx_id int,
    last_tx_id int,
    total_received FROZEN <currency>,
    total_spent FROZEN <currency>,
    in_degree int,
    out_degree int,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE address_incoming_relations (
    dst_address_id_group int,
    dst_address_id_secondary_group int,
    dst_address_id int,
    src_address_id int,
    no_transactions int,
    value FROZEN <currency>,
    PRIMARY KEY ((dst_address_id_group, dst_address_id_secondary_group), dst_address_id, src_address_id)
);

CREATE TABLE address_incoming_relations_secondary_ids (
    dst_address_id_group int PRIMARY KEY,
    max_secondary_id int
);

CREATE TABLE address_outgoing_relations (
    src_address_id_group int,
    src_address_id_secondary_group int,
    src_address_id int,
    dst_address_id int,
    no_transactions int,
    value frozen <currency>,
    PRIMARY KEY ((src_address_id_group, src_address_id_secondary_group), src_address_id, dst_address_id)
);

CREATE TABLE address_outgoing_relations_secondary_ids (
    src_address_id_group int PRIMARY KEY,
    max_secondary_id int
);

CREATE TABLE summary_statistics (
    timestamp int,
    no_blocks bigint PRIMARY KEY,
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint
);

CREATE TABLE balance (
    address_id_group int,
    address_id int,
    balance varint,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE configuration (
    keyspace_name text PRIMARY KEY,
    bucket_size int,
    address_prefix_length int,
    tx_prefix_length int,
    fiat_currencies list<text>
);
