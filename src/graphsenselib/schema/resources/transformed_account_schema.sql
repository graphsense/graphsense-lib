CREATE KEYSPACE IF NOT EXISTS 0x8BADF00D
    WITH replication = 0x8BADF00D_REPLICATION_CONFIG;

USE 0x8BADF00D;

// custom data types

CREATE TYPE currency (
    value varint,
    fiat_values list<float>
);

CREATE TYPE tx_reference (
    trace_index int,
    log_index int
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
    transaction_id bigint,
    transaction blob,
    PRIMARY KEY (transaction_id_group, transaction_id)
);

CREATE TABLE transaction_ids_by_transaction_prefix (
    transaction_prefix text,
    transaction blob,
    transaction_id bigint,
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
    tx_id bigint,
    PRIMARY KEY (block_id_group, block_id, tx_id)
) WITH CLUSTERING ORDER BY (block_id DESC, tx_id DESC);

CREATE TABLE address_transactions (
    address_id_group int,
    address_id_secondary_group int,
    address_id int,
    transaction_id bigint,
    tx_reference FROZEN <tx_reference>,
    currency text,
    is_outgoing boolean,
    PRIMARY KEY ((address_id_group, address_id_secondary_group), address_id, is_outgoing, currency, transaction_id, tx_reference)
) WITH CLUSTERING ORDER BY (address_id DESC, is_outgoing DESC, currency DESC, transaction_id DESC, tx_reference DESC);

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
    no_incoming_txs_zero_value int,
    no_outgoing_txs_zero_value int,
    first_tx_id bigint,
    last_tx_id bigint,
    total_received FROZEN <currency>,
    total_spent FROZEN <currency>,
    total_tokens_received map<text, frozen <currency>>,
    total_tokens_spent map<text, frozen <currency>>,
    in_degree int,
    out_degree int,
    in_degree_zero_value int,
    out_degree_zero_value int,
    is_contract boolean,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE address_incoming_relations (
    dst_address_id_group int,
    dst_address_id_secondary_group int,
    dst_address_id int,
    src_address_id int,
    no_transactions int,
    value FROZEN <currency>,
    token_values map<text, FROZEN <currency>>,
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
    token_values map<text, FROZEN <currency>>,
    PRIMARY KEY ((src_address_id_group, src_address_id_secondary_group), src_address_id, dst_address_id)
);

CREATE TABLE address_outgoing_relations_secondary_ids (
    src_address_id_group int PRIMARY KEY,
    max_secondary_id int
);

CREATE TABLE summary_statistics (
    id int PRIMARY KEY,
    timestamp int,
    timestamp_transform int,
    no_blocks bigint,
    no_blocks_transform bigint,
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint
);

CREATE TABLE balance (
    address_id_group int,
    address_id int,
    balance varint,
    currency text,
    PRIMARY KEY (address_id_group, address_id, currency)
);

CREATE TABLE configuration (
    keyspace_name text PRIMARY KEY,
    bucket_size int,
    block_bucket_size_address_txs int,
    addressrelations_ids_nbuckets int,
    address_prefix_length int,
    tx_prefix_length int,
    fiat_currencies list<text>
);


CREATE TABLE token_configuration (
    currency_ticker text PRIMARY KEY,
    token_address blob,
    standard text,
    decimals int,
    decimal_divisor bigint,
    peg_currency text
);
