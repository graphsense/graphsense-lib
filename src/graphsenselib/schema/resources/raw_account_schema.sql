CREATE KEYSPACE IF NOT EXISTS 0x8BADF00D
    WITH replication = 0x8BADF00D_REPLICATION_CONFIG;
USE 0x8BADF00D;

CREATE TABLE block (
    block_id_group int,
    block_id int,
    block_hash blob,
    parent_hash blob,
    nonce blob,
    sha3_uncles blob,
    logs_bloom blob,
    transactions_root blob,
    state_root blob,
    receipts_root blob,
    miner blob,
    difficulty varint,
    total_difficulty varint,
    size int,
    extra_data blob,
    gas_limit int,
    gas_used int,
    base_fee_per_gas bigint,
    timestamp int,
    transaction_count smallint,
    PRIMARY KEY (block_id_group, block_id)
);

CREATE TABLE log (
    block_id_group int,
    block_id int,
    block_hash blob,
    address blob,
    data blob,
    topics list<blob>,
    topic0 blob,
    tx_hash blob,
    log_index int,
    transaction_index smallint,
    PRIMARY KEY (block_id_group, block_id, topic0, log_index)
) WITH CLUSTERING ORDER BY (block_id ASC, topic0 ASC, log_index ASC);

CREATE TABLE transaction (
    tx_hash_prefix text,
    tx_hash blob,
    nonce int,
    block_hash blob,
    block_id int,
    transaction_index smallint,
    from_address blob,
    to_address blob,
    value varint,
    gas int,
    gas_price varint,
    input blob,
    block_timestamp int,
    max_fee_per_gas bigint,
    max_priority_fee_per_gas bigint,
    transaction_type bigint,
    receipt_cumulative_gas_used varint,
    receipt_gas_used varint,
    receipt_contract_address blob,
    receipt_root blob,
    receipt_status bigint,
    receipt_effective_gas_price bigint,
    PRIMARY KEY (tx_hash_prefix, tx_hash)
);

CREATE TABLE trace (
    block_id_group int,
    block_id int,
    tx_hash blob,
    transaction_index smallint,
    from_address blob,
    to_address blob,
    value varint,
    input blob,
    output blob,
    trace_type text,
    call_type text,
    reward_type text,
    gas int,
    gas_used bigint,
    subtraces int,
    trace_address text,
    error text,
    status smallint,
    trace_id text,
    trace_index int,
    PRIMARY KEY (block_id_group, block_id, trace_index)
);

CREATE TABLE exchange_rates (
    date text PRIMARY KEY,
    fiat_values map<text, float>
);

CREATE TABLE configuration (
    id text PRIMARY KEY,
    block_bucket_size int,
    tx_prefix_length int
);
