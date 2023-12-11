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
    gas_limit varint,
    gas_used bigint,
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
    gas bigint,
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
    internal_index smallint,
    caller_address blob,
    transferto_address blob,
    call_info_index smallint,
    call_token_id int,
    call_value varint,
    note text,
    rejected boolean,
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

CREATE TYPE IF NOT EXISTS trc10_frozen_supply (
    frozen_amount bigint,
    frozen_days bigint
);

CREATE TABLE trc10 (
    owner_address blob,
    name text,
    abbr text,
    total_supply varint, -- bigint probably barely enough, varint just to be safe
    trx_num varint, -- int probably barely enough, varint just to be safe
    num varint, -- int probably barely enough, varint just to be safe
    start_time varint, -- removed 3 last digits to conform with eth, not always zeros, but can be large 3 outliers
    end_time varint, -- removed 3 last digits to conform with eth, not always zeros, multiple outliers
    description text,
    url text,
    id int,
    frozen_supply list<FROZEN<trc10_frozen_supply>>,
    public_latest_free_net_time varint,
    vote_score smallint,
    free_asset_net_limit bigint,
    public_free_asset_net_limit bigint,
    precision smallint,
    PRIMARY KEY (id)
);


CREATE TABLE fee (
    tx_hash_prefix text,
    tx_hash blob,
    fee bigint,
    energy_usage bigint,
    energy_fee bigint,
    origin_energy_usage bigint,
    energy_usage_total bigint,
    net_usage bigint,
    net_fee bigint,
    result int,
    energy_penalty_total bigint,
    PRIMARY KEY (tx_hash_prefix, tx_hash)
);
