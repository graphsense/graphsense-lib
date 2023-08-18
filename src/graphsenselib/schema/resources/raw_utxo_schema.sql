CREATE KEYSPACE IF NOT EXISTS 0x8BADF00D
    WITH replication = 0x8BADF00D_REPLICATION_CONFIG;

USE 0x8BADF00D;

CREATE TABLE block (
    block_id_group int,
    block_id int,
    block_hash blob,
    timestamp int,
    no_transactions int,
    PRIMARY KEY(block_id_group, block_id)
) WITH CLUSTERING ORDER BY (block_id DESC);

CREATE TYPE IF NOT EXISTS tx_input_output (
    address list<text>,
    value bigint,
    address_type smallint
);

CREATE TABLE IF NOT EXISTS transaction (
    tx_id_group int,
    tx_id bigint,
    tx_hash blob,
    block_id int,
    timestamp int,
    coinbase boolean,
    total_input bigint,
    total_output bigint,
    inputs list<FROZEN<tx_input_output>>,
    outputs list<FROZEN<tx_input_output>>,
    coinjoin boolean,
    PRIMARY KEY (tx_id_group, tx_id)
);

CREATE TABLE IF NOT EXISTS transaction_spent_in (
    spent_tx_prefix text,
    spent_tx_hash blob,
    spent_output_index int,
    spending_tx_hash blob,
    spending_input_index int,
    PRIMARY KEY (spent_tx_prefix, spent_tx_hash, spent_output_index)
)
WITH CLUSTERING ORDER BY (spent_tx_hash ASC, spent_output_index ASC);

CREATE TABLE IF NOT EXISTS transaction_spending (
    spending_tx_prefix text,
    spending_tx_hash blob,
    spending_input_index int,
    spent_tx_hash blob,
    spent_output_index int,
    PRIMARY KEY (spending_tx_prefix, spending_tx_hash, spending_input_index)
)
WITH CLUSTERING ORDER BY (spending_tx_hash ASC, spending_input_index ASC);

CREATE TABLE IF NOT EXISTS transaction_by_tx_prefix (
    tx_prefix text,
    tx_hash blob,
    tx_id bigint,
    PRIMARY KEY (tx_prefix, tx_hash)
);

CREATE TYPE IF NOT EXISTS tx_summary (
    tx_id bigint,
    no_inputs int,
    no_outputs int,
    total_input bigint,
    total_output bigint
);

CREATE TABLE IF NOT EXISTS block_transactions (
    block_id_group int,
    block_id int,
    txs list<FROZEN<tx_summary>>,
    PRIMARY KEY (block_id_group, block_id)
) WITH CLUSTERING ORDER BY (block_id DESC);

CREATE TABLE IF NOT EXISTS exchange_rates (
    date text PRIMARY KEY,
    fiat_values map<text, float>
);

CREATE TABLE IF NOT EXISTS summary_statistics (
    id text PRIMARY KEY,
    no_blocks int,
    no_txs bigint,
    timestamp int
);

CREATE TABLE IF NOT EXISTS configuration (
    id text PRIMARY KEY,
    block_bucket_size int,
    tx_prefix_length int,
    tx_bucket_size int
);
