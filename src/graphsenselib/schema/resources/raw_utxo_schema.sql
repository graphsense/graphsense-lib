CREATE KEYSPACE IF NOT EXISTS graphsense
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE graphsense;

CREATE TABLE block (
    block_id_group int,
    block_id int,
    block_hash blob,
    timestamp int,
    no_transactions int,
    PRIMARY KEY(block_id_group, block_id)
) WITH CLUSTERING ORDER BY (block_id DESC);

CREATE TYPE tx_input_output (
    address list<text>,
    value bigint,
    address_type smallint
);

CREATE TABLE transaction (
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

CREATE TABLE transaction_by_tx_prefix (
    tx_prefix text,
    tx_hash blob,
    tx_id bigint,
    PRIMARY KEY (tx_prefix, tx_hash)
);

CREATE TYPE tx_summary (
    tx_id bigint,
    no_inputs int,
    no_outputs int,
    total_input bigint,
    total_output bigint
);

CREATE TABLE block_transactions (
    block_id_group int,
    block_id int,
    txs list<FROZEN<tx_summary>>,
    PRIMARY KEY (block_id_group, block_id)
) WITH CLUSTERING ORDER BY (block_id DESC);

CREATE TABLE exchange_rates (
    date text PRIMARY KEY,
    fiat_values map<text, float>
);

CREATE TABLE summary_statistics (
    id text PRIMARY KEY,
    no_blocks int,
    no_txs bigint,
    timestamp int
);

CREATE TABLE configuration (
    id text PRIMARY KEY,
    block_bucket_size int,
    tx_prefix_length int,
    tx_bucket_size int
);
