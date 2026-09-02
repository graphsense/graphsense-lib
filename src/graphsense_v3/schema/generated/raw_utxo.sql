-- generated: raw / utxo
-- Do not edit by hand; edit graphsense_v3.schema.definitions.

CREATE KEYSPACE IF NOT EXISTS __KEYSPACE__
    WITH replication = __REPLICATION__;

USE __KEYSPACE__;

CREATE TABLE IF NOT EXISTS block (
    block_id_group int,
    block_id int,
    block_hash blob,
    timestamp bigint,
    no_transactions int,
    PRIMARY KEY (block_id_group, block_id)
)
    WITH CLUSTERING ORDER BY (block_id DESC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- Serves block-by-date as one partition slice. v2 had no such index, so the
-- lookup was a ~25-read serial binary search or an ALLOW FILTERING full scan.
CREATE TABLE IF NOT EXISTS block_by_date (
    day date,
    timestamp bigint,
    block_id int,
    PRIMARY KEY (day, timestamp, block_id)
)
    WITH CLUSTERING ORDER BY (timestamp ASC, block_id ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- Addressed by id, not by hash (D13). tx_id is (block_id << 32) + index in
-- both families, so block_id_group is a shift and a division away and a
-- lookup by id is ONE point read -- where v2 spent two per transaction, an
-- id->hash mapping table then the transaction
-- (`cassandra.py:5177-5203`), for every row of every page.
--
-- Partitioned by a run of blocks rather than one block: TRON would otherwise
-- have 85.8M partitions. tx_block_bucket_size is per network because a BTC
-- block holds ~1 480 transactions and a ZEC block ~5.
--
-- Header only. no_inputs/no_outputs live here so /graph/compare can
-- apply its _MAX_TOTAL_IOS gate without fetching the IO lists.
CREATE TABLE IF NOT EXISTS transaction (
    block_id_group int,                     -- block_id // tx_block_bucket_size
    block_id int,
    tx_id bigint,                           -- (block_id << 32) + transaction_index
    tx_hash blob,
    timestamp bigint,
    coinbase boolean,
    coinjoin boolean,
    total_input bigint,
    total_output bigint,
    no_inputs int,                          -- graph/compare work gate
    no_outputs int,
    version int,
    lock_time bigint,
    PRIMARY KEY (block_id_group, tx_id)
)
    WITH CLUSTERING ORDER BY (tx_id ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'}
    AND compression = {'class':'ZstdCompressor','chunk_length_in_kb':16};

-- Replaces transaction.inputs/outputs list<FROZEN<tx_input_output>>.
-- Partitioned by block like `transaction`, so one transaction's IOs are
-- a clustering slice and a whole block's are one partition -- but it
-- pages, and there is no >16MB mutation cliff.
-- An oversized mutation is REJECTED, not truncated, so under v2 a
-- 20k-input transaction is simply unwritable.
CREATE TABLE IF NOT EXISTS transaction_io (
    block_id_group int,
    tx_id bigint,
    is_output boolean,
    io_index int,
    address frozen<list<blob>>,             -- was list<text>
    value bigint,
    address_type smallint,
    script_hex blob,
    txinwitness frozen<list<blob>>,
    sequence bigint,
    PRIMARY KEY (block_id_group, tx_id, is_output, io_index)
)
    WITH CLUSTERING ORDER BY (tx_id ASC, is_output ASC, io_index ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'}
    AND compression = {'class':'ZstdCompressor','chunk_length_in_kb':16};

CREATE TABLE IF NOT EXISTS transaction_spent_in (
    spent_tx_prefix text,
    spent_tx_hash blob,
    spent_output_index int,
    spending_tx_hash blob,
    spending_input_index int,
    PRIMARY KEY (spent_tx_prefix, spent_tx_hash, spent_output_index)
)
    WITH compaction = {'class':'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS transaction_spending (
    spending_tx_prefix text,
    spending_tx_hash blob,
    spending_input_index int,
    spent_tx_hash blob,
    spent_output_index int,
    PRIMARY KEY (spending_tx_prefix, spending_tx_hash, spending_input_index)
)
    WITH compaction = {'class':'SizeTieredCompactionStrategy'};

-- The only route from a hash to a transaction, in both families. An exact
-- hash is a point read giving tx_id; a prefix is a range slice over
-- tx_hash within the one partition.
CREATE TABLE IF NOT EXISTS transaction_by_tx_prefix (
    tx_prefix text,
    tx_hash blob,
    tx_id bigint,
    PRIMARY KEY (tx_prefix, tx_hash)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'NONE'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- list<float> -> map: the positional list silently corrupts every historical row when a fiat currency is added or reordered.
CREATE TABLE IF NOT EXISTS exchange_rates (
    date text,
    fiat_values frozen<map<text, double>>,
    PRIMARY KEY (date)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS configuration (
    keyspace_name text,
    entity_buckets int,                     -- murmur3(entity) % this
    tx_page_size int,                       -- rows per *_transactions partition
    relation_buckets int,
    epoch_size int,                         -- blocks per stats epoch
    address_prefix_length int,
    tx_prefix_length int,
    block_bucket_size int,
    tx_block_bucket_size int,               -- blocks per transaction partition
    fiat_currencies frozen<list<text>>,
    schema_version int,
    PRIMARY KEY (keyspace_name)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS state (
    key text,
    value text,
    updated_at bigint,
    PRIMARY KEY (key)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS summary_statistics (
    id int,
    timestamp bigint,                       -- was int: unix seconds, 2038 cliff
    timestamp_transform bigint,
    no_blocks bigint,
    no_blocks_transform bigint,
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint,
    PRIMARY KEY (id)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};
