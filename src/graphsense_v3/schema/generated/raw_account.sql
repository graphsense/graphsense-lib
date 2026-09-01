-- generated: raw / account
-- Do not edit by hand; edit graphsense_v3.schema.definitions.

CREATE KEYSPACE IF NOT EXISTS __KEYSPACE__
    WITH replication = __REPLICATION__;

USE __KEYSPACE__;

-- Unifies raw_account and raw_account_trx, which had silently drifted in three column types.
CREATE TABLE IF NOT EXISTS block (
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
    gas_limit bigint,                       -- was int (eth) / varint (trx)
    gas_used bigint,                        -- was int (eth) / bigint (trx)
    base_fee_per_gas bigint,
    timestamp bigint,
    transaction_count int,                  -- was smallint
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

-- Account tx_id is (block_id << 32) + transaction_index, so id -> hash
-- is a point read derivable from the id itself. Removes both transformed
-- mapping tables -- 56% of the TRX transformed keyspace -- and with them
-- the cross-table visibility race behind the 2026-07-03 incident.
CREATE TABLE IF NOT EXISTS block_transactions (
    block_id_group int,
    block_id int,
    transaction_index int,
    tx_hash blob,
    PRIMARY KEY (block_id_group, block_id, transaction_index)
)
    WITH CLUSTERING ORDER BY (block_id DESC, transaction_index ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- The four range pointers replace per-transaction log/trace tables.
-- A transaction's logs occupy a contiguous log_index range, because
-- log_index is a block-scoped counter and transactions execute in order,
-- so the pointers turn a whole-block partition scan into an exact
-- clustering slice -- the same read shape at ~2% of the storage
-- (~46 GB against ~2 TB on ETH). This is what /txs/{h}/flows and
-- normalize_address_transactions need; the latter does up to `pagesize`
-- such lookups per page.
-- PRE-RUN CHECK: contiguity is certain for ETH logs, unverified for
-- trace_index, and TRX uses a different trace model. If traces are not
-- contiguous, fall back to a duplicated table for traces only.
CREATE TABLE IF NOT EXISTS transaction (
    tx_hash_prefix text,
    tx_hash blob,
    nonce int,
    block_hash blob,
    block_id int,
    transaction_index int,
    from_address blob,
    to_address blob,
    value varint,
    gas bigint,
    gas_price varint,
    input blob,
    block_timestamp bigint,
    max_fee_per_gas bigint,
    max_priority_fee_per_gas bigint,
    transaction_type tinyint,               -- was bigint
    receipt_cumulative_gas_used bigint,     -- was varint
    receipt_gas_used bigint,                -- was varint
    receipt_contract_address blob,
    receipt_root blob,
    receipt_status tinyint,                 -- was bigint
    receipt_effective_gas_price bigint,
    max_fee_per_blob_gas bigint,
    blob_versioned_hashes frozen<list<blob>>,
    v smallint,
    r blob,                                 -- was varint; opaque 32 bytes
    s blob,
    first_log_index int,                    -- range pointer, see comment
    no_logs int,
    first_trace_index int,
    no_traces int,
    PRIMARY KEY (tx_hash_prefix, tx_hash)
)
    WITH compaction = {'class':'SizeTieredCompactionStrategy'}
    AND compression = {'class':'ZstdCompressor','chunk_length_in_kb':16};

-- Re-keyed off topic0. As a clustering column it meant a block's logs
-- could not be read in execution order, a specific log could not be
-- fetched without knowing its topic, and a topicless log had no legal
-- clustering value -- worked around by storing an empty blob.
CREATE TABLE IF NOT EXISTS log (
    block_id_group int,
    block_id int,
    log_index int,
    block_hash blob,
    address blob,
    data blob,
    topics frozen<list<blob>>,
    topic0 blob,
    tx_hash blob,
    transaction_index int,
    PRIMARY KEY (block_id_group, block_id, log_index)
)
    WITH CLUSTERING ORDER BY (block_id ASC, log_index ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'}
    AND compression = {'class':'ZstdCompressor','chunk_length_in_kb':16};

-- Chain-specific columns (eth vs trx) are appended per network.
CREATE TABLE IF NOT EXISTS trace (
    block_id_group int,
    block_id int,
    trace_index int,
    tx_hash blob,
    transaction_index int,
    PRIMARY KEY (block_id_group, block_id, trace_index)
)
    WITH CLUSTERING ORDER BY (block_id ASC, trace_index ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'}
    AND compression = {'class':'ZstdCompressor','chunk_length_in_kb':16};

CREATE TABLE IF NOT EXISTS exchange_rates (
    date text,
    fiat_values frozen<map<text, double>>,
    PRIMARY KEY (date)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS token_exchange_rates (
    asset text,                             -- 'token' is reserved
    date text,
    fiat_values frozen<map<text, double>>,
    PRIMARY KEY (asset, date)
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
