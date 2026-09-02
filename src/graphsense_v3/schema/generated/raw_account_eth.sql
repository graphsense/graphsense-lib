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
    no_transactions int,                    -- was smallint, and was transaction_count
    PRIMARY KEY (block_id_group, block_id)
)
    WITH CLUSTERING ORDER BY (block_id DESC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- Serves block-by-date as one partition slice. v2 had no such index, so the
-- lookup was a ~25-read serial binary search or an ALLOW FILTERING full scan.
CREATE TABLE IF NOT EXISTS block_by_date (
    day int,                                -- yyyymmdd, UTC
    timestamp bigint,
    block_id int,
    PRIMARY KEY (day, timestamp, block_id)
)
    WITH CLUSTERING ORDER BY (timestamp ASC, block_id ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

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
    block_id_group int,                     -- block_id // tx_block_bucket_size
    tx_id bigint,                           -- (block_id << 32) + transaction_index
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
    PRIMARY KEY (block_id_group, tx_id)
)
    WITH CLUSTERING ORDER BY (tx_id ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'}
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
    tx_id bigint,                           -- (block_id << 32) + transaction_index
    PRIMARY KEY (block_id_group, block_id, log_index)
)
    WITH CLUSTERING ORDER BY (block_id ASC, log_index ASC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'}
    AND compression = {'class':'ZstdCompressor','chunk_length_in_kb':16};

-- Shared columns first; the rest are that chain's own trace model.
CREATE TABLE IF NOT EXISTS trace (
    block_id_group int,
    block_id int,
    trace_index int,
    tx_hash blob,
    tx_id bigint,                           -- (block_id << 32) + transaction_index
    from_address blob,
    to_address blob,
    value varint,
    status smallint,                        -- 1 = success, 0 = failed
    input blob,
    output blob,
    trace_type text,
    call_type text,
    reward_type text,
    gas bigint,                             -- was int
    gas_used bigint,
    subtraces int,
    trace_address text,
    error text,
    trace_id text,
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
    tx_block_bucket_size int,               -- blocks per transaction partition
    fiat_currencies frozen<list<text>>,
    schema_version int,
    PRIMARY KEY (keyspace_name)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- Write-once flags about the keyspace as a whole. Named for what it
-- holds: v2 called this `state`, which reads as an invitation to keep
-- cursors here, and a cursor is exactly what must not live in it --
-- these rows are set once and never advanced.
--
-- Known keys, and there should be few:
--   complete -- every table of this keyspace has been fully written.
--     Written LAST, after every other write of the run. Nothing may
--     read the keyspace as authoritative without it: a half-written
--     keyspace is otherwise indistinguishable from a finished one,
--     which would make a comparison silently measure missing data.
CREATE TABLE IF NOT EXISTS markers (
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
