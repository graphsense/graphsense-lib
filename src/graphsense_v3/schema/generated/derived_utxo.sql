-- generated: derived / utxo
-- Do not edit by hand; edit graphsense_v3.schema.definitions.

CREATE KEYSPACE IF NOT EXISTS __KEYSPACE__
    WITH replication = __REPLICATION__;

USE __KEYSPACE__;

CREATE TYPE IF NOT EXISTS currency (
    value varint,
    fiat_values frozen<map<text, double>>
);

-- Aggregates as SUMMABLE ROWS, replacing v2's client-side read-modify-write.
--
-- A read is `WHERE bucket = ? AND entity = ?` and sum the slice. Base rows
-- and delta rows have the same shape, so there is no watermark: compaction
-- replaces N rows with their sum, which is idempotent and re-runnable, and a
-- read is correct whether it ran or not. That is the whole point -- an
-- append-only log with a `folded_through` watermark just relocates the
-- correctness bug it was meant to remove.
--
-- The ingest path therefore never reads: it blind-inserts one epoch row per
-- touched entity. Deleted with the read-before-write: 4-7 point reads per
-- touched address per batch, most of wal.py's reason to exist,
-- collection-overwrite tombstones (v2 relations sit at ~41% droppable),
-- db/parallel.py (which exists only for the ~1K reads/s per-process cap from
-- UDT deserialisation on that path), and the LOGGED multi-table batch.
--
-- Because the bucket is the whole partition key, every epoch row for an
-- entity is in ONE partition -- so a compaction (write the sum to epoch 0,
-- delete the absorbed epochs) is a single-partition batch, which Cassandra
-- applies atomically. Compaction cannot half-happen.
--
-- Degrees are distinct-counterparty counts and are NOT summable, so they live
-- on epoch 0 and are maintained by compaction: stale between runs, exact
-- after one. That is the accepted staleness tradeoff, spent where it costs
-- least. Identity columns are blind-upserted to epoch 0 by the writer, which
-- is a per-column upsert, not a read-modify-write.
CREATE TABLE IF NOT EXISTS address_stats (
    address_bucket int,                     -- crc32(entity) % entity_buckets
    address blob,
    epoch int,                              -- 0 = compacted base; else block_id // epoch_size + 1
    no_incoming_txs bigint,
    no_outgoing_txs bigint,
    no_incoming_txs_zero_value bigint,
    no_outgoing_txs_zero_value bigint,
    total_received frozen<currency>,
    total_spent frozen<currency>,
    first_tx_id bigint,                     -- min-merge
    last_tx_id bigint,                      -- max-merge
    in_degree bigint,                       -- epoch 0 only: not summable
    out_degree bigint,
    in_degree_zero_value bigint,
    out_degree_zero_value bigint,
    in_tx_page_max int,                     -- epoch 0 only: paging cursors
    out_tx_page_max int,
    in_tx_ordinal_next bigint,
    out_tx_ordinal_next bigint,
    in_zero_tx_page_max int,
    out_zero_tx_page_max int,
    in_zero_tx_ordinal_next bigint,
    out_zero_tx_ordinal_next bigint,
    PRIMARY KEY (address_bucket, address, epoch)
)
    WITH CLUSTERING ORDER BY (address ASC, epoch ASC)
    AND compaction = {'class':'LeveledCompactionStrategy','sstable_size_in_mb':'160'};

-- Paged by the entity's own transaction ordinal, so every partition holds
-- exactly tx_page_size rows: immune to burst and to dormancy, no empty
-- buckets, no 455M-row cell. tx_page_max lives on the stats row, which is
-- already read on nearly every path, so there is no side table.
--
-- is_outgoing is in the PARTITION key because Cassandra requires
-- clustering restrictions to form a prefix -- below tx_id it could not be
-- pushed down at all, and direction=out on an address with 10M incoming
-- and 100 outgoing would scan the lot. currency stays below tx_id: in the
-- partition key it would make the UNFILTERED query fan out over every
-- asset held, which is exactly the v2 pathology.
--
-- is_zero_value is there for the same reason. A zero-value transfer is a
-- contract call that moved nothing, and on ETH and TRON they dominate an
-- address's listing; excluding them is the DEFAULT read, so it must be a
-- point partition rather than a filter applied after paging -- otherwise
-- a page of 100 rows can return 3. Below tx_id it could not be pushed
-- down; above it, it would order the partition by zero-ness before
-- recency and break the ordering contract.
--
-- Cost: including zero-value merges two partition streams instead of
-- one, the way both directions already merge two. A value BRACKET was
-- considered and rejected -- N brackets means an N-way merge for the
-- unfiltered query, and the boundaries become a constant that can never
-- change without rewriting the table. Zero is the one boundary that is
-- a property of the transfer rather than of a chosen scale.
CREATE TABLE IF NOT EXISTS address_transactions (
    address blob,
    is_outgoing boolean,
    is_zero_value boolean,                  -- value == 0; see the comment
    tx_page int,
    tx_id bigint,
    value varint,
    balance varint,                         -- after this transaction
    PRIMARY KEY ((address, is_outgoing, is_zero_value, tx_page), tx_id)
)
    WITH CLUSTERING ORDER BY (tx_id DESC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- Append-only tail. Ingest writes ONLY here, keyed by block_batch, which
-- is derivable from the block and needs no read -- an ordinal would
-- reintroduce the read-before-write that the stats model removes.
-- Compaction drains a batch into the paged table, assigning ordinals in
-- tx_id order. A read merges tail-then-pages, both tx_id DESC. Since
-- newest-first is the API default, the common request is served from the
-- tail alone and never touches a page.
CREATE TABLE IF NOT EXISTS address_transactions_recent (
    address blob,
    is_outgoing boolean,
    is_zero_value boolean,                  -- value == 0; see the comment
    block_batch int,
    tx_id bigint,
    value varint,
    PRIMARY KEY ((address, is_outgoing, is_zero_value, block_batch), tx_id)
)
    WITH CLUSTERING ORDER BY (tx_id DESC)
    AND compaction = {'class':'LeveledCompactionStrategy'}
    AND gc_grace_seconds = 259200;

-- Ordinal pages are not tx_id-aligned -- the one thing block bucketing did
-- better -- so a height or date filter cannot compute which pages hold its
-- range. The entry page is a point-slice (first_tx_id <= :hi LIMIT 1) and the
-- walk proceeds downward. One row for a typical address, ~37 000 for TRON
-- USDT, all in one partition of about a megabyte. Read only when a range
-- filter is present.
--
-- Keyed per partition class of address_transactions, zero-ness included:
-- the pages it indexes are numbered per class, so one index per class.
CREATE TABLE IF NOT EXISTS address_tx_pages (
    address blob,
    is_outgoing boolean,
    is_zero_value boolean,
    first_tx_id bigint,
    tx_page int,
    PRIMARY KEY ((address, is_outgoing, is_zero_value), first_tx_id)
)
    WITH CLUSTERING ORDER BY (first_tx_id DESC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- One entity's relations, not 25 000 entities'. relation_buckets is a config
-- constant so a read scatters over 0..N-1 unconditionally and stops on
-- in_degree/out_degree -- the four *_secondary_ids watermark tables are gone.
-- only_ids stays a point read: the bucket is computed from the counterparty.
-- v2 uses 100 buckets; 16 is enough and cuts the fan-out four-fold.
--
-- The edge carries its OWN page cursor rather than deriving one from
-- no_transactions. The two are not interchangeable while the UTXO transform
-- nets flows per (tx, entity) -- and that is precisely the chain where /links
-- already cannot trust the netted edge.
CREATE TABLE IF NOT EXISTS address_incoming_relations (
    dst_address blob,
    rel_bucket int,                         -- crc32(far side) % relation_buckets
    src_address blob,
    epoch int,                              -- as address_stats: summable
    no_transactions bigint,                 -- was int
    value frozen<currency>,
    link_page_max int,                      -- epoch 0 only
    link_ordinal_next bigint,
    PRIMARY KEY ((dst_address, rel_bucket), src_address, epoch)
)
    WITH CLUSTERING ORDER BY (src_address ASC, epoch ASC)
    AND compaction = {'class':'LeveledCompactionStrategy','sstable_size_in_mb':'160'};

-- One entity's relations, not 25 000 entities'. relation_buckets is a config
-- constant so a read scatters over 0..N-1 unconditionally and stops on
-- in_degree/out_degree -- the four *_secondary_ids watermark tables are gone.
-- only_ids stays a point read: the bucket is computed from the counterparty.
-- v2 uses 100 buckets; 16 is enough and cuts the fan-out four-fold.
--
-- The edge carries its OWN page cursor rather than deriving one from
-- no_transactions. The two are not interchangeable while the UTXO transform
-- nets flows per (tx, entity) -- and that is precisely the chain where /links
-- already cannot trust the netted edge.
CREATE TABLE IF NOT EXISTS address_outgoing_relations (
    src_address blob,
    rel_bucket int,                         -- crc32(far side) % relation_buckets
    dst_address blob,
    epoch int,                              -- as address_stats: summable
    no_transactions bigint,                 -- was int
    value frozen<currency>,
    link_page_max int,                      -- epoch 0 only
    link_ordinal_next bigint,
    PRIMARY KEY ((src_address, rel_bucket), dst_address, epoch)
)
    WITH CLUSTERING ORDER BY (dst_address ASC, epoch ASC)
    AND compaction = {'class':'LeveledCompactionStrategy','sstable_size_in_mb':'160'};

-- The single most expensive table in v3 -- roughly three times the cost of
-- byte-keying everything else -- so the layout is chosen per family.
--
-- A partition costs its key plus ~40 bytes of overhead; a row costs its
-- clustering columns. The two layouts trade the counterparty between them.
-- UTXO has 1.5e9 BTC addresses averaging 1.2 transactions per edge, so
-- per-partition overhead dominates and partition-per-source wins by ~42%
-- (935 vs 1621 logical GiB). Account has an order of magnitude fewer
-- addresses with more transactions per edge (TRX 2.27, ETH 1.92), so the
-- repeated destination costs more than the partitions it saves.
--
-- Both serve /links as a point-slice; the UTXO form restricts dst as a
-- clustering PREFIX, so it is pushed down in full. Both writers already
-- materialise these tuples when computing no_transactions -- today they
-- aggregate them away, which is why /links has to rescan raw io membership.
CREATE TABLE IF NOT EXISTS address_link_transactions (
    src_address blob,
    dst_bucket int,                         -- crc32(dst) % relation_buckets
    dst_address blob,
    tx_id bigint,
    input_value varint,
    output_value varint,
    PRIMARY KEY ((src_address, dst_bucket), dst_address, tx_id)
)
    WITH CLUSTERING ORDER BY (dst_address ASC, tx_id DESC)
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

-- The current balance: sum the slice. Deltas are blind-inserted and
-- compaction folds them into epoch 0, so this table forgets history --
-- `balance_history` is what remembers it.
CREATE TABLE IF NOT EXISTS balance (
    address_bucket int,
    address blob,
    currency text,
    epoch int,
    balance varint,                         -- signed delta; summable like address_stats
    PRIMARY KEY (address_bucket, address, currency, epoch)
)
    WITH compaction = {'class':'LeveledCompactionStrategy','sstable_size_in_mb':'160'};

-- Balance over time. `balance` holds deltas that compaction folds away,
-- so the history has to be kept somewhere that is never folded.
--
-- A RUNNING TOTAL, not a delta, which is the one place this schema
-- departs from summable rows -- and deliberately. Summing a delta slice
-- to answer 'what was the balance on day D' costs one row per active day
-- since the address was created; a running total makes it a single
-- `day <= D LIMIT 1`, and a balance chart a single slice. The cost is
-- that the incremental path cannot maintain this without reading, so it
-- is written by the same periodic job that folds `balance` -- which
-- reads those epochs anyway. Never on the hot write path.
--
-- DAY, not epoch: a row per active day is at most ~5 500 for a
-- 15-year-old address, where a row per 1 000-block epoch would be
-- ~85 800 on TRON. Day is also the granularity a chart asks for.
-- Rows exist only for days the address moved; the balance between them
-- is the last row's, which is what `day <= D LIMIT 1` returns.
CREATE TABLE IF NOT EXISTS balance_history (
    address_bucket int,
    address blob,
    currency text,
    day int,                                -- yyyymmdd, UTC
    balance varint,                         -- RUNNING TOTAL at end of day, not a delta
    PRIMARY KEY (address_bucket, address, currency, day)
)
    WITH CLUSTERING ORDER BY (address ASC, currency ASC, day DESC)
    AND compaction = {'class':'LeveledCompactionStrategy','sstable_size_in_mb':'160'};

-- Search only -- exact address -> entity is now direct, so this is no longer
-- a lookup table and carries no id.
--
-- Packed bytes are not order-preserving ACROSS address types: get_codec
-- dispatches on alphabet (bech32 5 bits/char, base58 ~5.86, base62 ~5.95), so
-- a prefix range slice on the packed form returns wrong results. Prefix search
-- therefore reads the whole prefix partition and filters client-side, which is
-- only viable if partitions are small -- making the bech32 prefix fix a
-- PREREQUISITE, not an improvement. v2 strips only 'bc' from BTC, leaving
-- 1q.../1p... so two of four prefix chars are constants and the entire segwit
-- space lands in 32^2 = 1024 partitions of ~390k rows, where LTC gets ~1M
-- partitions of ~380.
CREATE TABLE IF NOT EXISTS address_by_prefix (
    address_prefix text,
    address blob,
    PRIMARY KEY (address_prefix, address)
)
    WITH compaction = {'class':'SizeTieredCompactionStrategy'};

-- One table for every asset's price, the native coin included. Split in
-- two, the token half inherited no bucketing -- v2 keys it
-- (asset, block_id), so one asset's partition grows with the chain:
-- 85.8M rows for a TRON stablecoin. Merged, both get the same key and
-- the same fix.
--
-- Bucketed on the block. v2's coin table keys by block_id alone: 20.25M
-- single-row partitions of 86 bytes on TRX, carrying a 24 MB bloom
-- filter and 2.5 MB index summary over 778 MB of data -- the partition
-- index exceeds what it indexes. /rates/{height} stays a point read and
-- order A5 (rate at or before a height) is preserved; the group is
-- computed client-side.
CREATE TABLE IF NOT EXISTS exchange_rates (
    asset text,                             -- native coin ticker, or a token's
    block_id_group int,
    block_id int,
    fiat_values frozen<map<text, double>>,
    PRIMARY KEY ((asset, block_id_group), block_id)
)
    WITH CLUSTERING ORDER BY (block_id DESC)
    AND caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS configuration (
    keyspace_name text,
    entity_buckets int,                     -- crc32(entity) % this; see codec.bucket
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

-- `no_blocks` is renamed. It held a HEIGHT (max block + 1), not a
-- count, which made it the one `no_*` column in the schema that was
-- not one -- every other (no_inputs, no_transactions, no_logs,
-- no_addresses) is a genuine count. The two coincide only for a
-- keyspace starting at block 0, so the ambiguity was invisible until
-- v3 started backfilling ranges.
--
-- lowest_block is new, and is what makes the range self-describing:
-- a partial keyspace can now say what it covers instead of implying
-- a height it does not reach.
--
-- The row describes THIS keyspace and nothing else. v2 also carried
-- timestamp_transform and no_blocks_transform, so the derived
-- keyspace could report how far the RAW one had got -- a second
-- keyspace's fact, copied, and therefore able to go stale. In v3 raw
-- and derived advance together, so there is no lag to record;
-- anything that wants to compare them reads both rows, which it can,
-- since it knows both keyspace names.
CREATE TABLE IF NOT EXISTS summary_statistics (
    id int,
    timestamp bigint,                       -- was int: unix seconds, 2038 cliff
    lowest_block bigint,                    -- the range this keyspace covers
    highest_block bigint,                   -- was no_blocks, which was a height
    no_transactions bigint,
    no_addresses bigint,
    no_address_relations bigint,
    PRIMARY KEY (id)
)
    WITH caching = {'keys':'ALL','rows_per_partition':'ALL'}
    AND compaction = {'class':'SizeTieredCompactionStrategy'};
