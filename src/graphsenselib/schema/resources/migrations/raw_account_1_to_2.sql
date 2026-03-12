CREATE TYPE IF NOT EXISTS access_list_entry (
    address blob,
    storage_keys list<blob>
);
ALTER TABLE transaction ADD max_fee_per_blob_gas bigint;
ALTER TABLE transaction ADD blob_versioned_hashes frozen<list<blob>>;
ALTER TABLE transaction ADD access_list frozen<list<frozen<access_list_entry>>>;
