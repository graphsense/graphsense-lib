CREATE TABLE delta_updater_history (
    last_synced_block bigint,
    last_synced_block_timestamp timestamp,
    highest_address_id int,
    timestamp timestamp,
    write_new boolean,
    write_dirty boolean,
    runtime_seconds int,
    PRIMARY KEY (last_synced_block)
);

CREATE TABLE delta_updater_status (
    keyspace_name text,
    last_synced_block bigint,
    last_synced_block_timestamp timestamp,
    highest_address_id int,
    timestamp timestamp,
    write_new boolean,
    write_dirty boolean,
    runtime_seconds int,
    PRIMARY KEY (keyspace_name)
);
