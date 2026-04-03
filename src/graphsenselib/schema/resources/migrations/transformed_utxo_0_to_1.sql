ALTER TABLE configuration ADD schema_version varint;

CREATE TABLE IF NOT EXISTS fresh_address_cluster (
    address_id int PRIMARY KEY,
    cluster_id int
);

CREATE TABLE IF NOT EXISTS fresh_cluster_addresses (
    cluster_id int,
    address_id int,
    PRIMARY KEY (cluster_id, address_id)
)
