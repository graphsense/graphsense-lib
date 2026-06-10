ALTER TABLE configuration ADD schema_version varint;

CREATE TABLE IF NOT EXISTS fresh_address_cluster (
    address_id_group int,
    address_id int,
    cluster_id int,
    PRIMARY KEY (address_id_group, address_id)
);

CREATE TABLE IF NOT EXISTS fresh_cluster_addresses (
    cluster_id_group int,
    cluster_id int,
    address_id int,
    PRIMARY KEY (cluster_id_group, cluster_id, address_id)
)
