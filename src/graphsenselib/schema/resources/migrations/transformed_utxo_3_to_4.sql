DROP TABLE IF EXISTS fresh_address_cluster;

CREATE TABLE fresh_address_cluster (
    address_id_group int,
    address_id int,
    cluster_id int,
    PRIMARY KEY (address_id_group, address_id)
);

DROP TABLE IF EXISTS fresh_cluster_addresses;

CREATE TABLE fresh_cluster_addresses (
    cluster_id_group int,
    cluster_id int,
    address_id int,
    PRIMARY KEY (cluster_id_group, cluster_id, address_id)
);
