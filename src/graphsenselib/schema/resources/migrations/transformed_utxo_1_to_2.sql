CREATE TABLE IF NOT EXISTS fresh_cluster_stats (
    cluster_id_group int,
    cluster_id int,
    no_addresses bigint,
    min_address_id int,
    PRIMARY KEY (cluster_id_group, cluster_id)
);
