CREATE TABLE IF NOT EXISTS fresh_cluster_stats (
    cluster_id int PRIMARY KEY,
    size bigint,
    min_address_id int
);
