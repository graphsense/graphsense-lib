ALTER TABLE fresh_cluster_stats ADD (
    no_incoming_txs int,
    no_outgoing_txs int,
    in_degree int,
    out_degree int,
    first_tx_id bigint,
    last_tx_id bigint,
    total_received FROZEN<currency>,
    total_spent FROZEN<currency>,
    total_received_adj FROZEN<currency>,
    total_spent_adj FROZEN<currency>
);
