ALTER TABLE configuration ADD schema_version varint;

CREATE TABLE IF NOT EXISTS token_exchange_rates (
    asset text,
    block_id int,
    fiat_values list<float>,
    PRIMARY KEY (asset, block_id)
) WITH CLUSTERING ORDER BY (block_id DESC);
