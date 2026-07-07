CREATE TABLE IF NOT EXISTS token_exchange_rates (
    asset text,
    date text,
    fiat_values map<text, float>,
    PRIMARY KEY (asset, date)
);
