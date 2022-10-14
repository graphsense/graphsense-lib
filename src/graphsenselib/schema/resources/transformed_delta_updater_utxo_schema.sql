CREATE TABLE dirty_addresses (
    address_prefix text,
    address text,
    PRIMARY KEY (address_prefix, address)
);

CREATE TABLE new_addresses (
    address_prefix text,
    address text,
    address_id int,
    block_id int,
    timestamp int,
    tx_hash blob,
    PRIMARY KEY (address_prefix, address)
);
