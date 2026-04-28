ALTER TYPE tx_input_output ADD sequence bigint;
ALTER TABLE transaction ADD version int;
ALTER TABLE transaction ADD lock_time bigint;
