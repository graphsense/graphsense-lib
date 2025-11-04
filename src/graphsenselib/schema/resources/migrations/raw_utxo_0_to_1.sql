ALTER TYPE tx_input_output ADD script_hex blob;
ALTER TYPE tx_input_output ADD txinwitness list<blob>;
ALTER TABLE configuration ADD schema_version varint;
