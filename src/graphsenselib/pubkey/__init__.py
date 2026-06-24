"""Cross-chain pubkey → address lookup pipeline.

Populates ``pubkey.pubkey_by_address`` by extracting signing public keys from
per-chain Delta Lake transactions, intersecting them across chains via a
Delta Lake intermediate, and writing derived addresses for cross-chain
pubkeys to Cassandra. See ``src/graphsenselib/pubkey/job.py`` for the Spark
job and ``src/graphsenselib/pubkey/extract.py`` for the extraction logic.
"""
