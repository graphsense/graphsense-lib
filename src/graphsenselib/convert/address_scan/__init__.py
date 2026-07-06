"""Scan text/SQL files (and compressed containers) for cryptocurrency addresses.

Extraction is deliberately permissive; the real filter is checksum validation
(reusing :mod:`graphsenselib.utils.address`), which removes the many
address-shaped false positives found in database dumps (hashes, session ids,
base64 blobs, filenames).
"""
