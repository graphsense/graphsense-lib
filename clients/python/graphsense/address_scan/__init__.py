# AUTO-GENERATED — DO NOT EDIT.
# Synced from src/graphsenselib/convert/address_scan/__init__.py via
# clients/python/scripts/sync_address_scan.py. Edit the source and re-run
# `make -C clients/python sync-address-scan`.
"""Scan text/SQL files (and compressed containers) for cryptocurrency addresses.

Extraction is deliberately permissive; the real filter is checksum validation
(reusing :mod:`graphsenselib.utils.address`), which removes the many
address-shaped false positives found in database dumps (hashes, session ids,
base64 blobs, filenames).
"""
