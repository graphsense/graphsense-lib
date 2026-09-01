"""GraphSense backend v3.

A self-contained rewrite of the storage layer: schema definitions, a clean data
access layer, and a Spark backfill. It may *call* into ``graphsenselib`` (codecs,
config, utils) but never modifies it, and nothing in ``graphsenselib`` imports
from here.
"""

__all__ = ["schema"]
