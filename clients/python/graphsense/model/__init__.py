"""
Backward compatibility alias for graphsense.models.

v5 used graphsense.model, v7 uses graphsense.models.
This module provides backward compatibility by re-exporting from models.
"""

# Re-export all models for backward compatibility
from graphsense.models import *  # noqa: F401, F403
from graphsense.models import (
    Rate,
    Rates,
    Values,
    Address,
    AddressTag,
    AddressTags,
    Block,
    Entity,
    # Add more as needed
)
