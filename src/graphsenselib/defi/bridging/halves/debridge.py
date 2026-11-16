"""deBridge half bridges (defi/bridging/delegated.py). Not read yet: no event
of deBridge counts, so a tx gets none."""

from typing import List

from graphsenselib.defi.bridging.half_common import Log
from graphsenselib.defi.bridging.models import Bridge


def read(network: str, logs: List[Log]) -> List[Bridge]:
    """A tx's deBridge half bridges on network, in log order."""
    return []
