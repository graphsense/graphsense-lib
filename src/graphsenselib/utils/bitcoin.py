"""Bitcoin protocol helpers (pure-Python, no Spark dependency)."""

from typing import Iterable, Optional

# BIP125: a tx is RBF-signaling iff any input has sequence < 0xfffffffe.
RBF_SENTINEL = 0xFFFFFFFE


def is_rbf_signaled(sequences: Iterable[Optional[int]]) -> bool:
    """Return True iff any sequence value indicates BIP125 opt-in RBF.

    `None` values (e.g. from coinbase / shielded inputs in the Delta source)
    are treated as final (not RBF).
    """
    return any(s is not None and s < RBF_SENTINEL for s in sequences)
