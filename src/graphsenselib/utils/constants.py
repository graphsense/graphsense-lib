TRON_DUMMY_REPLACEMENT_ADDRESS = (
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00ikna"
)

NULL_ADDRESS_ACCOUNT_BYTES = b"\x00" * 20


def replace_tron_dummy_address_with_valid_null_address(
    address: bytes, replace_empty: bool = False
) -> bytes:
    if address == TRON_DUMMY_REPLACEMENT_ADDRESS or (address == b"" and replace_empty):
        return NULL_ADDRESS_ACCOUNT_BYTES
    return address


# Fresh-clustering public id space.
#
# Fresh cluster ids are stored raw in the fresh_* Cassandra tables
# (cluster_id == min(address_id) of the cluster), which numerically collides
# with legacy cluster ids. Everywhere outside those tables — REST responses,
# incoming entity ids, the tagstore address_cluster_mapping_v2 — a fresh
# cluster id is shifted by this offset so any id is self-describing:
# id >= offset -> fresh (raw id = id - offset), id < offset -> legacy.
# The offset must stay above every raw id ever handed out: cluster ids are
# derived from address ids (root == min address id), and BTC is already at
# ~0.7 * 2**32 unique addresses in 2026, so 2**32 would collide within its
# remaining headroom. 2**33 doubles the runway while shifted ids stay far
# below the 2**53 JS-safe-integer limit.
FRESH_CLUSTER_ID_OFFSET = 2**33


def is_fresh_cluster_id(entity_id: int) -> bool:
    """True if the (public) entity id addresses the fresh cluster id space."""
    return entity_id >= FRESH_CLUSTER_ID_OFFSET


def to_public_fresh_cluster_id(raw_cluster_id: int) -> int:
    """Raw fresh cluster id (== root/min address id) -> public API id."""
    return raw_cluster_id + FRESH_CLUSTER_ID_OFFSET


def to_raw_fresh_cluster_id(entity_id: int) -> int:
    """Public fresh entity id -> raw cluster id in the fresh_* tables."""
    return entity_id - FRESH_CLUSTER_ID_OFFSET


def is_representable_entity_id(entity_id: int) -> bool:
    """True if a public entity id can exist at all.

    Legacy ids are int32-bound (Cassandra ``cluster_id``/``address_id`` and
    the tagstore mapping columns are 32-bit), and a fresh id unshifts to a
    raw id (min address id) that is int32-bound too. Ids in the gap between
    the legacy ceiling and the offset, beyond the fresh range, or negative
    address no row in any store — treat them like absent ids instead of
    letting them reach an int32/int4 bind.
    """
    return (0 <= entity_id < 2**31) or (
        FRESH_CLUSTER_ID_OFFSET <= entity_id < FRESH_CLUSTER_ID_OFFSET + 2**31
    )
