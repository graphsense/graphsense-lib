"""Unit tests for :func:`graphsenselib.pathfinder.verify_structural`.

These exercise the in-spec consistency checks directly against the
dict-shaped spec — no MCP tool, no FastAPI, no backend. The MCP tool
integration tests still cover the build-tool wiring; these cover the
structural-check logic itself in isolation.
"""

from __future__ import annotations

from graphsenselib.pathfinder import verify_structural


def test_clean_spec_produces_no_warnings() -> None:
    spec = {
        "addresses": [
            {"id": "addrA", "starting_point": True},
            {"id": "addrB"},
        ],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "addrA", "b": "addrB", "tx_ids": ["tx1"]}],
    }
    assert verify_structural(spec) == []


def test_empty_spec_produces_no_warnings() -> None:
    """A wholly empty spec is degenerate but not warning-worthy — every
    check needs something to point at."""
    assert verify_structural({}) == []
    assert verify_structural({"addresses": [], "txs": [], "agg_edges": []}) == []


def test_edges_without_txs_warns() -> None:
    spec = {
        "addresses": [{"id": "a"}, {"id": "b"}],
        "txs": [],
        "agg_edges": [{"a": "a", "b": "b"}],
    }
    warnings = verify_structural(spec)
    assert any("no txs were provided" in w for w in warnings), warnings


def test_edge_without_tx_ids_warns_when_txs_present() -> None:
    """When the spec has txs but an edge omits tx_ids, the txs won't
    actually connect to that edge — flag the count."""
    spec = {
        "addresses": [{"id": "a"}, {"id": "b"}],
        "txs": [{"id": "tx1"}],
        "agg_edges": [
            {"a": "a", "b": "b", "tx_ids": ["tx1"]},
            {"a": "a", "b": "b"},  # no tx_ids
        ],
    }
    warnings = verify_structural(spec)
    assert any("1 of 2 agg_edge(s) have no tx_ids" in w for w in warnings), warnings


def test_unknown_tx_in_edge_tx_ids_warns() -> None:
    spec = {
        "addresses": [{"id": "a"}, {"id": "b"}],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "a", "b": "b", "tx_ids": ["tx1", "missing"]}],
    }
    warnings = verify_structural(spec)
    assert any(
        "references tx hash(es) not in `txs`" in w and "missing" in w for w in warnings
    ), warnings


def test_unknown_address_in_edge_endpoint_warns() -> None:
    """The most likely silent failure: a typo in an edge endpoint draws
    to a node that doesn't exist."""
    spec = {
        "addresses": [{"id": "a"}],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "a", "b": "typoB", "tx_ids": ["tx1"]}],
    }
    warnings = verify_structural(spec)
    assert any(
        "reference address(es) not in `addresses`" in w and "typoB" in w
        for w in warnings
    ), warnings


def test_orphan_tx_warns() -> None:
    """A tx in `txs` but not referenced from any edge tx_ids renders as
    a floating node — flag it the same as a stray address."""
    spec = {
        "addresses": [{"id": "a", "starting_point": True}, {"id": "b"}],
        "txs": [{"id": "tx1"}, {"id": "txOrphan"}],
        "agg_edges": [{"a": "a", "b": "b", "tx_ids": ["tx1"]}],
    }
    warnings = verify_structural(spec)
    assert any(
        "not referenced from any agg_edge.tx_ids" in w and "txOrphan" in w
        for w in warnings
    ), warnings


def test_stray_address_warns() -> None:
    """An address in `addresses` that no edge references AND that is
    not a starting_point renders as a floating address node."""
    spec = {
        "addresses": [
            {"id": "a", "starting_point": True},
            {"id": "b"},
            {"id": "stray"},
        ],
        "txs": [{"id": "tx1"}],
        "agg_edges": [{"a": "a", "b": "b", "tx_ids": ["tx1"]}],
    }
    warnings = verify_structural(spec)
    assert any(
        "are not referenced from any agg_edge" in w and "stray" in w for w in warnings
    ), warnings


def test_starting_point_address_with_no_edges_is_not_flagged() -> None:
    """The explicit opt-out: starting_point=True means "I deliberately
    want this as an anchor node, even with no edges yet"."""
    spec = {
        "addresses": [{"id": "a", "starting_point": True}],
        "txs": [],
        "agg_edges": [],
    }
    assert verify_structural(spec) == []


def test_truncation_caps_lists_at_ten() -> None:
    """A spec with many invalid entries must not bloat the warning
    text — cap each list at 10 with a `(+N more)` suffix."""
    spec = {
        "addresses": [{"id": "a", "starting_point": True}],
        "txs": [{"id": f"tx{i}"} for i in range(15)],
        "agg_edges": [],  # so every tx is orphan
    }
    warnings = verify_structural(spec)
    [orphan_w] = [w for w in warnings if "not referenced from any agg_edge.tx_ids" in w]
    assert "tx0" in orphan_w
    assert "tx9" in orphan_w
    assert "tx10" not in orphan_w
    assert "(+5 more)" in orphan_w


def test_missing_id_keys_do_not_crash() -> None:
    """Defensive: items without an `id` key are silently ignored, not
    crashing the structural check. The MCP tool's pydantic model
    enforces `id` at the boundary; this is a belt-and-suspenders test
    so other callers can pass partial specs without surprise."""
    spec = {
        "addresses": [{"network": "btc"}],  # no id
        "txs": [{"network": "btc"}],
        "agg_edges": [],
    }
    # Must not raise; warnings list shape is unspecified for this case
    # (the contract is: don't crash).
    verify_structural(spec)


CHECKSUMMED = "0x4e1773615dFc62A5dDc901b36223F1eAedB8F6Df"


def test_case_variant_duplicate_address_warns() -> None:
    """The same EVM address listed checksummed AND lowercase is one
    address, not two — flag it so the caller fixes the spec (the
    encoder merges them regardless)."""
    spec = {
        "addresses": [
            {"id": CHECKSUMMED, "starting_point": True},
            {"id": CHECKSUMMED.lower()},
        ],
        "txs": [],
        "agg_edges": [],
    }
    warnings = verify_structural(spec)
    assert any("more than once" in w and "case-insensitive" in w for w in warnings), (
        warnings
    )


def test_case_variant_duplicate_tx_warns() -> None:
    tx = "0x" + "AB" * 32
    spec = {
        "addresses": [{"id": "a", "starting_point": True}],
        "txs": [{"id": tx}, {"id": tx.lower()}],
        "agg_edges": [{"a": "a", "b": "a", "tx_ids": [tx]}],
    }
    warnings = verify_structural(spec)
    assert any("same transaction more than once" in w for w in warnings), warnings


def test_same_id_on_different_networks_is_not_a_duplicate() -> None:
    spec = {
        "addresses": [
            {"id": CHECKSUMMED, "network": "eth", "starting_point": True},
            {"id": CHECKSUMMED, "network": "base", "starting_point": True},
        ],
        "txs": [],
        "agg_edges": [],
    }
    assert verify_structural(spec) == []


def test_case_variant_edge_endpoint_is_not_unknown() -> None:
    """An edge endpoint written checksummed must resolve against an
    address listed lowercase (and vice versa) — no spurious
    unknown-address or stray-address warnings."""
    other = "0x" + "ab" * 20
    tx = "0x" + "cd" * 32
    spec = {
        "addresses": [
            {"id": CHECKSUMMED.lower(), "starting_point": True},
            {"id": other},
        ],
        "txs": [{"id": tx.upper().replace("0X", "0x")}],
        "agg_edges": [{"a": CHECKSUMMED, "b": other, "tx_ids": [tx]}],
    }
    assert verify_structural(spec) == []
