"""The v2/v3 comparison.

The risk this file guards is agreement that was never established: a harness
that normalises too much reports "identical" for backends that differ. So the
tests are as much about what is NOT flattened as what is.
"""

from decimal import Decimal

from graphsense_v3 import compare

LTC_P2PKH = "LLcHNPNWE7s6FfLzkt4fD8kJPbsK1V8pyT"
BTC_VERSIONED = "12PL7B4g9Td2zreqak5Mw7gYBPW2vmsiUj"


def test_the_stale_lakes_btc_version_byte_is_normalised_away() -> None:
    """Same hash160, LTC's 0x30 against BTC's 0x00. The lake predates the
    2026-06-15 P2PK fix, so v3 reads the second where production has the first,
    on ~28% of early-chain addresses."""
    assert compare.reversion_address("ltc", BTC_VERSIONED) == LTC_P2PKH
    assert compare.reversion_address("ltc", LTC_P2PKH) == LTC_P2PKH


def test_reversioning_is_a_no_op_for_the_chain_that_owns_the_byte() -> None:
    """The same string is CORRECT on btc; rewriting it there would invent a
    difference rather than remove one."""
    assert compare.reversion_address("btc", BTC_VERSIONED) == BTC_VERSIONED


def test_non_base58check_strings_pass_through_untouched() -> None:
    """A currency ticker, a bech32 address and a hash are not addresses to
    re-version, and mangling them would corrupt real comparisons."""
    for value in ("LTC", "ltc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "", "abc"):
        assert compare.reversion_address("ltc", value) == value


def test_a_p2sh_address_is_not_rewritten_to_p2pkh() -> None:
    """P2SH is ALSO one version byte plus a 20-byte hash, so a length test
    would silently turn a valid P2SH address into a valid, different P2PKH one.
    Only another network's P2PKH byte is rewritten."""
    import hashlib

    for version in (0x32, 0x05):  # LTC P2SH, and the legacy 3-prefix form
        body = bytes([version]) + b"\x11" * 20
        digest = hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
        p2sh = compare._b58encode(body + digest)
        assert compare.reversion_address("ltc", p2sh) == p2sh


def test_surrogate_ids_are_excluded_with_a_reason() -> None:
    """They cannot agree and neither is wrong; comparing them would report a
    difference on every single call."""
    left = {"address_id": 308666, "no_incoming_txs": 3}
    right = {"address_id": 99, "no_incoming_txs": 3}
    assert compare.diff(left, right, "ltc") == []
    assert "address_id" in compare.IGNORED_FIELDS


def test_tx_id_is_excluded_but_the_hash_is_not() -> None:
    """v2's tx_id is dense, v3's is (block_id << 32) + index. The HASH is the
    comparable identity, so a differing hash must still be reported."""
    left = {"tx_id": 5, "tx_hash": "aa"}
    right = {"tx_id": 423114408198144, "tx_hash": "bb"}
    differences = compare.diff(left, right, "ltc")
    assert [d.path for d in differences] == ["$.tx_hash"]


def test_a_decimal_and_an_int_are_the_same_balance() -> None:
    assert compare.diff({"v": Decimal("100")}, {"v": 100}, "ltc") == []
    assert compare.diff({"v": 1.0}, {"v": 1}, "ltc") == []


def test_bytes_compare_as_hex_whatever_the_driver_returned() -> None:
    assert compare.diff({"h": b"\xaa\xbb"}, {"h": bytearray(b"\xaa\xbb")}, "ltc") == []


def test_a_real_difference_survives_every_normalisation() -> None:
    """The point of the whole file: normalising must not swallow a wrong
    balance."""
    differences = compare.diff({"balance": 100}, {"balance": 101}, "ltc")
    assert len(differences) == 1
    assert differences[0].path == "$.balance"


def test_lists_are_compared_positionally() -> None:
    """Order is part of the answer for a transaction listing -- sorting here
    would hide a paging bug, which is exactly what this harness exists to
    find."""
    left = {"txs": [{"tx_hash": "aa"}, {"tx_hash": "bb"}]}
    right = {"txs": [{"tx_hash": "bb"}, {"tx_hash": "aa"}]}
    differences = compare.diff(left, right, "ltc")
    assert len(differences) == 2


def test_a_length_mismatch_is_reported_once_not_per_element() -> None:
    left = {"txs": [1, 2, 3]}
    right = {"txs": [1]}
    differences = compare.diff(left, right, "ltc")
    assert len(differences) == 1
    assert "3 items" in str(differences[0])


def test_a_field_present_on_only_one_side_is_a_difference() -> None:
    """Not silently skipped: a missing field is how a backend quietly fails to
    populate something."""
    differences = compare.diff({"a": 1}, {"a": 1, "b": 2}, "ltc")
    assert [d.path for d in differences] == ["$.b"]


def test_the_report_states_what_it_did_not_compare() -> None:
    """ "These agree" means nothing without knowing what was excluded."""
    reports = [
        compare.compare(
            "get_address",
            {"address_id": 1, "balance": 5},
            {"address_id": 2, "balance": 5},
            "ltc",
        )
    ]
    text = compare.report(reports)
    assert "1 calls, 0 with differences" in text
    assert "NOT COMPARED" in text
    assert "address_id" in text


def test_the_report_lists_each_difference() -> None:
    reports = [compare.compare("get_address", {"balance": 5}, {"balance": 6}, "ltc")]
    text = compare.report(reports)
    assert "DIFF" in text
    assert "$.balance" in text
    assert "1 calls, 1 with differences" in text


def test_summarise_counts_agreements() -> None:
    reports = [
        compare.compare("a", {"x": 1}, {"x": 1}, "ltc"),
        compare.compare("b", {"x": 1}, {"x": 2}, "ltc"),
    ]
    assert compare.summarise(reports) == "1/2 calls agree"


def test_range_dependent_statistics_are_excluded_with_a_reason() -> None:
    """v2 is kept current by the delta updater while a v3 keyspace is a
    snapshot, so these count different spans of chain."""
    left = {"no_blocks": 3171805, "no_txs": 413136629, "balance": 5}
    right = {"no_blocks": 3171362, "no_txs": 413004464, "balance": 5}
    assert compare.diff(left, right, "ltc") == []


def test_timestamp_is_never_ignored_by_name() -> None:
    """It is range-dependent on the statistics response, but it is also on
    every transaction -- and a transaction timestamp mismatch is what exposed
    the direction bug. Ignoring it by name would hide that."""
    assert "timestamp" not in compare.IGNORED_FIELDS
    differences = compare.diff(
        {"timestamp": 1752063841}, {"timestamp": 1752065706}, "ltc"
    )
    assert [d.path for d in differences] == ["$.timestamp"]


def test_run_level_caveats_print_at_the_top_of_the_report() -> None:
    """A caveat that lives only in a CLI flag is one nobody remembers a week
    later; in the report it survives being pasted into a ticket."""
    reports = [compare.compare("get_address", {"a": 1}, {"a": 1}, "ltc")]
    text = compare.report(reports, ["CLUSTERS ARE STUBBED (--stub-clusters)"])
    assert "CLUSTERS ARE STUBBED" in text
    assert text.index("CLUSTERS ARE STUBBED") < text.index("get_address")


def test_a_report_without_caveats_is_unchanged() -> None:
    reports = [compare.compare("get_address", {"a": 1}, {"a": 1}, "ltc")]
    assert "!!" not in compare.report(reports)
