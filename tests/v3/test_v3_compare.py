"""The v2/v3 comparison.

The risk this file guards is agreement that was never established: a harness
that normalises too much reports "identical" for backends that differ. So the
tests are as much about what is NOT flattened as what is.
"""

from decimal import Decimal

from graphsense_v3 import compare

LTC_P2PKH = "LLcHNPNWE7s6FfLzkt4fD8kJPbsK1V8pyT"
BTC_VERSIONED = "12PL7B4g9Td2zreqak5Mw7gYBPW2vmsiUj"


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


def test_two_different_paging_tokens_agree_that_there_is_a_next_page() -> None:
    """v2's "49469955:1" and v3's tx_id cursor are both opaque and neither is
    wrong. Comparing the values would report a difference on every paged
    call."""
    left = {"next_page": "49469955:1", "txs": [1]}
    right = {"next_page": "8270462039621668", "txs": [1]}
    assert compare.diff(left, right, "ltc") == []


def test_a_backend_that_never_pages_is_still_caught() -> None:
    """The bug this must not hide: v3 returned None unconditionally, so every
    address looked like it had exactly one page and a caller would never see
    past the first pagesize rows."""
    differences = compare.diff({"next_page": "49469955:1"}, {"next_page": None}, "ltc")
    assert [d.path for d in differences] == ["$.next_page"]


def test_both_sides_ending_a_listing_agree() -> None:
    assert compare.diff({"next_page": None}, {"next_page": None}, "ltc") == []


def test_timing_is_reported_per_call_name_not_per_fixture() -> None:
    """One address is not a measurement; the median over a sample is."""
    reports = []
    for index, (v2, v3) in enumerate([(10.0, 5.0), (20.0, 10.0), (30.0, 15.0)]):
        entry = compare.compare(f"get_address(addr{index})", {"a": 1}, {"a": 1}, "ltc")
        entry.left_ms, entry.right_ms = v2, v3
        reports.append(entry)
    text = compare.report(reports)
    assert "TIMING" in text
    # Median of 10/20/30 against 5/10/15 -> 20.0 vs 10.0, a 0.50x ratio.
    assert "20.0" in text and "10.0" in text and "0.50x" in text


def test_untimed_reports_print_no_timing_section() -> None:
    reports = [compare.compare("get_address", {"a": 1}, {"a": 1}, "ltc")]
    assert "TIMING" not in compare.report(reports)


def test_timing_never_affects_agreement() -> None:
    """A fast wrong answer is still wrong."""
    entry = compare.compare("get_address", {"a": 1}, {"a": 2}, "ltc")
    entry.left_ms, entry.right_ms = 100.0, 1.0
    assert entry.agrees is False


def test_a_field_can_be_excused_on_one_call_without_being_excused_everywhere():
    """`timestamp` is range-dependent on the statistics response and the
    harness's sharpest signal on a transaction -- a mismatch there is what
    exposed the direction bug. IGNORED_FIELDS matches by name everywhere and
    cannot say that."""
    stats = compare.compare(
        "get_currency_statistics",
        {"timestamp": 1788866891},
        {"timestamp": 1657257464},
        "eth",
    )
    assert stats.agrees
    assert "timestamp" in stats.ignored

    tx = compare.compare(
        "get_tx(abcd)", {"timestamp": 1788866891}, {"timestamp": 1657257464}, "eth"
    )
    assert not tx.agrees
    assert "timestamp" not in tx.ignored


def test_the_reason_for_a_call_scoped_exclusion_says_which_call():
    """The NOT COMPARED block is read without the code next to it, so a reason
    that did not say 'on this call only' would read as a blanket exclusion."""
    text = compare.report(
        [
            compare.compare(
                "get_currency_statistics", {"timestamp": 1}, {"timestamp": 2}, "eth"
            )
        ]
    )
    assert "on get_currency_statistics only" in text


def test_a_length_mismatch_names_the_rows_not_just_the_count():
    """ "68 items" against "72 items" says a page is short without saying which
    rows are missing, and that is the one question such a difference always
    raises -- it is what an over-count on a block listing turns into."""
    left = [{"identifier": "a"}, {"identifier": "b"}]
    right = [{"identifier": "a"}, {"identifier": "b"}, {"identifier": "c"}]
    found = compare.diff({"txs": left}, {"txs": right}, "eth")
    assert len(found) == 1
    assert "identifier=c" in str(found[0].right)
    assert "2 items" in str(found[0].left) and "3 items" in str(found[0].right)


def test_a_length_mismatch_reports_the_DIFFERENCE_not_a_prefix():
    """The two lists usually share a long prefix -- a block listing that
    differs by two rows agrees on the other sixty-six. Showing the first few
    of each side prints the SAME rows twice and buries the ones that differ,
    which is what made the first version of this useless."""
    shared = [{"identifier": f"s{i}"} for i in range(8)]
    found = compare.diff(
        {"txs": shared + [{"identifier": "onlyv2"}]},
        {"txs": shared + [{"identifier": "onlyv3"}, {"identifier": "alsov3"}]},
        "eth",
    )
    left, right = str(found[0].left), str(found[0].right)
    assert "only here: identifier=onlyv2" in left
    assert "identifier=onlyv3" in right and "identifier=alsov3" in right
    assert "s0" not in left and "s0" not in right


def test_a_side_that_is_a_strict_subset_says_so_by_omission():
    """Nothing is unique to the shorter side, so it reports its count alone --
    which is itself the finding: those rows are MISSING, not different."""
    rows = [{"identifier": f"s{i}"} for i in range(3)]
    found = compare.diff({"txs": rows[:1]}, {"txs": rows}, "eth")
    assert str(found[0].left) == "1 items"
    assert "only here" in str(found[0].right)


def test_a_long_length_mismatch_is_sampled_not_dumped():
    """A windowed backend differs by hundreds of rows; the identities are then
    noise. A handful shows WHICH rows, and the count still says how many."""
    left: list = []
    right = [{"tx_hash": f"{i:02x}"} for i in range(50)]
    found = compare.diff({"txs": left}, {"txs": right}, "eth")
    rendered = str(found[0].right)
    assert "50 items" in rendered
    assert f"+{50 - compare.IDENTITY_SAMPLE} more" in rendered


def test_a_list_of_unnameable_items_still_reports_its_length():
    left = [1, 2]
    right = [1, 2, 3]
    found = compare.diff({"xs": left}, {"xs": right}, "eth")
    assert str(found[0].left) == "2 items"
    assert str(found[0].right) == "3 items"
