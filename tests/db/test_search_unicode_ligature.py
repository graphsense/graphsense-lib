"""Search must survive unicode ligatures pasted from PDFs.

PDF typography renders the letter pair "ff" as the single ligature glyph
U+FB00 ("ﬀ"), so hashes copy-pasted from reports may contain it. U+FB00 is
the only unicode codepoint whose ``str.upper()`` expands to pure hex digits,
which let it slip through ``is_hexadecimal`` and crash ``int(s, 16)`` in
``create_upper_bound`` with an unhandled ValueError (prod 500).

Desired behavior: ``casefold()`` maps the ligature back to "ff" so the
search *works*, while ``is_hexadecimal`` is ascii-strict so nothing
non-ascii can ever reach the hex parsing again.
"""

import asyncio

import pytest

from graphsenselib.db.asynchronous.cassandra import Cassandra, is_hexadecimal

LIGATURE_TX = "d2b79bc0e7001bdfc66cf6a80f5ﬀd1d2702097b725fca2bad5ea0c0e31c020f"
FOLDED_TX = "d2b79bc0e7001bdfc66cf6a80f5ffd1d2702097b725fca2bad5ea0c0e31c020f"


class TestIsHexadecimalStrict:
    def test_rejects_ff_ligature(self):
        assert is_hexadecimal(LIGATURE_TX) is False

    def test_accepts_lower_upper_mixed(self):
        assert is_hexadecimal(FOLDED_TX) is True
        assert is_hexadecimal(FOLDED_TX.upper()) is True
        assert is_hexadecimal("0xDeadBeef") is True

    def test_rejects_non_hex(self):
        assert is_hexadecimal("xyz") is False


class _EmptyResult:
    current_rows = []

    def is_empty(self):
        return True


class _FakeDb:
    """Minimal stand-in for the bits list_matching_* touches."""

    def __init__(self, tx_prefix_length=5, address_prefix_length=5):
        self._lengths = {"address": address_prefix_length, "tx": tx_prefix_length}
        self.captured = []

    def get_prefix_lengths(self, currency):
        return self._lengths

    def scrub_prefix(self, currency, expression):
        return expression[2:] if expression.startswith("0x") else expression

    async def execute_async(self, currency, kind, query, params):
        self.captured.append((query, params))
        return _EmptyResult()


def _run(coro):
    return asyncio.run(coro)


class TestListMatchingTxsLigature:
    @pytest.mark.parametrize("currency", ["btc", "eth"])
    def test_ligature_matches_folded_query(self, currency):
        """Ligature input must produce the same DB query as its ascii form."""
        db_lig, db_ascii = _FakeDb(), _FakeDb()
        _run(
            Cassandra.list_matching_txs(
                db_lig, currency, LIGATURE_TX, 10, include_sub_tx_identifiers=False
            )
        )
        _run(
            Cassandra.list_matching_txs(
                db_ascii, currency, FOLDED_TX, 10, include_sub_tx_identifiers=False
            )
        )
        assert db_lig.captured == db_ascii.captured
        assert len(db_lig.captured) == 1

    def test_uppercase_utxo_prefix_folded(self):
        """Uppercase hex input must query the lowercase-stored tx_prefix."""
        db = _FakeDb()
        _run(
            Cassandra.list_matching_txs(
                db, "btc", FOLDED_TX.upper(), 10, include_sub_tx_identifiers=False
            )
        )
        assert db.captured[0][1][0] == FOLDED_TX[:5]


class TestListMatchingAddressesLigature:
    def test_eth_ligature_matches_folded_query(self):
        addr_lig = "0xa5c3a51e9bﬀdeadbeef"
        addr_ascii = "0xa5c3a51e9bffdeadbeef"
        db_lig, db_ascii = _FakeDb(), _FakeDb()
        _run(Cassandra.list_matching_addresses(db_lig, "eth", addr_lig))
        _run(Cassandra.list_matching_addresses(db_ascii, "eth", addr_ascii))
        assert db_lig.captured == db_ascii.captured
        assert len(db_lig.captured) == 1

    def test_base58_not_folded(self):
        """Base58 is case-sensitive; UTXO expressions must stay as typed."""
        db = _FakeDb()
        _run(Cassandra.list_matching_addresses(db, "btc", "1BoatSLRHt"))
        assert db.captured[0][1][1] == "1BoatSLRHt"
