"""TRX address tag lookups also match TRX tags stored in their 0x form.

DB-free: the real TagsService over a fake tagstore that records the call.
"""

import asyncio
import logging

from graphsenselib.db.asynchronous.services.tags_service import (
    MockConceptProtocol,
    TagsService,
)

TRON = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
EVM = "0xa614f803b6fd780986a42c78ec9c7f77e6ded13c"


class RecordingTagstore:
    def __init__(self):
        self.calls = []

    async def get_tags_by_subjectid(
        self, address, offset, limit, groups, scoped_identifier=None
    ):
        self.calls.append((address, scoped_identifier))
        return []


def _lookup(currency, address):
    tagstore = RecordingTagstore()
    service = TagsService(
        None, tagstore, MockConceptProtocol(), logging.getLogger(__name__)
    )
    asyncio.run(service.list_tags_by_address_raw(currency, address, ["public"]))
    return tagstore.calls


def test_trx_lookup_scopes_evm_form_to_trx():
    assert _lookup("trx", TRON) == [(TRON, (EVM, "TRX"))]


def test_eth_lookup_has_no_scoped_identifier():
    assert _lookup("eth", EVM) == [(EVM, None)]


def test_invalid_trx_address_has_no_scoped_identifier():
    assert _lookup("trx", "Tnotavalidaddress") == [("Tnotavalidaddress", None)]
