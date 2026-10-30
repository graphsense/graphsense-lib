"""Reporting a tag that already exists succeeds with the first report's id.

The duplicate used to come back as ``None``, which failed the REST response
model (``UserTagReportResponse.id: str``) with a 500. A duplicate has nothing
new to review, so it must not notify Slack either.

DB-free: the real TagsService.report_tag over a fake tagstore.
"""

import asyncio
import logging
from types import SimpleNamespace

from graphsenselib.config.config import SlackTopic
from graphsenselib.db.asynchronous.services import tags_service as tags_service_mod
from graphsenselib.db.asynchronous.services.tags_service import (
    MockConceptProtocol,
    TagsService,
)
from graphsenselib.tagstore.db import TagAlreadyExistsException
from graphsenselib.tagstore.db.queries import UserReportedAddressTag

TAG = UserReportedAddressTag(
    address="1Archive1n2C579dMsAu3iC6tWzuQJz8dN",
    network="BTC",
    actor=None,
    label="test",
    description="",
)
CONFIG = SimpleNamespace(
    enable_user_tag_reporting=True,
    privacy_preserving_tag_notification=True,
    slack_info_hook=SlackTopic(hooks=["http://127.0.0.1:1/hook"]),
)


class FakeTagstore:
    def __init__(self, result):
        self.result = result

    async def add_user_reported_tag(self, tag, acl_group):
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _report(tagstore, monkeypatch):
    sent = []
    monkeypatch.setattr(
        tags_service_mod, "send_message_to_slack", lambda msg, h: sent.append(msg)
    )
    service = TagsService(
        None, tagstore, MockConceptProtocol(), logging.getLogger(__name__)
    )
    return asyncio.run(service.report_tag(TAG, CONFIG, "public")), sent


def test_duplicate_returns_existing_id_without_notification(monkeypatch):
    report_id, sent = _report(
        FakeTagstore(TagAlreadyExistsException("existing-uuid")), monkeypatch
    )

    assert report_id == "existing-uuid"
    assert sent == []


def test_new_tag_returns_id_and_notifies(monkeypatch):
    report_id, sent = _report(FakeTagstore("new-uuid"), monkeypatch)

    assert report_id == "new-uuid"
    assert len(sent) == 1
