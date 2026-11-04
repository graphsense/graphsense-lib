"""Reporting a tag that already exists succeeds with the first report's id.

The duplicate used to come back as ``None``, which failed the REST response
model (``UserTagReportResponse.id: str``) with a 500. A duplicate has nothing
new to review, so it must not notify Slack either.

DB-free: the real TagsService.report_tag over a fake tagstore.
"""

import asyncio
import logging
from types import SimpleNamespace

import pytest

from graphsenselib.config.config import SlackTopic
from graphsenselib.db.asynchronous.services import tags_service as tags_service_mod
from graphsenselib.db.asynchronous.services.tags_service import (
    MockConceptProtocol,
    TagsService,
)
from graphsenselib.errors import BadUserInputException
from graphsenselib.tagstore.db import (
    TagAlreadyExistsException,
    UnknownConceptsException,
)
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
    privacy_preserving_tag_notifications=True,
    slack_info_hook=SlackTopic(hooks=["http://127.0.0.1:1/hook"]),
)


class FakeTagstore:
    def __init__(self, result):
        self.result = result
        self.tags = []

    async def add_user_reported_tag(self, tag, acl_group):
        self.tags.append(tag)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _report(tagstore, monkeypatch, config=CONFIG, tag=TAG):
    sent = []
    monkeypatch.setattr(
        tags_service_mod, "send_message_to_slack", lambda msg, h: sent.append(msg)
    )
    service = TagsService(
        None, tagstore, MockConceptProtocol(), logging.getLogger(__name__)
    )
    return asyncio.run(service.report_tag(tag, config, "public")), sent


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


@pytest.mark.parametrize("private", [True, False])
def test_privacy_setting_decides_what_slack_gets(monkeypatch, private):
    """The config field is `privacy_preserving_tag_notifications` (plural);
    the service read the singular name and always fell back to True."""
    config = SimpleNamespace(**{**vars(CONFIG)})
    config.privacy_preserving_tag_notifications = private
    tag = TAG.model_copy(update={"label": "a secret label"})
    _, sent = _report(FakeTagstore("new-uuid"), monkeypatch, config, tag)

    assert len(sent) == 1
    assert ("a secret label" in sent[0]) is not private
    assert "new-uuid" in sent[0] or not private


def test_concepts_reach_the_tagstore(monkeypatch):
    store = FakeTagstore("new-uuid")
    tag = TAG.model_copy(update={"concepts": ["hot_wallet"]})
    _report(store, monkeypatch, tag=tag)

    assert store.tags[0].concepts == ["hot_wallet"]


def test_unknown_concept_is_a_bad_input(monkeypatch):
    with pytest.raises(BadUserInputException, match="no_such_concept"):
        _report(
            FakeTagstore(UnknownConceptsException(["no_such_concept"])), monkeypatch
        )
