from unittest.mock import MagicMock, patch

from graphsenselib.monitoring import notifications


def _config(redis):
    cfg = MagicMock(use_redis_locks=redis, redis_url="redis://x" if redis else None)
    return cfg


def test_without_redis_every_message_is_sent():
    with (
        patch.object(notifications, "get_config", return_value=_config(False)),
        patch.object(notifications, "send_msg_to_topic") as send,
    ):
        assert notifications.send_msg_to_topic_rate_limited("t", "m", "k", 60)
        assert notifications.send_msg_to_topic_rate_limited("t", "m", "k", 60)
    assert send.call_count == 2


def test_repeat_within_interval_is_suppressed():
    client = MagicMock()
    client.set.side_effect = [True, None]
    with (
        patch.object(notifications, "get_config", return_value=_config(True)),
        patch.object(notifications, "send_msg_to_topic") as send,
        patch("redis.from_url", return_value=client),
    ):
        assert notifications.send_msg_to_topic_rate_limited("t", "m", "k", 60)
        assert not notifications.send_msg_to_topic_rate_limited("t", "m", "k", 60)
    send.assert_called_once_with("t", "m")
    client.set.assert_called_with("graphsense:alert:k", "1", nx=True, ex=60)


def test_redis_error_still_sends():
    with (
        patch.object(notifications, "get_config", return_value=_config(True)),
        patch.object(notifications, "send_msg_to_topic") as send,
        patch("redis.from_url", side_effect=ConnectionError("down")),
    ):
        assert notifications.send_msg_to_topic_rate_limited("t", "m", "k", 60)
    send.assert_called_once()
