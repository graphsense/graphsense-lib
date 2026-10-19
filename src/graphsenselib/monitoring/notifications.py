import logging

from ..config import get_config
from ..utils.slack import send_message_to_slack

logger = logging.getLogger(__name__)


def send_msg_to_topic(topic: str, msg: str):
    config = get_config()
    slack_topic = config.get_slack_hooks_by_topic(topic)

    if slack_topic is None or slack_topic.hooks is None:
        logger.warning(f"Topic {topic} not properly configured, no msg sent.")
    else:
        for hook in slack_topic.hooks:
            send_message_to_slack(msg, hook)


def send_msg_to_topic_rate_limited(
    topic: str, msg: str, dedup_key: str, every_seconds: int
) -> bool:
    """Send ``msg`` at most once per ``every_seconds`` for ``dedup_key``.

    The marker lives in Redis (``SET NX EX``), so the limit holds across
    processes -- which is the point: a check that runs every few seconds must
    not turn one stalled chain into a Slack flood. Without Redis
    (``use_redis_locks`` off) every message is sent, as before. Returns whether
    the message was sent.
    """
    config = get_config()
    if config.use_redis_locks and config.redis_url:
        try:
            import redis

            client = redis.from_url(config.redis_url)
            key = f"graphsense:alert:{dedup_key}"
            if not client.set(key, "1", nx=True, ex=every_seconds):
                logger.debug(f"Suppressed repeat notification {key}")
                return False
        except Exception as e:
            # Better a duplicate alert than a lost one.
            logger.warning(f"Could not rate-limit notification {dedup_key}: {e}")
    send_msg_to_topic(topic, msg)
    return True
