import logging

from ..config import config
from ..utils.slack import send_message_to_slack

logger = logging.getLogger(__name__)


def send_msg_to_topic(topic: str, msg: str):
    slack_topic = config.get_slack_hooks_by_topic(topic)

    if slack_topic is None or slack_topic.hooks is None:
        logger.warning(f"Topic {topic} not properly configured, no msg sent.")
    else:
        for hook in slack_topic.hooks:
            send_message_to_slack(msg, hook)
