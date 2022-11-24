import json
from typing import Dict

import requests


def send_message_to_slack(msg: str, webhook: str):
    return send_payload_to_slack({"message": msg}, webhook)


def send_payload_to_slack(payload: Dict, webhook: str):
    """Send a payload to slack via a webhook

    Args:
        payload (dict): is teh
        webhook (str): Webhook url.

    Returns:
        HTTP response code, i.e. <Response [503]>
    """

    return requests.post(webhook, json.dumps(payload))
