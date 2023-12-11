import json
import logging
import os
import sys
from contextlib import contextmanager
from typing import Dict, List

import click
import requests

from .errorhandling import get_exception_digest

logger = logging.getLogger(__name__)


class ClickSlackErrorNotificationContext:
    def __init__(self, webhooks: List[str]):
        self.hooks = webhooks

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        e = sys.exc_info()[1]
        if isinstance(e, click.exceptions.Exit) and (
            e.exit_code == 0 or e.exit_code == 911
        ):
            # this is how click communicated all is well
            return
        if isinstance(e, click.exceptions.ClickException):
            return
        if isinstance(e, SystemExit) and (e.code > 10):
            # Exit code 911 is used when lockfile is already used in
            # delta updates we don't what notifications for that
            # exit codes are chosen such that only below 10 are critical
            return
        if isinstance(e, KeyboardInterrupt):
            return
        if e is not None:
            for hook in self.hooks:
                try:
                    ret = send_exception_digest_to_slack(e, hook)
                    if ret.status_code != 200:
                        logger.error(
                            f"Failed to send exception detail to slack - {e}. "
                            f"Got status code {ret}, url wrong?"
                        )
                except Exception as e:
                    logger.error(f"Failed to send exception detail to slack - {e}")
        return False


@contextmanager
def on_exception_notify_slack(webhooks: List[str]):
    try:
        yield
    except Exception as e:
        for hook in webhooks:
            try:
                ret = send_exception_digest_to_slack(e, hook)
                if ret.status_code != 200:
                    logger.error(
                        f"Failed to send exception detail to slack - {e}. "
                        f"Got status code {ret}, url wrong?"
                    )
            except Exception as e:
                logger.error(f"Failed to send exception detail to slack - {e}")
        raise e


def send_exception_digest_to_slack(ex, webhook: str):
    machine = os.uname()
    machine_str = f"{machine.nodename} ({machine.sysname})"
    return send_message_to_slack(
        f"{get_exception_digest(ex)} \n"
        f"in {' '.join(sys.argv)}.\n"
        f"on {machine_str}\n"
        "Check the logs for more detail.",
        webhook,
    )


def send_message_to_slack(msg: str, webhook: str):
    return send_payload_to_slack({"text": msg}, webhook)


def send_payload_to_slack(payload: Dict, webhook: str):
    """Send a payload to slack via a webhook

    Args:
        payload (dict): is teh
        webhook (str): Webhook url.

    Returns:
        HTTP response code, i.e. <Response [503]>
    """
    headers = {"Content-type": "application/json"}
    return requests.post(webhook, json.dumps(payload), headers=headers)
