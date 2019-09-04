
import os
import json
import requests

SLACK_WEBHOOK = os.environ.get('GCPSPY_SLACK_WEBHOOK') or None


def send_slack_msg(**context):
    """ sends a slack message """
    data = {
        "attachments": [
            {
                "fallback": None,
                "pretext": None,
                "fields": None,
                "color": "#0f9d58",
                "mrkdwn_in": ["fields"]
            },
            {
                "fields": None,
                "color": "#4285f4",
                "mrkdwn_in": ["fields"]
            }
        ]
    }

    requests.post(
        url=SLACK_WEBHOOK,
        data=json.dumps(data),
        headers={'Content-type': 'application/json'}
    )