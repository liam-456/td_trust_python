#!/usr/bin/env python3

# Standard
import argparse
import json
from time import sleep

# Third party
import stomp

# Internal
from util import trust
from util.area_config import NAMED_AREAS

# Argument parser setup
parser = argparse.ArgumentParser()
parser.add_argument(
    "-d", "--durable", action='store_true',
    help="Request a durable subscription. Note README before trying this."
)

action = parser.add_mutually_exclusive_group(required=False)
action.add_argument('--td', action='store_true', help='Show messages from TD feed (include selected area)', default=True)
action.add_argument('--trust', action='store_true', help='Show messages from TRUST feed')
action.add_argument('--tdSQL', action='store_true', help='Write TD messages to MySQL database')

parser.add_argument("-a",
    "--area",
    type=str,
    help="Filter TD messages by named area",
    default="all"
)

args = parser.parse_args()

# Validate named area
named_area = args.area.lower()
if named_area not in NAMED_AREAS:
    print(f"Warning: Unknown named area '{args.namedArea}'. Defaulting to 'all'.")
    named_area = "all"

# Import appropriate TD module and set area filter

if args.tdTotSM:
    from util import tdSQL as td
    td.set_named_area(named_area)
    td.create_table()  # ✅ Only tdSQL has this
else:
    from util import td
    td.set_named_area(named_area)


# Listener class
class Listener(stomp.ConnectionListener):
    _mq: stomp.Connection

    def __init__(self, mq: stomp.Connection, durable=False):
        self._mq = mq
        self.is_durable = durable

    def on_message(self, frame):
        headers, message_raw = frame.headers, frame.body
        parsed_body = json.loads(message_raw)

        if self.is_durable:
            self._mq.ack(id=headers["ack"], subscription=headers["subscription"])

        if "TRAIN_MVT_" in headers["destination"]:
            trust.print_trust_frame(parsed_body)
        elif "TD_" in headers["destination"]:
            td.print_td_frame(parsed_body)
        else:
            print("Unknown destination: ", headers["destination"])

    def on_error(self, frame):
        print('Received an error:', frame.body)

    def on_disconnected(self):
        print('Disconnected from feed.')

# Main execution
if __name__ == "__main__":
    with open("secrets.json") as f:
        feed_username, feed_password = json.load(f)

    connection = stomp.Connection(
        [("publicdatafeeds.networkrail.co.uk", 61618)],
        keepalive=True,
        heartbeats=(5000, 5000)
    )
    connection.set_listener('', Listener(connection, durable=args.durable))

    connect_headers = {
        "username": feed_username,
        "passcode": feed_password,
        "wait": True,
    }

    if args.durable:
        connect_headers["client-id"] = feed_username

    connection.connect(**connect_headers)

    # Determine topic
    topic = None
    if args.trust:
        topic = "/topic/TRAIN_MVT_ALL_TOC"
    elif args.td or args.tdTotSM:
        topic = "/topic/TD_ALL_SIG_AREA"

    # Subscription headers
    subscribe_headers = {
        "destination": topic,
        "id": 1,
    }

    if args.durable:
        subscribe_headers.update({
            "activemq.subscriptionName": feed_username + topic,
            "ack": "client-individual"
        })
    else:
        subscribe_headers["ack"] = "auto"

    connection.subscribe(**subscribe_headers)

    while connection.is_connected():
        sleep(1)
