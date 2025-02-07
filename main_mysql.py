#!/usr/bin/env python3

# Standard
import argparse
import json
from time import sleep
import mysql.connector  # MySQL connector for Python

# Third party
import stomp

# Internal
from util import trust
from util import td  # Default to td.py

# Argument parser setup
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--durable", action='store_true',
                    help="Request a durable subscription. Note README before trying this.")
action = parser.add_mutually_exclusive_group(required=False)
action.add_argument('--td', action='store_true', help='Show messages from TD feed', default=True)
action.add_argument('--trust', action='store_true', help='Show messages from TRUST feed')
action.add_argument('--tdQ1Q3', action='store_true', help='Use filtered TD feed for Q1 and Q3')

args = parser.parse_args()

# Import tdQ1Q3.py if --tdQ1Q3 is used
if args.tdQ1Q3:
    from util import tdQ1Q3 as td  # Override td module

# Database connection function
def create_db_connection():
    return mysql.connector.connect(
        host="localhost",  # Change to your DB server
        user="liam",  # Your MySQL username
        password="password",  # Your MySQL password
        database="td_database"  # The database where you want to insert data
    )

# Insert the parsed message into the MySQL database
def insert_into_db(parsed_body, topic):
    connection = create_db_connection()
    cursor = connection.cursor()

    # Construct an SQL query to insert message data
    sql = """
    INSERT INTO messages (topic, message_data, timestamp)
    VALUES (%s, %s, NOW())
    """
    # Here, we store the topic and the parsed message (could be JSON or any format)
    cursor.execute(sql, (topic, json.dumps(parsed_body)))
    connection.commit()
    cursor.close()
    connection.close()

class Listener(stomp.ConnectionListener):
    _mq: stomp.Connection

    def __init__(self, mq: stomp.Connection, durable=False):
        self._mq = mq
        self.is_durable = durable

    def on_message(self, frame):
        headers, message_raw = frame.headers, frame.body
        parsed_body = json.loads(message_raw)

        if self.is_durable:
            # Acknowledging messages is important in client-individual mode
            self._mq.ack(id=headers["ack"], subscription=headers["subscription"])

        if "TRAIN_MVT_" in headers["destination"]:
            trust.print_trust_frame(parsed_body)
        elif "TD_" in headers["destination"]:
            td.print_td_frame(parsed_body)  # Uses the correct module
        else:
            print("Unknown destination: ", headers["destination"])

        # Insert the message data into MySQL
        insert_into_db(parsed_body, headers["destination"])

    def on_error(self, frame):
        print('received an error {}'.format(frame.body))

    def on_disconnected(self):
        print('disconnected')


if __name__ == "__main__":
    with open("secrets.json") as f:
        feed_username, feed_password = json.load(f)

    # STOMP connection setup
    connection = stomp.Connection([('publicdatafeeds.networkrail.co.uk', 61618)], keepalive=True, heartbeats=(5000, 5000))
    connection.set_listener('', Listener(connection, durable=args.durable))

    connect_headers = {
        "username": feed_username,
        "passcode": feed_password,
        "wait": True,
    }
    if args.durable:
        connect_headers["client-id"] = feed_username

    connection.connect(**connect_headers)

    # Determine topic to subscribe
    topic = None
    if args.trust:
        topic = "/topic/TRAIN_MVT_ALL_TOC"
    elif args.td or args.tdQ1Q3:
        topic = "/topic/TD_ALL_SIG_AREA"

    # Subscription
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
