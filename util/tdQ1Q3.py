# Python standard
from datetime import datetime

# Third party
import mysql.connector
from mysql.connector import Error
from pytz import timezone

TIMEZONE_LONDON = timezone("Europe/London")

# TD message types
C_BERTH_STEP = "CA"
C_BERTH_CANCEL = "CB"
C_BERTH_INTERPOSE = "CC"
C_HEARTBEAT = "CT"

S_SIGNALLING_UPDATE = "SF"
S_SIGNALLING_REFRESH = "SG"
S_SIGNALLING_REFRESH_FINISHED = "SH"

# Database Configuration (Update with your credentials)
DB_CONFIG = {
    "host": "localhost",  # Change to your MySQL server
    "user": "liam",
    "password": "password",
    "database": "td_database"
}

# Function to create a MySQL connection
def create_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error: {e}")
    return None

# Function to create the table if it does not exist
def create_table():
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS td_messages_Q1_Q3 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME,
            message_type VARCHAR(2),
            area_id VARCHAR(2),
            description VARCHAR(10),
            from_berth VARCHAR(10),
            to_berth VARCHAR(10)
        );
        """)
        conn.commit()
        cursor.close()
        conn.close()

# Function to insert data into the database
def insert_into_db(timestamp, message_type, area_id, description, from_berth, to_berth):
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        query = """
        INSERT INTO td_messages_Q1_Q3 (timestamp, message_type, area_id, description, from_berth, to_berth)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        cursor.execute(query, (timestamp, message_type, area_id, description, from_berth, to_berth))
        conn.commit()
        cursor.close()
        conn.close()

# Function to process and export TD messages
def print_td_frame(parsed_body):
    for outer_message in parsed_body:
        message = list(outer_message.values())[0]
        message_type = message["msg_type"]

        if message_type in [C_BERTH_STEP, C_BERTH_CANCEL, C_BERTH_INTERPOSE]:
            timestamp = int(message["time"]) / 1000
            area_id = message["area_id"]

            if area_id not in ["Q1", "Q3"]:
                continue

            description = message.get("descr", "")
            from_berth = message.get("from", "")
            to_berth = message.get("to", "")

            utc_datetime = datetime.utcfromtimestamp(timestamp)
            uk_datetime = TIMEZONE_LONDON.fromutc(utc_datetime)

            print("{} [{:2}] {:2} {:4} {:>5}->{:5}".format(
                uk_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                message_type, area_id, description, from_berth, to_berth,
            ))

            # Insert into MySQL database
            insert_into_db(uk_datetime, message_type, area_id, description, from_berth, to_berth)

# Run table creation once
create_table()
