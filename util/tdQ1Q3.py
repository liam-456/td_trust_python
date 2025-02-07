import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from pytz import timezone

# Configure logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_file = "td_processor.log"

handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)  # 5MB per log, keep last 3
handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

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
    "user": "your_user",
    "password": "your_password",
    "database": "train_data"
}

# Function to create a MySQL connection
def create_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            logger.info("Connected to MySQL database successfully.")
            return conn
    except Error as e:
        logger.error(f"MySQL Connection Error: {e}")
    return None

# Function to create the table if it does not exist
def create_table():
    conn = create_connection()
    if conn:
        try:
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
            logger.info("Table `td_messages_Q1_Q3` ensured to exist.")
        except Error as e:
            logger.error(f"MySQL Table Creation Error: {e}")
        finally:
            cursor.close()
            conn.close()

# Function to insert data into the database
def insert_into_db(timestamp, message_type, area_id, description, from_berth, to_berth):
    conn = create_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = """
            INSERT INTO td_messages_Q1_Q3 (timestamp, message_type, area_id, description, from_berth, to_berth)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            cursor.execute(query, (timestamp, message_type, area_id, description, from_berth, to_berth))
            conn.commit()
            logger.info(f"Inserted TD message: {timestamp}, {message_type}, {area_id}, {description}, {from_berth}, {to_berth}")
        except Error as e:
            logger.error(f"MySQL Insert Error: {e}")
        finally:
            cursor.close()
            conn.close()

# Function to process and export TD messages
def process_td_messages_Q1_Q3(parsed_body):
    for outer_message in parsed_body:
        message = list(outer_message.values())[0]
        message_type = message["msg_type"]

        if message_type in [C_BERTH_STEP, C_BERTH_CANCEL, C_BERTH_INTERPOSE]:
            timestamp = int(message["time"]) / 1000
            area_id = message["area_id"]

            if area_id not in ["Q1", "Q3"]:
                logger.debug(f"Ignored message from area {area_id}.")
                continue

            description = message.get("descr", "")
            from_berth = message.get("from", "")
            to_berth = message.get("to", "")

            utc_datetime = datetime.utcfromtimestamp(timestamp)
            uk_datetime = TIMEZONE_LONDON.fromutc(utc_datetime)

            log_message = f"{uk_datetime.strftime('%Y-%m-%d %H:%M:%S')} [{message_type}] {area_id} {description} {from_berth}->{to_berth}"
            print(log_message)
            logger.info(f"Processed message: {log_message}")

            # Insert into MySQL database
            insert_into_db(uk_datetime, message_type, area_id, description, from_berth, to_berth)

# Run table creation once
create_table()
