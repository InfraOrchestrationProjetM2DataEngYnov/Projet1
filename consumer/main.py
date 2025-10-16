import os
import json
import time
from datetime import datetime
import pytz
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
TOPIC = os.getenv("TOPIC_NAME")
GROUP_ID = os.getenv("GROUP_ID")

PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_DB = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def connect_pg():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                date_time TIMESTAMP,
                msg_offset BIGINT,
                partition INT,
                value JSONB
            )
        """)
        conn.commit()

def insert_weather(conn, date_time, offset, partition, value):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO weather (date_time, msg_offset, partition, value) VALUES (%s,%s,%s,%s)",
            (date_time, offset, partition, json.dumps(value))
        )
        conn.commit()

def connect_kafka(retries=10, delay=10):
    for i in range(retries):
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready, retry {i+1}/{retries}")
            time.sleep(delay)
    raise Exception("Kafka not reachable after retries")

def main():
    consumer = connect_kafka()
    print(f"✅ Consumer started, listening on topic '{TOPIC}'")

    conn = None
    while conn is None:
        try:
            conn = connect_pg()
            create_table(conn)
            print("✅ Connected to PostgreSQL")
        except Exception as e:
            print("Waiting for PostgreSQL...", e)
            time.sleep(5)

    for message in consumer:
        data = message.value
        offset = message.offset
        partition = message.partition
        timestamp = message.timestamp
        local_tz = pytz.timezone("Europe/Paris")
        timestamp_str = datetime.fromtimestamp(timestamp / 1000, tz=local_tz).strftime('%Y-%m-%d %H:%M:%S')

        print(f"🌤️ Received message at offset {offset} (partition {partition})")
        insert_weather(conn, timestamp_str, offset, partition, data)

if __name__ == "__main__":
    main()