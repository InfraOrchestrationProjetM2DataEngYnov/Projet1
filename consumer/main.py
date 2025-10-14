import os
import json
import time
import psycopg2
time.sleep(30)
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC_NAME", "weather-api")
GROUP_ID = os.getenv("GROUP_ID", "weather-consumer-group")

# Config PostgreSQL
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "weatherdb")
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

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
                    Value JSONB
                )
        """)
        conn.commit()

def insert_weather(conn, date_time, value):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO weather (date_time, value) VALUES (%s,%s)",
            (date_time, json.dumps(value))
        )
        conn.commit()

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    print(f"✅ Consumer started, listening on topic '{TOPIC}'")

    # Connexion PostgreSQL
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
        print("🌤️ Received:", data)
        insert_weather(conn, time.strftime('%Y-%m-%d %H:%M:%S'), data)


if __name__ == "__main__":
    main()
