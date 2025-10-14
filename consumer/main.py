import os
import json
import time
from datetime import datetime
import pytz
import psycopg2
time.sleep(30)
from kafka import KafkaConsumer
from hdfs import InsecureClient

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC_NAME", "weather-api")
GROUP_ID = os.getenv("GROUP_ID", "weather-consumer-group")

# Config PostgreSQL
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "weatherdb")
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

# Config Hadoop
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")  # WebHDFS/HttpFS
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/weather")
FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", "1000"))          # nb de messages par fichier
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "60"))
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

# def connect_pg():
#     return psycopg2.connect(
#         host=PG_HOST,
#         port=PG_PORT,
#         dbname=PG_DB,
#         user=PG_USER,
#         password=PG_PASSWORD
#     )

# def create_table(conn):
#     with conn.cursor() as cur:
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS weather (
#                 date_time TIMESTAMP,
#                 msg_offset BIGINT,
#                 partition INT,
#                 value JSONB
#             )
#         """)

#         conn.commit()

# def insert_weather(conn, date_time, offset, partition, value):
#     with conn.cursor() as cur:
#         cur.execute(
#             "INSERT INTO weather (date_time, msg_offset, partition, value) VALUES (%s,%s,%s,%s)",
#             (date_time, offset, partition, json.dumps(value))
#         )
#         conn.commit()
    

# def main():
#     consumer = KafkaConsumer(
#         TOPIC,
#         bootstrap_servers=[BOOTSTRAP_SERVERS],
#         group_id=GROUP_ID,
#         value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#         auto_offset_reset="earliest",
#         enable_auto_commit=True
#     )
#     print(f"✅ Consumer started, listening on topic '{TOPIC}'")

#     # Connexion PostgreSQL
#     conn = None
#     while conn is None:
#         try:
#             conn = connect_pg()
#             create_table(conn)
#             print("✅ Connected to PostgreSQL")
#         except Exception as e:
#             print("Waiting for PostgreSQL...", e)
#             time.sleep(5)

#     for message in consumer:
#         data = message.value
#         offset = message.offset
#         partition = message.partition
#         timestamp = message.timestamp  # epoch (ms)
#         local_tz = pytz.timezone("Europe/Paris")

#         timestamp_str = datetime.fromtimestamp(timestamp / 1000, tz=local_tz).strftime('%Y-%m-%d %H:%M:%S')

#         print(f"🌤️ Received message at offset {offset} (partition {partition})")

#         insert_weather(conn, timestamp_str, offset, partition, data)
LOCAL_TZ = pytz.timezone("Europe/Paris")


def ensure_dir(client: InsecureClient, path: str):
    if not client.status(path, strict=False):
        client.makedirs(path)

def target_dir_for(dt: datetime) -> str:
    return f"{HDFS_BASE_PATH}/dt={dt.strftime('%Y-%m-%d')}"

def flush_batch(client, dir_path, batch):
    if not batch:
        return
    ts = datetime.now(LOCAL_TZ).strftime("%Y%m%dT%H%M%S%f")
    fname = f"weather_{ts}.jsonl"
    path = f"{dir_path}/{fname}"
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in batch) + "\n"
    client.write(path, data=payload, overwrite=False, encoding="utf-8")
    print(f"✅ Flushed {len(batch)} records to HDFS: {path}")

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

    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
    ensure_dir(hdfs_client, HDFS_BASE_PATH)

    batch = []
    last_flush = time.time()
    current_dir = None

    for msg in consumer:
        data = msg.value
        ts_ms = msg.timestamp
        event_dt = datetime.fromtimestamp(ts_ms / 1000, tz=LOCAL_TZ)
        dir_path = target_dir_for(event_dt)

        if dir_path != current_dir:
            ensure_dir(hdfs_client, dir_path)
            current_dir = dir_path

        record = {
            "kafka_partition": msg.partition,
            "kafka_offset": msg.offset,
            "event_time": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "value": data
        }
        batch.append(record)

        if len(batch) >= FLUSH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL_SEC:
            flush_batch(hdfs_client, current_dir, batch)
            batch = []
            last_flush = time.time()



if __name__ == "__main__":
    main()
