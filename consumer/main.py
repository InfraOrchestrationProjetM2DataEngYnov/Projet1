import os
import json
import time
from datetime import datetime
import pytz
import requests
import psycopg2
from kafka import KafkaConsumer, errors
from hdfs import InsecureClient

# =====================
# CONFIG ENV
# =====================
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC_NAME", "weather-api")
GROUP_ID = os.getenv("GROUP_ID", "weather-consumer-group")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "weatherdb")
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/user/hdfs/weather")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", "1000"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "60"))

LOCAL_TZ = pytz.timezone("Europe/Paris")

# =====================
# POSTGRESQL
# =====================
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

# =====================
# HDFS
# =====================
def ensure_dir(client: InsecureClient, path: str):
    if not client.status(path, strict=False):
        client.makedirs(path)

def target_dir_for(dt: datetime) -> str:
    return f"{HDFS_BASE_PATH}/dt={dt.strftime('%Y-%m-%d')}"

def flush_batch(client, dir_path, batch):
    if not batch:
        return
    ts = datetime.now(LOCAL_TZ).strftime("%Y%m%dT%H%M%S%f")
    fname = f"weather_{ts}.json"
    path = f"{dir_path}/{fname}"
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in batch) + "\n"
    client.write(path, data=payload, overwrite=False, encoding="utf-8")
    print(f"✅ Flushed {len(batch)} records to HDFS: {path}")

# =====================
# WAIT
# =====================
def wait_for_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            print("✅ Connected to Kafka")
            return consumer
        except errors.NoBrokersAvailable:
            print("⏳ Waiting for Kafka broker...")
            time.sleep(5)

def wait_for_postgres():
    conn = None
    while conn is None:
        try:
            conn = connect_pg()
            create_table(conn)
            print("✅ Connected to PostgreSQL")
        except Exception as e:
            print("⏳ Waiting for PostgreSQL...", e)
            time.sleep(5)
    return conn

def wait_for_hdfs(url):
    while True:
        try:
            r = requests.get(f"{url}/webhdfs/v1/?op=LISTSTATUS")
            if r.status_code == 200:
                print("✅ HDFS ready")
                return
        except requests.exceptions.RequestException:
            pass
        print("⏳ Waiting for HDFS...")
        time.sleep(20)
# =====================
# MAIN ================
# =====================
def main():
    # Wait configuration
    consumer = wait_for_kafka()
    conn = wait_for_postgres()
    wait_for_hdfs(HDFS_URL)
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

        # Création du dossier HDFS si nécessaire
        if dir_path != current_dir:
            ensure_dir(hdfs_client, dir_path)
            current_dir = dir_path

        # Préparer record
        record = {
            "kafka_partition": msg.partition,
            "kafka_offset": msg.offset,
            "event_time": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "value": data
        }
        batch.append(record)

        # Insérer dans PostgreSQL
        insert_weather(conn, event_dt, msg.offset, msg.partition, data)

        # Flush HDFS si taille batch ou intervalle
        if len(batch) >= FLUSH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL_SEC:
            flush_batch(hdfs_client, current_dir, batch)
            batch = []
            last_flush = time.time()

if __name__ == "__main__":
    main()
