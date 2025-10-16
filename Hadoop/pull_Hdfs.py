import os
import json
from datetime import datetime
import pytz
import psycopg2
from hdfs import InsecureClient
import requests
import time

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "weatherdb")
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/user/hdfs/weather")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")

LOCAL_TZ = pytz.timezone("Europe/Paris")

def connect_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )

def fetch_weather(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT date_time, msg_offset, partition, value FROM weather ORDER BY msg_offset ASC")
        return cur.fetchall()

def ensure_dir(client, path):
    if not client.status(path, strict=False):
        client.makedirs(path)

def target_dir_for(dt):
    return f"{HDFS_BASE_PATH}/dt={dt.strftime('%Y-%m-%d')}"

def export_to_hdfs(client, dir_path, rows):
    if not rows:
        return
    ts = datetime.now(LOCAL_TZ).strftime("%Y%m%dT%H%M%S%f")
    fname = f"weather_{ts}.json"
    path = f"{dir_path}/{fname}"
    payload = "\n".join(json.dumps({
        "kafka_partition": r[2],
        "kafka_offset": r[1],
        "event_time": r[0].strftime("%Y-%m-%d %H:%M:%S"),
        "value": r[3]
    }) for r in rows) + "\n"
    client.write(path, data=payload, overwrite=False, encoding="utf-8")
    print(f"✅ Exporté {len(rows)} records vers HDFS: {path}")

def wait_for_hdfs(url):
    import time
    while True:
        try:
            r = requests.get(f"{url}/webhdfs/v1/?op=LISTSTATUS")
            if r.status_code == 200:
                return
        except:
            pass
        time.sleep(5)

def main():
    time.sleep(30)
    conn = connect_pg()
    wait_for_hdfs(HDFS_URL)
    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

    today_dir = target_dir_for(datetime.now(LOCAL_TZ))
    ensure_dir(hdfs_client, today_dir)

    rows = fetch_weather(conn)
    export_to_hdfs(hdfs_client, today_dir, rows)

if __name__ == "__main__":
    main()
