#!/usr/bin/env python3
import os
import logging
import time
import json
import socket
from datetime import datetime
from hdfs import InsecureClient
from pyhive import hive
from thrift.transport import TTransport

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Config via env
HDFS_WEBHDFS = os.getenv("HDFS_WEBHDFS", "http://namenode:9870")  # url webhdfs
HDFS_BASE = os.getenv("HDFS_BASE_PATH", "/user/hdfs/weather")
HIVE_HOST = os.getenv("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
HIVE_DB = os.getenv("HIVE_DATABASE", "default")
HIVE_TABLE = os.getenv("HIVE_TABLE", "weather_data")
INITIAL_DELAY_SEC = int(os.getenv("INITIAL_DELAY_SEC", "30"))
HDFS_LIST_RETRIES = int(os.getenv("HDFS_LIST_RETRIES", "10"))
HIVE_RETRIES = int(os.getenv("HIVE_RETRIES", "15"))

def wait(seconds):
    logger.info(f"Pause de {seconds}s ...")
    time.sleep(seconds)

def wait_for_tcp(host, port, retries=10, delay=5):
    for i in range(retries):
        try:
            s = socket.create_connection((host, port), timeout=5)
            s.close()
            logger.info(f"✔ TCP {host}:{port} reachable")
            return True
        except Exception as e:
            logger.warning(f"TCP {host}:{port} non reachable (tentative {i+1}/{retries}) : {e}")
            time.sleep(delay)
    return False

def list_hdfs_dirs(client, base, retries=5, delay=3):
    """
    Retourne la liste des sous-dossiers sous `base` qui commencent par 'dt='.
    """
    for attempt in range(retries):
        try:
            logger.info(f"Listing HDFS base: '{base}' (attempt {attempt+1}/{retries})")
            statuses = client.list(base, status=True)
            # client.list(base) retourne une dict-like list of (name, status)
            # selon la version, client.list(base) peut retourner just names -> handle both
            dirs = []
            if isinstance(statuses, dict) or isinstance(statuses, list):
                # hdfs.InsecureClient.list peut retourner list of names OR list of tuples (name, status)
                for item in statuses:
                    if isinstance(item, tuple) or isinstance(item, list):
                        name = item[0]
                    else:
                        name = item
                    if name.startswith("dt="):
                        dirs.append(name)
            logger.info(f"Found dt dirs: {dirs}")
            return dirs
        except Exception as e:
            logger.warning(f"Erreur list HDFS '{base}': {e} - retry dans {delay}s")
            time.sleep(delay)
    raise RuntimeError(f"Impossible de lister '{base}' après {retries} tentatives")

def read_json_files_from_hdfs(client, path):
    """
    Marche récursivement et retourne une liste de dicts (une ligne JSON = un dict).
    """
    records = []
    logger.info(f"Walking path: {path}")
    try:
        for dirpath, dirs, files in client.walk(path):
            logger.info(f"  walk: dirpath={dirpath} dirs={dirs} files={files}")
            for f in files:
                if not f.endswith(".json"):
                    continue
                file_path = f"{dirpath}/{f}"
                logger.info(f"    reading file {file_path}")
                with client.read(file_path, encoding='utf-8') as reader:
                    for line in reader:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            records.append(json.loads(line))
                        except Exception as e:
                            logger.warning(f"Failed to parse JSON line in {file_path}: {e}")
    except Exception as e:
        logger.exception(f"Erreur pendant walk/read HDFS: {e}")
        raise
    return records

def insert_into_hive(records, host, port, database, table):
    # attendre hive TCP
    if not wait_for_tcp(host, port, retries=HIVE_RETRIES, delay=5):
        raise RuntimeError(f"HiveServer2 {host}:{port} inaccessible après plusieurs tentatives")
    # connexion
    try:
        conn = hive.Connection(host=host, port=port, database=database)
        cursor = conn.cursor()
    except TTransport.TTransportException as e:
        raise RuntimeError(f"Erreur connexion Hive: {e}")

    # create table if not exists (simple schema)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            event_time STRING,
            kafka_partition INT,
            kafka_offset INT,
            value STRING,
            ingestion_time TIMESTAMP
        ) STORED AS PARQUET
    """)

    # Insert ligne par ligne (attention perf)
    inserted = 0
    for rec in records:
        try:
            cursor.execute(
                f"INSERT INTO {table} (event_time, kafka_partition, kafka_offset, value, ingestion_time) VALUES (%s, %s, %s, %s, %s)",
                (
                    rec.get("event_time"),
                    rec.get("kafka_partition"),
                    rec.get("kafka_offset"),
                    json.dumps(rec.get("value")),
                    datetime.now()
                )
            )
            inserted += 1
        except Exception as e:
            logger.exception(f"Erreur insert record into Hive: {e} -- skipping record")
    cursor.close()
    conn.close()
    logger.info(f"{inserted}/{len(records)} inserted into Hive table {table}")

def main():
    logger.info("=== Démarrage du process HDFS → Hive ===")
    if INITIAL_DELAY_SEC > 0:
        wait(INITIAL_DELAY_SEC)

    client = InsecureClient(HDFS_WEBHDFS, user='hdfs')
    logger.info(f"Instantiated {client!r}")

    # check base exists
    try:
        stat = client.status(HDFS_BASE, strict=False)
        logger.info(f"HDFS_BASE status for '{HDFS_BASE}': {stat}")
    except Exception as e:
        logger.warning(f"status('{HDFS_BASE}') failed: {e}")

    # récupère les dossiers dt=
    try:
        dt_dirs = list_hdfs_dirs(client, HDFS_BASE, retries=HDFS_LIST_RETRIES)
    except Exception as e:
        logger.exception("Impossible d'énumérer les sous-dossiers dt= sous base")
        return

    if not dt_dirs:
        logger.info("Aucun dossier dt= trouvé. Fin du job.")
        return

    # agrège les enregistrements de tous les dt_dirs trouvés
    ALL = []
    base_root = HDFS_BASE.rsplit("/", 1)[0]
    for d in dt_dirs:
        full = f"{HDFS_BASE}/{d}"

        logger.info(f"Processing dt directory: {full}")
        try:
            recs = read_json_files_from_hdfs(client, full)
            logger.info(f"  -> found {len(recs)} records in {full}")
            ALL.extend(recs)
        except Exception as e:
            logger.exception(f"Error reading files in {full} - skip")

    if not ALL:
        logger.info("Aucune donnée trouvée dans les dt dirs. Fin.")
        return

    # Insert into Hive
    try:
        insert_into_hive(ALL, HIVE_HOST, HIVE_PORT, HIVE_DB, HIVE_TABLE)
    except Exception as e:
        logger.exception(f"Insertion Hive failed: {e}")

if __name__ == "__main__":
    main()
