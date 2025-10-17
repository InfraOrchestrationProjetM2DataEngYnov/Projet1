import os
import json
from datetime import datetime
import pytz
import psycopg2
from hdfs import InsecureClient
import requests
import time
import logging

# Configuration du logger
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger()

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "weatherdb")
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/user/hdfs/weather")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")

LOCAL_TZ = pytz.timezone("Europe/Paris")

def connect_pg():
    try:
        logger.info(f"Connexion à PostgreSQL en cours : {PG_HOST}:{PG_PORT}/{PG_DB}")
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD
        )
        logger.info("Connexion à PostgreSQL réussie !")
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à PostgreSQL: {e}")
        raise

def fetch_weather(conn):
    try:
        logger.info("Récupération des données météo depuis PostgreSQL...")
        with conn.cursor() as cur:
            cur.execute("SELECT date_time, msg_offset, partition, value FROM weather ORDER BY msg_offset ASC")
            rows = cur.fetchall()
            logger.info(f"{len(rows)} enregistrements récupérés.")
            return rows
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données météo: {e}")
        raise

# def ensure_dir(client, path):
#     try:
#         logger.info(f"Vérification du répertoire HDFS: {path}")
#         if not client.status(path, strict=False):
#             client.makedirs(path)
#             logger.info(f"Répertoire HDFS créé: {path}")
#         else:
#             logger.info(f"Répertoire HDFS existe déjà: {path}")
#     except Exception as e:
#         logger.error(f"Erreur lors de la création du répertoire HDFS: {e}")
#         raise

def target_dir_for(dt):
    return f"{HDFS_BASE_PATH}/dt={dt.strftime('%Y-%m-%d')}"

def export_to_hdfs(client, dir_path, rows):
    if not rows:
        logger.info("Aucune donnée à exporter vers HDFS.")
        return
    try:
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
        logger.info(f"✅ Exporté {len(rows)} enregistrements vers HDFS: {path}")
    except Exception as e:
        logger.error(f"Erreur lors de l'export vers HDFS: {e}")
        raise

def wait_for_hdfs(url):
    logger.info(f"Attente de la disponibilité de HDFS à {url}...")
    while True:
        try:
            r = requests.get(f"{url}/webhdfs/v1/?op=LISTSTATUS")
            if r.status_code == 200:
                logger.info("HDFS est disponible !")
                return
        except Exception as e:
            logger.warning(f"Erreur lors de la vérification de HDFS: {e}")
        time.sleep(5)

def main():
    logger.info("Démarrage du processus de traitement météo...")
    
    logger.info("Pause initiale de 30 secondes avant de commencer...")
    time.sleep(30)

    logger.info("Début de la connexion à PostgreSQL...")
    conn = connect_pg()
    
    wait_for_hdfs(HDFS_URL)
    hdfs_client = InsecureClient(HDFS_URL, user='hdfs')
    # ensure_dir(hdfs_client, HDFS_BASE_PATH)

    today_dir = target_dir_for(datetime.now(LOCAL_TZ))
    
    rows = fetch_weather(conn)
    export_to_hdfs(hdfs_client, today_dir, rows)

if __name__ == "__main__":
    main()
