import os
import json
from datetime import datetime
import pytz
import psycopg2
from hdfs import InsecureClient
import requests
import time
import logging

# =========================
# Configuration & Logging
# =========================
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "weatherdb")
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/user/hdfs/weather").rstrip("/")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")

LOCAL_TZ = pytz.timezone("Europe/Paris")


# =========================
# Fonctions PostgreSQL
# =========================
def connect_pg():
    """Connexion à PostgreSQL."""
    logger.info(f"Connexion à PostgreSQL : {PG_HOST}:{PG_PORT}/{PG_DB} (user={PG_USER})")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        conn.autocommit = True
        logger.info("Connexion PostgreSQL OK.")
        return conn
    except Exception as e:
        logger.exception("Erreur de connexion à PostgreSQL")
        raise


def fetch_weather(conn):
    """Récupère les lignes de la table weather."""
    sql = """
        SELECT date_time, msg_offset, partition, value
        FROM weather
        ORDER BY msg_offset ASC
    """
    logger.info("Récupération des données météo depuis PostgreSQL...")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        logger.info(f"{len(rows)} enregistrements récupérés.")
        return rows
    except Exception as e:
        logger.exception("Erreur lors de la récupération des données météo")
        raise


# =========================
# Fonctions HDFS
# =========================
def wait_for_hdfs(url: str, timeout_sec: int = 300, step_sec: int = 5):
    """Attend que WebHDFS soit prêt avant de continuer."""
    logger.info(f"Attente disponibilité HDFS ({url})...")
    deadline = time.time() + timeout_sec
    last_err = None
    while time.time() < deadline:
        try:
            r = requests.get(f"{url}/webhdfs/v1/?op=LISTSTATUS", timeout=10)
            if r.status_code == 200:
                logger.info("HDFS est disponible.")
                return
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        time.sleep(step_sec)
    raise RuntimeError(f"HDFS indisponible après {timeout_sec}s (dernier état: {last_err})")


def validate_hdfs_base_path(base_path: str):
    """Empêche toute écriture accidentelle à la racine HDFS."""
    if not base_path or base_path.strip() == "" or base_path.strip() == "/":
        raise ValueError(f"HDFS_BASE_PATH invalide: '{base_path}' (interdit d'écrire à la racine /)")


# def ensure_dir(client: InsecureClient, path: str):
#     """
#     Crée le répertoire HDFS (idempotent).
#     Utilise makedirs qui ne plante pas si le dossier existe déjà.
#     """
#     try:
#         logger.info(f"Vérification/Création répertoire HDFS: {path}")
#         client.makedirs(path)
#     except Exception:
#         logger.exception(f"Erreur lors de la création du répertoire HDFS: {path}")
#         raise


def target_dir_for(dt: datetime) -> str:
    """Construit le répertoire de partition par date: .../dt=YYYY-MM-DD"""
    validate_hdfs_base_path(HDFS_BASE_PATH)
    return f"{HDFS_BASE_PATH}/dt={dt.strftime('%Y-%m-%d')}"


def export_to_hdfs(client: InsecureClient, dir_path: str, rows):
    """Écrit les données sous forme JSON dans HDFS."""
    if not rows:
        logger.info("Aucune donnée à exporter vers HDFS.")
        return

    ts = datetime.now(LOCAL_TZ).strftime("%Y%m%dT%H%M%S%f")
    fname = f"weather_{ts}.json"
    path = f"{dir_path}/{fname}"

    try:
        payload_lines = []
        for r in rows:
            line = {
                "kafka_partition": r[2],
                "kafka_offset": r[1],
                "event_time": r[0].strftime("%Y-%m-%d %H:%M:%S"),
                "value": r[3],
            }
            payload_lines.append(json.dumps(line, ensure_ascii=False))
        payload = "\n".join(payload_lines) + "\n"
    except Exception:
        logger.exception("Erreur de sérialisation JSON")
        raise

    try:
        logger.info(f"Écriture HDFS vers: {path} (user={HDFS_USER})")
        client.write(path, data=payload, overwrite=False, encoding="utf-8")
        logger.info(f"✅ Exporté {len(rows)} enregistrements vers HDFS: {path}")
    except Exception:
        logger.exception("Erreur lors de l'export vers HDFS")
        raise


# =========================
# Main
# =========================
def main():
    logger.info("Démarrage du traitement météo (HDFS).")
    logger.info(f"Paramètres HDFS: URL={HDFS_URL}, USER={HDFS_USER}, BASE_PATH={HDFS_BASE_PATH}")

    initial_delay = int(os.getenv("INITIAL_DELAY_SEC", "30"))
    if initial_delay > 0:
        logger.info(f"Pause initiale de {initial_delay} sec...")
        time.sleep(initial_delay)

    conn = connect_pg()
    wait_for_hdfs(HDFS_URL)

    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
    today_dir = target_dir_for(datetime.now(LOCAL_TZ))

    # ensure_dir(hdfs_client, HDFS_BASE_PATH)
    # ensure_dir(hdfs_client, today_dir)

    rows = fetch_weather(conn)
    export_to_hdfs(hdfs_client, today_dir, rows)


if __name__ == "__main__":
    main()
