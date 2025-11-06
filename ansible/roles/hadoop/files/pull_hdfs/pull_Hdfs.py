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
# CONFIGURATION ET LOGGING
# =========================

# Configuration du format des logs
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Variables d’environnement
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_DB = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/user/hdfs/weather").rstrip("/")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")

# Fuseau horaire local
LOCAL_TZ = pytz.timezone("Europe/Paris")


# =========================
# FONCTIONS POSTGRESQL
# =========================

def connect_pg():
    """Établit une connexion à la base PostgreSQL."""
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
        logger.info("Connexion PostgreSQL réussie.")
        return conn
    except Exception as e:
        logger.exception("Erreur de connexion à PostgreSQL")
        raise


def create_table_ref_date(conn):
    """Crée la table REF_DATE si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS REF_DATE (
                ref_date TIMESTAMP PRIMARY KEY DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


        
def get_last_pull(conn) -> datetime:
    """Renvoie le dernier timestamp de pull ou None si aucun."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ref_date) FROM REF_DATE")
        last_pull = cur.fetchone()[0]
    return last_pull


def fetch_weather(conn, last_pull: datetime = None):
    """Récupère seulement les lignes après le dernier pull."""
    sql = "SELECT date_time, msg_offset, partition, value FROM weather"
    params = ()
    if last_pull:
        sql += " WHERE date_time > %s"
        params = (last_pull,)
    sql += " ORDER BY msg_offset ASC"

    logger.info(f"Récupération des données météo depuis PostgreSQL après {last_pull}...")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    logger.info(f"{len(rows)} enregistrements récupérés depuis PostgreSQL.")
    return rows



# =========================
# FONCTIONS HDFS
# =========================

def wait_for_hdfs(url: str, timeout_sec: int = 300, step_sec: int = 5):
    """
    Attend que le service WebHDFS soit prêt avant de lancer l’écriture.
    Vérifie la disponibilité pendant un délai donné.
    """
    logger.info(f"Attente de la disponibilité de HDFS ({url})...")
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

def insert_pull_timestamp(conn, ts: datetime):
    """Insère le timestamp du pull courant dans REF_DATE."""
    with conn.cursor() as cur:
        cur.execute("INSERT INTO REF_DATE(ref_date) VALUES (%s)", (ts,))
        conn.commit()

def validate_hdfs_base_path(base_path: str):
    """Empêche toute écriture accidentelle à la racine du système HDFS."""
    if not base_path or base_path.strip() == "" or base_path.strip() == "/":
        raise ValueError(f"HDFS_BASE_PATH invalide: '{base_path}' (interdit d'écrire à la racine /)")


def target_dir_for(dt: datetime) -> str:
    """Construit le répertoire cible basé sur la date (partition journalière)."""
    validate_hdfs_base_path(HDFS_BASE_PATH)
    return f"{HDFS_BASE_PATH}/dt={dt.strftime('%Y-%m-%d')}"


def export_to_hdfs(client: InsecureClient, dir_path: str, rows):
    """
    Exporte les données récupérées depuis PostgreSQL vers HDFS
    sous forme de fichiers JSON structurés.
    """
    if not rows:
        logger.info("Aucune donnée à exporter vers HDFS.")
        return

    # Génération du nom de fichier basé sur la date et l’heure actuelles
    ts = datetime.now(LOCAL_TZ).strftime("%Y%m%dT%H%M%S%f")
    fname = f"weather_{ts}.json"
    path = f"{dir_path}/{fname}"

    try:
        # Préparation du contenu JSON ligne par ligne
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
        logger.exception("Erreur lors de la sérialisation JSON")
        raise

    try:
        logger.info(f"Écriture dans HDFS : {path} (user={HDFS_USER})")
        client.write(path, data=payload, overwrite=False, encoding="utf-8")
        logger.info(f"Export réussi de {len(rows)} enregistrements vers HDFS : {path}")
    except Exception:
        logger.exception("Erreur lors de l'export vers HDFS")
        raise


# =========================
# FONCTION PRINCIPALE
# =========================

def main():
    """Programme principal : lecture PostgreSQL → export vers HDFS."""
    logger.info("Démarrage du processus d’export des données météo vers HDFS.")

    initial_delay = int(os.getenv("INITIAL_DELAY_SEC", "30"))
    if initial_delay > 0:
        logger.info(f"Pause initiale de {initial_delay} secondes...")
        time.sleep(initial_delay)

    while True:
        logger.info("=== Nouveau cycle de pull démarré ===")

        # Connexion à PostgreSQL
        logger.info("Connexion à PostgreSQL...")
        conn = connect_pg()
        create_table_ref_date(conn)  # Assure que la table existe
        logger.info("Table REF_DATE vérifiée / créée si nécessaire.")

        # Récupération du dernier pull
        last_pull = get_last_pull(conn)
        if last_pull:
            logger.info(f"Dernier pull enregistré : {last_pull}")
        else:
            logger.info("Aucun pull précédent trouvé. Export complet.")

        # Vérification de la disponibilité de HDFS
        logger.info("Vérification de la disponibilité de HDFS...")
        wait_for_hdfs(HDFS_URL)

        # Initialisation du client HDFS
        hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
        today_dir = target_dir_for(datetime.now(LOCAL_TZ))
        logger.info(f"Répertoire HDFS cible pour l'export : {today_dir}")

        # Récupération et export des données
        rows = fetch_weather(conn, last_pull)
        if rows:
            logger.info(f"{len(rows)} nouvelles lignes à exporter vers HDFS...")
            export_to_hdfs(hdfs_client, today_dir, rows)
            insert_pull_timestamp(conn, datetime.now(LOCAL_TZ))
            logger.info("Pull et export terminés avec succès.")
        else:
            logger.info("Aucune nouvelle donnée à exporter.")

        # Fermeture de la connexion
        conn.close()
        logger.info("Connexion PostgreSQL fermée.")

        # Pause avant le prochain cycle
        logger.info("Pause de 10 minutes avant le prochain pull...")
        time.sleep(10*60)  # 10 minutes



if __name__ == "__main__":
    main()
