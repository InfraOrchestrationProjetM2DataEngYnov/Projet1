import os
import time
from datetime import datetime
from typing import List, Tuple
import logging

import psycopg2
from psycopg2.extras import DictCursor

from pyspark.sql.functions import col, from_json, schema_of_json, lit, from_unixtime, to_json, struct
from pyspark.sql.types import StructType

from pyspark.sql.functions import col, from_unixtime, to_json, struct
from pyspark.sql import SparkSession


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


# ------------ Postgres helpers ------------

def get_pg_conn():
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


def get_ref_date(application_name: str) -> datetime:
    """
    Read ref_date from REF_DATE for the given application_name.
    If not found, return a very old date.
    """
    with get_pg_conn() as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT ref_date
            FROM REF_DATE
            WHERE application_name = %s
            """,
            (application_name,),
        )
        row = cur.fetchone()
        if row and row["ref_date"]:
            return row["ref_date"]

    # default watermark if not present
    return datetime(2000, 1, 1, 0, 0, 0)


def update_ref_date(application_name: str, ref_date: datetime) -> None:
    """
    Upsert ref_date for the given application_name.
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO REF_DATE (application_name, ref_date)
            VALUES (%s, %s)
            ON CONFLICT (application_name)
            DO UPDATE SET ref_date = EXCLUDED.ref_date
            """,
            (application_name, ref_date),
        )
        conn.commit()


# ------------ HDFS listing via Spark JVM ------------

def list_new_files(spark, base_path: str, since_dt: datetime) -> Tuple[List[str], datetime]:
    """
    Recursively list files under base_path with modification time > since_dt.
    Uses Hadoop FileSystem via Spark JVM.
    Adds detailed logging to understand what is happening.
    """
    sc = spark.sparkContext
    jvm = sc._jvm

    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = FileSystem.get(hadoop_conf)

    logger.info(f"Scanning HDFS from base_path={base_path} with watermark={since_dt.isoformat()}")

    stack = [Path(base_path)]
    new_paths: List[str] = []
    latest_dt = since_dt

    while stack:
        current_path = stack.pop()
        logger.info(f"[HDFS] Listing path: {current_path}")

        try:
            status_list = fs.listStatus(current_path)
        except Exception as e:
            logger.warning(f"[HDFS] Cannot list path {current_path}: {e}")
            continue

        for status in status_list:
            p = status.getPath()
            path_str = p.toString()

            if status.isDirectory():
                logger.info(f"[HDFS] Found directory: {path_str}, pushing to stack")
                stack.append(p)
                continue

            # File case
            mod_ms = status.getModificationTime()
            file_dt = datetime.fromtimestamp(mod_ms / 1000.0)

            logger.info(
                f"[HDFS] Found file: {path_str} | mtime={file_dt.isoformat()} "
                f"| since={since_dt.isoformat()} | is_new={file_dt > since_dt}"
            )

            if file_dt > since_dt:
                new_paths.append(path_str)
                if file_dt > latest_dt:
                    latest_dt = file_dt

    logger.info(f"[HDFS] New files found: {len(new_paths)}, latest_dt={latest_dt.isoformat()}")
    for p in new_paths:
        logger.info(f"[HDFS] New file selected: {p}")

    return new_paths, latest_dt



# ------------ Spark processing ------------

def process_files(spark, paths: List[str], database_name: str, table_name: str):
    """
    Read weather JSON files and append them into a Hive table.
    Expects a top-level 'value' struct containing keys:
      weather, forecast, sun_info, air_pollution, precipitations_info
    """
    if not paths:
        print("[INFO] No new files to process.")
        return

    print(f"[INFO] Processing {len(paths)} new file(s).")
    df = spark.read.json(paths)

    # If files are wrapped in a 'value' column (event_time, kafka_offset, value, ...)
    if "value" in df.columns:
        print("[INFO] Detected 'value' struct column. Expanding it...")
        df = df.select(col("value.*"))

    # At this point df has columns: weather, forecast, sun_info, air_pollution, precipitations_info

    cleaned_df = (
        df.select(
            # City / location (current weather)
            col("weather.id").cast("long").alias("city_id"),
            col("weather.name").cast("string").alias("city_name"),
            col("weather.coord.lat").cast("double").alias("lat"),
            col("weather.coord.lon").cast("double").alias("lon"),

            # Timestamps
            from_unixtime(col("weather.dt")).cast("timestamp").alias("obs_ts_utc"),
            from_unixtime(col("weather.sys.sunrise")).cast("timestamp").alias("sunrise_utc"),
            from_unixtime(col("weather.sys.sunset")).cast("timestamp").alias("sunset_utc"),

            # Main metrics
            col("weather.main.temp").cast("double").alias("temp"),
            col("weather.main.feels_like").cast("double").alias("feels_like"),
            col("weather.main.temp_min").cast("double").alias("temp_min"),
            col("weather.main.temp_max").cast("double").alias("temp_max"),
            col("weather.main.pressure").cast("double").alias("pressure"),
            col("weather.main.humidity").cast("double").alias("humidity"),

            # Wind
            col("weather.wind.speed").cast("double").alias("wind_speed"),
            col("weather.wind.deg").cast("double").alias("wind_deg"),

            # Weather description (first element)
            col("weather.weather")[0]["id"].cast("int").alias("weather_code"),
            col("weather.weather")[0]["main"].cast("string").alias("weather_main"),
            col("weather.weather")[0]["description"].cast("string").alias("weather_description"),
            col("weather.weather")[0]["icon"].cast("string").alias("weather_icon"),

            # Timezone offset
            col("weather.timezone").cast("int").alias("timezone_offset_seconds"),

            # Raw payload backup
            to_json(struct("*")).alias("raw_payload"),
        )
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    (
        cleaned_df.write
        .mode("append")
        .format("parquet")
        .option("path", "hdfs://namenode:8020/user/hive/warehouse/weather.db/events")
        .saveAsTable(f"{database_name}.{table_name}")
    )

    print(f"[INFO] Written data to {database_name}.{table_name}.")



# ------------ Main daemon ------------

def wait_for_hdfs_ready(spark, max_retries=30, delay=10):
    """
    Wait for HDFS to exit safe mode.
    """
    sc = spark.sparkContext
    jvm = sc._jvm
    
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem
    hadoop_conf = sc._jsc.hadoopConfiguration()
    
    for attempt in range(max_retries):
        try:
            fs = FileSystem.get(hadoop_conf)
            # Try to create a test directory
            test_path = jvm.org.apache.hadoop.fs.Path("/tmp/.hdfs_ready_test")
            fs.mkdirs(test_path)
            fs.delete(test_path, True)
            logger.info("HDFS is ready and out of safe mode")
            return True
        except Exception as e:
            if "SafeModeException" in str(e):
                logger.warning(f"HDFS in safe mode, waiting... (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                logger.error(f"Unexpected error checking HDFS: {e}")
                raise
    
    raise RuntimeError("HDFS did not exit safe mode in time")

def main():
    application_name = "SPARK"

    base_path = "/user/hdfs/weather"
    database_name = "weather"
    table_name = "events"
    poll_interval_seconds = 600
    # Let Hadoop defaultFS handle the scheme; path is from HDFS root    
    spark = (
        SparkSession.builder
                .appName("WeatherToHiveDaemon")
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
                .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate()
                )

    # AJOUT : Attendre que HDFS soit prêt
    logger.info("Checking if HDFS is ready...")
    wait_for_hdfs_ready(spark)
    
    # AJOUT : Forcer la sortie du safe mode si nécessaire
    try:
        sc = spark.sparkContext
        jvm = sc._jvm
        FileSystem = jvm.org.apache.hadoop.fs.FileSystem
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = FileSystem.get(hadoop_conf)
        
        # Essayer de désactiver le safe mode (si on a les permissions)
        logger.info("Attempting to leave safe mode...")
        dfs_admin = jvm.org.apache.hadoop.hdfs.tools.DFSAdmin()
        dfs_admin.setConf(hadoop_conf)
        dfs_admin.run(["-safemode", "leave"])
    except Exception as e:
        logger.warning(f"Could not force leave safe mode: {e}")

    watermark = get_ref_date(application_name)
    logger.info(f"Starting daemon with initial watermark = {watermark.isoformat()}")

    try:
        while True:
            logger.info("Scanning HDFS for new JSON files under %s", base_path)
            new_paths, latest_dt = list_new_files(spark, base_path, watermark)

            if new_paths:
                process_files(
                    spark=spark,
                    paths=new_paths,
                    database_name=database_name,
                    table_name=table_name,
                )
                # update local + DB watermark
                new_watermark = max(latest_dt, watermark)
                update_ref_date(application_name, new_watermark)
                watermark = new_watermark
                logger.info(f"Updated watermark to {watermark.isoformat()}")
            else:
                logger.info("No new files found since last watermark.")

            logger.info(f"Sleeping for {poll_interval_seconds} seconds.")
            time.sleep(poll_interval_seconds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
