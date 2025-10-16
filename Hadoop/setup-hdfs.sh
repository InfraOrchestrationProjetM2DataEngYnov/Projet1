#!/bin/bash
set -e

# Création du dossier HDFS
HDFS_DIR=${HDFS_BASE_PATH:-/user/hdfs/weather}
hdfs dfs -mkdir -p "$HDFS_DIR" || true
hdfs dfs -chown -R hdfs:hdfs "$HDFS_DIR"
hdfs dfs -chmod -R 755 "$HDFS_DIR"

# Lancer le NameNode en foreground pour que Docker garde le conteneur actif
hdfs namenode -foreground
