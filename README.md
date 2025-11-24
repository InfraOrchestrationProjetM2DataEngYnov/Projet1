## Projet Orchestration Data Météo (Kafka → PostgreSQL → HDFS → Mapreduce → Hive)

### Présentation
Pipeline de données temps réel et batch autour de la météo:
- Producer récupère les données OpenWeather et les envoie dans Kafka.
- Consumer lit Kafka et persiste les messages dans PostgreSQL (JSONB).
- Un job d’export pousse périodiquement les données de PostgreSQL vers HDFS (partition journalière).
- Un job Python ingère les fichiers HDFS dans Hive (via HiveServer2 avec PyHive).
- Monitoring via Prometheus et Grafana, Kafka UI pour l’observation des topics.

### Architecture (services principaux)
- Kafka (mode KRaft) + Kafka UI
- PostgreSQL + postgres-exporter (Prometheus)
- Producer Python (OpenWeather → Kafka)
- Consumer Python (Kafka → PostgreSQL)
- Hadoop (NameNode + DataNode) exposant WebHDFS
- Export PostgreSQL → HDFS (Python)
- MapReduce Python HDFS → Hive (PyHive)
- Prometheus + Grafana

![Texte alternatif](image/NotreArchitecture.jpg)


Flux de données:
1) OpenWeather → Producer → Kafka topic `weather-api`
2) Kafka → Consumer → table PostgreSQL `weather`
3) PostgreSQL → Export Python → HDFS `/user/hdfs/weather/dt=YYYY-MM-DD/weather_*.json`
4) HDFS → Job Python → Hive (table `weather_data`)

---

## Prérequis
- Docker et Docker Compose
- Ansible (pour le déploiement automatisé)
- Accès Internet (API OpenWeather)

Facultatif pour tests/CLI:
- Python 3.12+ si vous souhaitez exécuter localement les scripts

---

```

Note: Les rôles Ansible référencent des templates `.env.j2`. S’ils ne sont pas fournis, utilisez le bloc ci-dessus pour générer manuellement un `.env` sur la machine cible.

---

## Déploiement avec Ansible
Inventaire local fourni: `ansible/inventory.ini`

Commande:
```bash
ansible-playbook -i ansible/inventory.ini ansible/site.yml
```

Ce playbook applique:
- Rôle `kafka`: déploie Kafka, Kafka UI, PostgreSQL, Producer, Consumer, Prometheus, Grafana.
- Rôle `hadoop`: déploie NameNode, DataNode, export Postgres→HDFS, ingestion HDFS→Hive.

Le réseau Docker externe `infra-kafka` est assuré par Ansible.

---

## Détails techniques

### Producer (`producer/main.py`)
- Récupère périodiquement:
  - météo courante, prévisions 5j, qualité de l’air, précipitations, infos soleil
- Envoie un message JSON sur le topic `TOPIC_NAME` (défaut: `weather-api`) toutes les 10 minutes
- Dépend de: `OPENWEATHER_API_KEY`, `CITY`, `BOOTSTRAP_SERVERS`

### Consumer (`consumer/main.py`)
- Consomme le topic, insère dans PostgreSQL:
  - table `weather(date_time TIMESTAMP, msg_offset BIGINT, partition INT, value JSONB)`
- Convertit les timestamps en Europe/Paris

### Export PostgreSQL → HDFS (`Hadoop/pull_Hdfs.py` et `Hadoop/hadoop-pull/pull_Hdfs.py`)
- Lit les nouvelles lignes depuis la dernière exécution (table `REF_DATE`)
- Écrit des fichiers `weather_*.json` en JSON Lines sous `HDFS_BASE_PATH/dt=YYYY-MM-DD`
- Utilise WebHDFS via `InsecureClient`

### Ingestion HDFS → Hive (`ansible/roles/hadoop/files/MapReduce/Map_Reduce.py`)
- Parcourt récursivement les partitions `dt=*` sous `HDFS_BASE_PATH` et lit les fichiers `.json` (JSON Lines).
- Attend HiveServer2, crée la table si absente (schéma ci-dessous), puis insère une ligne par enregistrement.

```sql
CREATE TABLE IF NOT EXISTS weather_data (
  event_time STRING,
  kafka_partition INT,
  kafka_offset INT,
  value STRING,
  ingestion_time TIMESTAMP
) STORED AS PARQUET;
```

### Monitoring
- Prometheus scrape:
  - `postgres-exporter:9187`
  - `kafka:9092` (note: pour des métriques Kafka plus riches, prévoir un exporter dédié)
- Grafana: admin/admin par défaut

---

## Accès aux services (ports par défaut)
- Kafka: 9092 (broker), 9093 (controller interne)
- Kafka UI: http://localhost:8080
- PostgreSQL: 5432
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- HDFS NameNode Web UI: http://localhost:9870


---

## Lancement rapide (résumé)
1) Créez un `.env` complet (voir exemple plus haut).
2) Exécutez:
```bash
ansible-playbook -i ansible/inventory.ini ansible/site.yml
```
3) Vérifiez les UIs (Kafka UI, Grafana, Prometheus, HDFS UI).

---

## Dépannage (FAQ)
- Kafka non prêt / consumer en attente:
  - Vérifier la santé du conteneur Kafka, la variable `BOOTSTRAP_SERVERS` et le réseau `infra-kafka`.
- Aucune donnée dans PostgreSQL:
  - Vérifier la clé `OPENWEATHER_API_KEY` et l’accès Internet; contrôler les logs du producer.
- Export HDFS échoue:
  - Vérifier `HDFS_URL` et l’UI du NameNode; attendre que WebHDFS soit up (le code attend automatiquement).
- Ingestion HDFS → Hive n'insère rien:
  - Vérifier la présence de dossiers `dt=YYYY-MM-DD` et de fichiers `.json` valides sous `HDFS_BASE_PATH`.
  - Vérifier `HIVE_HOST`/`HIVE_PORT` et l’accessibilité TCP à HiveServer2.
  - Consulter les logs du conteneur pour les erreurs PyHive/Thrift et de parsing JSON.


---

## Notes
- Les fichiers de rôles Ansible référencent des templates `.env.j2` non inclus: pensez à créer le `.env` à la main sur la cible ou à ajouter vos templates.
- Les volumes Docker persisteront les données Kafka/PostgreSQL/HDFS entre redéploiements.
- Le topic Kafka par défaut est `weather-api` (auto-création activée côté broker).

