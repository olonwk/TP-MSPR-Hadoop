# DOCUMENTATION TECHNIQUE FINALE — PIPELINE BIG DATA FINDATA SOLUTIONS
## Module DEPE855 | EPSI Mastère Expert en Ingénierie des données | 2025-2026

> **Version 2** — NiFi (remplace Sqoop pour Flux 1) | Kafka + Spark Structured Streaming (Flux 3) | Apache Hive (couche SQL sur production)

---

## 1. ARCHITECTURE GÉNÉRALE ET SCHÉMA DE FLUX

### 1.1 Vue d'ensemble

Le pipeline de données FinData Solutions est une solution hybride déployée sur un cluster Hadoop 3.x, combinant un traitement **batch quotidien** (Flux 1 et 2 via Airflow) et un traitement **streaming continu** (Flux 3 via Kafka + Spark Structured Streaming). Une couche Apache Hive expose les données de production via SQL aux équipes consommatrices.

```
╔═══════════════════════════════════════════════════════════════════════════════════════════╗
║                  PIPELINE BIG DATA — FINDATA SOLUTIONS  v2                               ║
║     Batch : Airflow (06h00) | Streaming : Spark Continuous (24h/24) | SQL : Hive         ║
╠═══════════════╦══════════════════╦══════════════════╦═════════════════╦══════════════════╗
║  SOURCES      ║  INGESTION       ║  HDFS /raw       ║  TRANSFORMATION ║  /prod + Hive    ║
╠═══════════════╬══════════════════╬══════════════════╬═════════════════╬══════════════════╣
║ PostgreSQL    ║ Apache NiFi      ║ /raw/trans/      ║ PySpark Job 1   ║ /prod/trans/     ║
║ (500K/j)      ║ QueryDatabase    ║ (Parquet)        ║ filtr+pseudo    ║ annee/mois/jour  ║
║               ║ TableRecord JDBC ║       ↓  ←dep    ║ +conv+aggr.     ║ → Hive:          ║
║               ║ → PutHDFS        ║                  ║                 ║ transactions_agg ║
╠═══════════════╬══════════════════╬══════════════════╬═════════════════╬══════════════════╣
║ SFTP CSV      ║ Shell script     ║ /raw/courses/    ║ PySpark Job 2   ║ /ref/cours/      ║
║ (1 fic/j)     ║ sftp+hdfs put    ║ (CSV brut)       ║ filtr+clôture   ║ date_partition   ║
║               ║                  ║       ↓          ║                 ║ → Hive:          ║
║               ║                  ║ /ref/cours/      ║                 ║ cours_boursiers  ║
╠═══════════════╬══════════════════╬══════════════════╬═════════════════╬══════════════════╣
║ JSON Logs     ║ Apache NiFi      ║  ┌─ Kafka ─────┐ ║ Spark           ║ /prod/logs/      ║
║ (continu)     ║ TailFile         ║  │ findata.     │ ║ Structured      ║ service/date_log ║
║               ║ +PublishKafka    ║  │ logs.raw     │ ║ Streaming       ║ → Hive:          ║
║               ║                  ║  └─────────────┘ ║ (30s batches)   ║ logs_erreurs     ║
╚═══════════════╩══════════════════╩══════════════════╩═════════════════╩══════════════════╝
```

### 1.2 Zones HDFS

| Zone | Chemin | Description | Rétention |
|------|--------|-------------|-----------|
| Raw | `/raw/transactions/`, `/raw/courses/` | Données brutes post-ingestion NiFi | 7 jours |
| Référentiel | `/ref/cours/` | Taux de change de clôture validés | 5 ans |
| Production | `/production/transactions/`, `/production/logs/` | Données transformées et pseudonymisées | 5 ans |
| Checkpoints | `/checkpoints/logs/` | État du streaming Spark (exactly-once) | Permanent |

---

## 2. DESCRIPTION DE CHAQUE COMPOSANT

### 2.1 Apache NiFi — Ingestion Flux 1 (PostgreSQL → HDFS)

**Rôle** : Extraction batch quotidienne des transactions bancaires depuis PostgreSQL, remplacement de Sqoop.

**Flow NiFi** : `QueryDatabaseTableRecord` → `UpdateAttribute` → `PutHDFS`

**Configuration clé** :

| Processor | Propriété clé | Valeur |
|-----------|--------------|--------|
| `QueryDatabaseTableRecord` | Connection Pool | DBCPConnectionPool (JDBC PostgreSQL) |
| | Table Name | `transactions` |
| | WHERE clause | `DATE(date_heure) = '${now():format("yyyy-MM-dd")}'` |
| | Record Writer | `ParquetRecordSetWriter` (Snappy) |
| | Scheduling | CRON `0 30 6 * * ?` (06h30 quotidien) |
| `PutHDFS` | Directory | `/raw/transactions/date=${now():format("yyyy-MM-dd")}/` |
| | Replication | `3` |

**Avantage vs Sqoop** : Sqoop est archivé (fin de support 2017). NiFi offre une interface de monitoring, des retries natifs, et une Dead Letter Queue pour les FlowFiles en erreur.

---

### 2.2 Script Shell SFTP — Ingestion Flux 2

**Rôle** : Téléchargement du fichier CSV de cours boursiers depuis SFTP et chargement HDFS.

```bash
sftp -i /secure/keys/sftp_rsa_key sftp_reader@sftp.findata.internal:/data/courses/${DATE}.csv /tmp/
md5sum /tmp/courses_${DATE}.csv   # vérification intégrité
hdfs dfs -put -f /tmp/courses_${DATE}.csv /raw/courses/${DATE}.csv
```

---

### 2.3 Apache NiFi — Ingestion Flux 3 vers Kafka

**Rôle** : Collecte continue des logs JSON et publication dans le topic Kafka `findata.logs.raw`.

**Flow NiFi** : `TailFile` → `SplitJson` → `PublishKafkaRecord_2_6`

| Processor | Propriété clé | Valeur |
|-----------|--------------|--------|
| `TailFile` | Files to Tail | `/var/log/findata/apps/*.json` |
| | Rolling Filename Pattern | `${filename}.${now():format('yyyy-MM-dd')}` |
| `SplitJson` | JsonPath Expression | `$.*` (1 FlowFile = 1 log) |
| `PublishKafkaRecord_2_6` | Kafka Brokers | `kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092` |
| | Topic Name | `findata.logs.raw` |
| | Record Writer | `JsonRecordSetWriter` |

**Cluster Kafka** : 3 brokers, `replication.factor=2`, `num.partitions=4` pour le topic `findata.logs.raw`.

---

### 2.4 Apache Kafka — Bus de messages Flux 3

**Rôle** : Découplage entre le producteur (NiFi) et le consommateur (Spark Streaming). Tampon de messages en cas d'indisponibilité de Spark.

**Configuration topic** :
```bash
kafka-topics.sh --create \
  --bootstrap-server kafka-broker1:9092 \
  --topic findata.logs.raw \
  --partitions 4 \
  --replication-factor 2 \
  --config retention.ms=86400000 \   # Rétention 24h
  --config max.message.bytes=1048576  # Max 1 Mo par message
```

---

### 2.5 Apache Spark 3.x — Jobs de transformation

**Batch (Flux 1 et 2)** : exécutés quotidiennement par Airflow via `SparkSubmitOperator`.

**Streaming (Flux 3)** : service long-running démarré comme application YARN.

```bash
# Démarrage du streaming Flux 3 (service continu)
spark-submit --master yarn --deploy-mode cluster \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --conf spark.sql.catalogImplementation=hive \
  flux3_logs_streaming.py
```

**Configuration autoscaling** (commune aux 3 jobs) :
```
spark.dynamicAllocation.enabled       = true
spark.dynamicAllocation.minExecutors  = 2
spark.dynamicAllocation.maxExecutors  = 10
spark.dynamicAllocation.initialExecutors = 4
spark.shuffle.service.enabled         = true
```

---

### 2.6 Apache Hive — Couche SQL sur les données de production

**Rôle** : Expose les données Parquet de HDFS via SQL (HiveQL) pour les équipes B.I. et Data Science sans connaissance de HDFS.

**Tables créées** (voir `hive_tables.sql`) :

| Table Hive | Source HDFS | Partition | Utilisateurs |
|------------|-------------|-----------|--------------|
| `findata.transactions_agg` | `/production/transactions/` | annee/mois/jour | B.I. (PowerBI), Data Science |
| `findata.cours_boursiers` | `/ref/cours/` | date_partition | Data Science, Conformité |
| `findata.logs_erreurs` | `/production/logs/` | service/date_log | Équipe technique, Data Science |

**Accès PowerBI** : connexion HiveServer2 via connecteur ODBC `Microsoft Hive ODBC Driver`.
```
Server: hiveserver2.findata.internal
Port: 10000
Database: findata
Authentication: Username/Password
```

**Mise à jour des partitions** : `MSCK REPAIR TABLE findata.<table>` exécuté par Airflow après chaque job Spark (ou déclenché par `foreachBatch` dans le streaming).

---

### 2.7 Apache Airflow — Orchestration batch

**DAG** : `findata_pipeline_quotidien_v2` — schedule `0 6 * * *`

**Séquencement** :
```
check_flux2_hdfs ──(ok)──────────────────────────────┐
check_flux2_hdfs ──(fail)──→ fallback_cours_veille ──┘
                              ↓
                         transform_flux2 → repair_hive_cours ──┐
                                                                ↓
check_flux1_hdfs ──────────────────────────→ transform_flux1 → repair_hive_trans ──┐
                                                                                    ├→ notify_success
check_kafka_flux3 ─────────────────────────────────────→ repair_hive_logs ──────────┘
```

---

## 3. RÈGLES DE TRANSFORMATION PAR FLUX

### Flux 1 — Transactions bancaires

| # | Règle | Implémentation |
|---|-------|----------------|
| 1 | Filtrage : montant > 0 ET statut non NULL | `.filter((col("montant") > 0) & col("statut").isNotNull())` |
| 2 | Pseudonymisation IBAN → SHA-256(sel + iban) | `sha2(concat(lit(SALT), col("iban")), 256)` |
| 3 | Pseudonymisation client_id → séquentiel anonyme | `dense_rank().over(Window.orderBy("client_id"))` |
| 4 | Conversion devises → EUR via référentiel Flux 2 | `col("montant") / col("taux_conversion")` |
| 5 | Agrégation par code_banque + jour | `groupBy(...).agg(sum, avg, count, count(FRAUD))` |
| 6 | Partition HDFS + Hive REPAIR | `.partitionBy("annee","mois","jour")` + MSCK REPAIR |

### Flux 2 — Cours boursiers

| # | Règle | Implémentation |
|---|-------|----------------|
| 1 | Filtrage sources autorisées | `.filter(col("source").isin(["ECB","Bloomberg","Reuters"]))` |
| 2 | Taux de clôture uniquement | `row_number().over(Window.partitionBy("devise").orderBy(desc("horodatage")))` |
| 3 | Partition HDFS + Hive REPAIR | `.partitionBy("date_partition")` + MSCK REPAIR |

### Flux 3 — Logs applicatifs (Streaming)

| # | Règle | Implémentation |
|---|-------|----------------|
| 1 | Lecture Kafka (topic findata.logs.raw) | `readStream.format("kafka")` |
| 2 | Filtrage ERROR/CRITICAL | `.filter(col("level").isin(["ERROR","CRITICAL"]))` |
| 3 | Extraction codes métier ERR-XXXX | `regexp_extract(col("message"), r"ERR-[0-9]{4}", 0)` |
| 4 | Agrégation fenêtre 1h (watermark 5 min) | `withWatermark(...,"5 min").groupBy(window(...,"1 hour"),...)` |
| 5 | Écriture HDFS micro-batch 30s | `.writeStream.trigger(processingTime="30 seconds")` |

---

## 4. PROCÉDURE DE MAINTENANCE

### 4.1 Planification des jobs

| Tâche | Déclenchement | Heure | Outil |
|-------|--------------|-------|-------|
| Pipeline batch complet | Automatique | 06h00 UTC | Airflow DAG |
| Backfill en cas d'échec | Manuel | À la demande | `airflow dags backfill` |
| Redémarrage Flux 3 Streaming | Si job YARN mort | À la demande | `spark-submit` |
| Vérification alertes YARN | Automatique | Toutes les 10 min | Script cron |
| Vérification stockage HDFS | Automatique | Toutes les 30 min | Script cron |

### 4.2 Purge HDFS

```bash
# Purge données raw > 7 jours (cron quotidien 02h00)
DATE_LIMITE=$(date -d "-7 days" +%Y-%m-%d)
hdfs dfs -ls /raw/transactions/ | awk '{print $8}' | \
  while read dir; do
    date_dir=$(echo "$dir" | grep -oP '\d{4}-\d{2}-\d{2}')
    if [[ "$date_dir" < "$DATE_LIMITE" ]]; then
      hdfs dfs -rm -r "$dir"
      echo "Supprimé : $dir"
    fi
  done

# Nettoyage checkpoints Spark Streaming > 30 jours
hdfs dfs -ls /checkpoints/logs/ | awk 'NR>1{print $8}' | \
  xargs -I{} hdfs dfs -rm -r -skipTrash {}
```

### 4.3 Gestion du streaming Kafka

```bash
# Vérifier l'état du consumer group Spark Streaming
kafka-consumer-groups.sh \
  --bootstrap-server kafka-broker1:9092 \
  --describe --group findata-spark-streaming-logs

# Lag acceptable : < 10 000 messages (sinon le streaming prend du retard)
# Si lag > 10 000 → augmenter maxExecutors dans la config Spark

# Vérifier les partitions du topic
kafka-topics.sh --describe \
  --bootstrap-server kafka-broker1:9092 \
  --topic findata.logs.raw
```

---

## 5. GUIDE D'UTILISATION PAR ÉQUIPE

### Équipe Data Science

**Accès via PySpark (recommandé)** :
```python
spark = SparkSession.builder \
    .appName("DS-Analysis") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Option 1 : SQL via Hive (simple)
df = spark.sql("SELECT * FROM findata.transactions_agg WHERE annee=2024 AND mois=1")

# Option 2 : lecture directe HDFS (contrôle total)
df = spark.read.parquet("/production/transactions/") \
         .filter((col("annee") == 2024) & (col("mois") == 1))
```

**Accès aux logs en quasi temps réel** : les logs ERROR/CRITICAL sont disponibles dans Hive dans un délai de ~30 secondes (latence Spark Streaming micro-batch).

---

### Cellule B.I. (PowerBI)

**Connexion HiveServer2** : Menu PowerBI → `Obtenir des données` → `Hive LLAP` ou `ODBC`.

```
Driver: Microsoft Hive ODBC Driver
Host: hiveserver2.findata.internal:10000
Database: findata
Auth: Username + Password
```

**Tables disponibles dans PowerBI** : `findata.transactions_agg`, `findata.cours_boursiers`, `findata.logs_erreurs`.

**Rafraîchissement** : planifier le refresh à 08h00 (pipeline batch terminé + partitions Hive réparées).

---

### Service Conformité & Audit

**Rejouer l'historique via HiveQL** :
```sql
-- Volume mensuel des 5 dernières années pour DSP2
SELECT annee, mois,
       SUM(nb_transactions)  AS transactions_mois,
       SUM(volume_total_eur) AS volume_mois_eur,
       SUM(nb_fraudes)       AS fraudes_mois
FROM findata.transactions_agg
WHERE annee BETWEEN 2020 AND 2024
GROUP BY annee, mois
ORDER BY annee, mois;
```

**Traçabilité** : chaque fichier Parquet embarque les métadonnées `date_traitement`, `version_job`, `taux_source` (LIVE ou FALLBACK).

---

## 6. DEUX AMÉLIORATIONS PRIORITAIRES

### Amélioration 1 — Data Quality avec Great Expectations

**Problématique** : les règles de qualité (montant > 0, sources autorisées, schéma JSON valide) sont codées en dur dans les jobs Spark. Toute nouvelle règle = modification du code. Aucune mesure systématique du taux de rejet n'est exposée aux équipes métier.

**Solution** : intégrer **Great Expectations (GX)** comme couche de Data Quality :
- Définir des "expectation suites" par flux (ex: `montant.min > 0`, `statut.not_null_percentage > 99%`)
- Exécuter les validations GX avant et après chaque job Spark (dans le DAG Airflow)
- Publier un portail **Data Docs** accessible aux équipes métier (taux de rejet, dérive statistique)
- Intégrer les résultats GX dans les métadonnées Hive (`TBLPROPERTIES`)

**Impact** : visibilité complète sur la qualité, détection précoce des anomalies, réduction du debug de 70%. Effort : 3 semaines.

---

### Amélioration 2 — Catalogue de données Apache Atlas

**Problématique** : avec 3 flux, 5 tables Hive, et plusieurs équipes, il n'existe pas de catalogue centralisé décrivant la provenance des données (lineage), leur signification métier, leur classification (CONFIDENTIEL, INTERNE) et les contacts responsables. Les data scientists doivent interroger les data engineers pour comprendre le schéma et l'origine de chaque colonne.

**Solution** : déployer **Apache Atlas** (intégré à l'écosystème Hadoop) :
- **Lineage automatique** : Atlas détecte les transformations Spark et Hive et construit le graphe de provenance `PostgreSQL → NiFi → HDFS → Hive → PowerBI`
- **Glossaire métier** : définitions des termes (`nb_fraudes`, `iban_hash`, `taux_eur`) en langage métier
- **Classification RGPD** : tagguer automatiquement les colonnes pseudonymisées (`iban_hash` → TAG: PII_PSEUDONYMIZED)
- **Intégration Hive** : Atlas s'intègre directement avec le Hive Metastore existant

**Impact** : autonomie des équipes consommatrices, conformité RGPD renforcée (inventaire DCP automatique), onboarding des nouveaux data scientists accéléré. Effort : 4 semaines.

---

## 7. RÉPONSES AUX QUESTIONS DE RÉFLEXION (S4)

### Question 1 — Passage au temps réel pour le Flux 1 (détection de fraude < 5 secondes)

**Réponse :**

Avec l'architecture v2, le Flux 3 (logs) est déjà en streaming via Kafka. Pour passer le Flux 1 (transactions) en temps réel pour la détection de fraude :

**Composants à remplacer :**

1. **NiFi `QueryDatabaseTableRecord` (batch) → Debezium + Kafka (CDC)**
   - Debezium est un connecteur Kafka Connect qui lit le Write-Ahead Log (WAL) de PostgreSQL
   - Chaque `INSERT` dans la table `transactions` génère immédiatement un message dans le topic Kafka `findata.transactions.raw`
   - Latence : < 500ms (vs 24h pour le batch NiFi)

2. **PySpark batch (`flux1_transactions.py`) → Spark Structured Streaming**
   - Même code, mais avec `readStream.format("kafka")` au lieu de `spark.read.parquet()`
   - Application d'un modèle ML de scoring fraude sur des fenêtres de 5 secondes
   - Sortie vers un topic Kafka `findata.fraud.alerts` consommé par le service de notification

**Ce qui reste inchangé** : HDFS, Hive, les Flux 2 et 3, Airflow pour les agrégats B.I. quotidiens.

---

### Question 2 — Disponibilité continue en cas de panne d'un nœud Hadoop

**Réponse :**

| Mécanisme | Configuration | Protection |
|-----------|--------------|------------|
| **HDFS NameNode HA** | 2 NameNodes (Active/Standby) + 3 JournalNodes + ZooKeeper | Failover automatique < 30s |
| **HDFS Réplication × 3** | `dfs.replication=3` | Panne d'un DataNode transparente pour les lectures |
| **YARN ResourceManager HA** | 2 RM + ZooKeeper | Failover automatique des jobs YARN |
| **Kafka réplication** | `replication.factor=2` sur `findata.logs.raw` | Panne d'un broker Kafka sans perte de messages |
| **Spark task retry** | `spark.task.maxFailures=4` | Relance des tâches sur executor tombé |
| **Spark Streaming checkpoint** | `/checkpoints/logs/` sur HDFS | Reprise exactement là où le streaming s'est arrêté |
| **Airflow retry** | `retries=2, retry_delay=10min` | Relance automatique des jobs batch |

**Cas critique — panne du NameNode actif** : ZooKeeper détecte l'absence de heartbeat en ~10s, déclenche le failover vers le Standby NameNode en ~20s. Les DataNodes redirigent automatiquement vers le nouveau NameNode actif. Durée d'indisponibilité totale : < 60 secondes.

**Cas Kafka — panne d'un broker** : le partition leader est réélu parmi les replicas en < 10s. Les producteurs NiFi et les consommateurs Spark se reconnectent automatiquement via la liste de brokers configurée (`kafka.bootstrap.servers` liste les 3 brokers).
