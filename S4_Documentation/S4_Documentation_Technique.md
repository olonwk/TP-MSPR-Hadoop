# DOCUMENTATION TECHNIQUE FINALE — PIPELINE BIG DATA FINDATA SOLUTIONS
## Module DEPE855 | EPSI Mastère Expert en Ingénierie des données | 2025-2026

---

## 1. ARCHITECTURE GÉNÉRALE ET SCHÉMA DE FLUX

### 1.1 Vue d'ensemble

Le pipeline de données FinData Solutions est une solution de traitement batch quotidien déployée sur un cluster Hadoop 3.x. Il collecte, transforme et met à disposition des données issues de trois sources hétérogènes pour trois équipes consommatrices internes.

```
╔════════════════════════════════════════════════════════════════════════════╗
║              PIPELINE BIG DATA — FINDATA SOLUTIONS                        ║
║           Apache Airflow | DAG quotidien | 06h00 UTC                      ║
╠════════════╦══════════════╦════════════════╦═══════════════╦══════════════╗
║  SOURCES   ║  INGESTION   ║  HDFS /raw     ║  TRANSFORM.   ║  /production ║
╠════════════╬══════════════╬════════════════╬═══════════════╬══════════════╣
║ PostgreSQL ║ Apache Sqoop ║ /raw/trans/    ║ PySpark Job1  ║ /prod/trans/ ║
║ (500K/j)  ║ JDBC batch   ║                ║ (filtr+pseudo ║ annee/mois/j ║
║           ║              ║                ║ +conv+agg)    ║ (Parquet)    ║
╠════════════╬══════════════╬════════════════╬═══════════════╬══════════════╣
║ SFTP CSV  ║ Shell script ║ /raw/courses/  ║ PySpark Job2  ║ /ref/cours/  ║
║ (1 fic/j) ║ sftp+hdfsput ║                ║ (filtr+clôt.) ║ date_part.   ║
║           ║              ║                ║               ║ (Parquet)    ║
╠════════════╬══════════════╬════════════════╬═══════════════╬══════════════╣
║ JSON Logs ║ Apache NiFi  ║ /staging/logs/ ║ PySpark Job3  ║ /prod/logs/  ║
║ (200K/j)  ║ GetFile      ║                ║ (filtr+regex  ║ service/date ║
║           ║              ║                ║ +comptage)    ║ (Parquet)    ║
╚════════════╩══════════════╩════════════════╩═══════════════╩══════════════╝
```

### 1.2 Zones HDFS

| Zone | Chemin | Description | Rétention |
|------|--------|-------------|-----------|
| Raw | `/raw/` | Données brutes post-ingestion | 7 jours |
| Staging | `/staging/` | Zone tampon NiFi pour logs | 3 jours |
| Référentiel | `/ref/` | Taux de change validés | 5 ans |
| Production | `/production/` | Données transformées et pseudonymisées | 5 ans |

---

## 2. DESCRIPTION DE CHAQUE COMPOSANT

### 2.1 Apache Sqoop — Ingestion Flux 1

**Rôle** : Extraction batch quotidienne des transactions bancaires depuis PostgreSQL vers HDFS.

**Configuration clé** :
```bash
sqoop import \
  --connect jdbc:postgresql://pg-findata.internal:5432/findata_db \
  --username findata_sqoop_reader \
  --password-file /secure/credentials/sqoop_pg_password \
  --table transactions \
  --where "DATE(date_heure) = '${DATE}'" \
  --target-dir /raw/transactions/date=${DATE} \
  --as-parquetfile \
  --compress --compression-codec snappy \
  --num-mappers 4
```

**Points clés** : 4 mappers parallèles, extraction filtrée par date pour éviter les doublons, format Parquet natif, authentification par fichier sécurisé (pas de mot de passe en clair).

---

### 2.2 Script Shell SFTP — Ingestion Flux 2

**Rôle** : Téléchargement du fichier CSV de cours boursiers depuis le serveur SFTP et chargement dans HDFS.

**Configuration clé** :
```bash
sftp -i /secure/keys/sftp_rsa_key sftp_reader@sftp.findata.internal:/data/courses/${DATE}.csv /tmp/
hdfs dfs -put -f /tmp/courses_${DATE}.csv /raw/courses/${DATE}.csv
```

**Points clés** : Vérification MD5 de l'intégrité du fichier, exit code non nul si fichier absent (déclenchement du fallback Airflow), nettoyage du fichier temporaire local après chargement.

---

### 2.3 Apache NiFi — Ingestion Flux 3

**Rôle** : Collecte continue des logs JSON depuis les applications vers la zone de staging HDFS.

**Flow NiFi** :
```
[GetFile / ListenHTTP] → [UpdateAttribute] → [RouteOnAttribute (filter JSON valide)]
                       → [PutHDFS → /staging/logs/]
```

**Configuration clé** :
- Processor `GetFile` : répertoire source `/var/log/findata/apps/`, polling toutes les 60s
- Processor `PutHDFS` : répertoire cible `/staging/logs/`, format de fichier `${filename}-${now():format('yyyyMMddHHmmss')}.json`
- `RouteOnAttribute` : valide la présence des champs `timestamp`, `service`, `level`, `message`

---

### 2.4 Apache Spark 3.x (PySpark) — Transformation des 3 flux

**Rôle** : Nettoyage, transformation et agrégation des données selon les règles métier.

**Configuration commune** :
```
spark.dynamicAllocation.enabled    = true
spark.dynamicAllocation.minExecutors  = 2
spark.dynamicAllocation.maxExecutors  = 10
spark.dynamicAllocation.initialExecutors = 4
spark.shuffle.service.enabled      = true
```

**Paramètre d'exécution** : chaque job reçoit la date de traitement en argument CLI (`sys.argv[1]`), garantissant l'idempotence et la possibilité de backfill.

---

### 2.5 Apache Airflow — Orchestration

**Rôle** : Planification, séquencement et surveillance des tâches du pipeline.

**DAG** : `findata_pipeline_quotidien` — schedule `0 6 * * *`

**Séquencement** :
```
ingest_flux2 → transform_flux2 ──┐
                                  ├→ transform_flux1
ingest_flux1 ────────────────────┘
check_flux3  → transform_flux3
[transform_flux1, transform_flux3] → notify_success
```

**SLA** : 45 minutes par tâche — dépassement → callback Slack.

---

## 3. RÈGLES DE TRANSFORMATION PAR FLUX

### Flux 1 — Transactions bancaires

| # | Règle | Implémentation PySpark |
|---|-------|----------------------|
| 1 | Supprimer montant ≤ 0 ou statut NULL | `.filter((col("montant") > 0) & col("statut").isNotNull())` |
| 2 | Pseudonymiser IBAN → SHA-256(sel + iban) | `sha2(concat(lit(SALT), col("iban")), 256)` |
| 3 | Pseudonymiser client_id → séquentiel anonyme | `dense_rank().over(Window.orderBy("client_id"))` |
| 4 | Convertir montants en EUR | `col("montant") / col("taux_conversion")` (join avec référentiel Flux 2) |
| 5 | Agréger par code_banque + jour | `groupBy(...).agg(sum, avg, count, count(FRAUD))` |
| 6 | Partitionner par année/mois/jour | `.partitionBy("annee", "mois", "jour")` |

### Flux 2 — Cours boursiers

| # | Règle | Implémentation PySpark |
|---|-------|----------------------|
| 1 | Filtrer sources non autorisées | `.filter(col("source").isin(["ECB", "Bloomberg", "Reuters"]))` |
| 2 | Conserver taux de clôture uniquement | `row_number().over(Window.partitionBy("devise").orderBy(desc("horodatage"))) == 1` |
| 3 | Partitionner par date | `.partitionBy("date_partition")` |

### Flux 3 — Logs applicatifs

| # | Règle | Implémentation PySpark |
|---|-------|----------------------|
| 1 | Filtrer ERROR et CRITICAL | `.filter(col("level").isin(["ERROR", "CRITICAL"]))` |
| 2 | Extraire codes d'erreur (ERR-XXXX) | `regexp_extract(col("message"), r"ERR-[0-9]{4}", 0)` |
| 3 | Compter par service et heure | `groupBy("service", "date_log", "heure", "code_erreur").agg(count("*"))` |
| 4 | Partitionner par service et date | `.partitionBy("service", "date_log")` |

---

## 4. PROCÉDURE DE MAINTENANCE

### 4.1 Planification des jobs

| Tâche | Fréquence | Heure | Responsable |
|-------|-----------|-------|-------------|
| Pipeline complet (DAG Airflow) | Quotidien | 06h00 UTC | Automatique |
| Backfill en cas d'échec | À la demande | — | Data Engineer on-call |
| Vérification des alertes YARN | Automatique | Toutes les 10 min | Script cron |
| Vérification stockage HDFS | Automatique | Toutes les 30 min | Script cron |

Commande de backfill Airflow :
```bash
airflow dags backfill findata_pipeline_quotidien \
  --start-date 2024-01-14 \
  --end-date 2024-01-14 \
  --reset-dagruns
```

### 4.2 Purge HDFS

```bash
# Purge des données raw (> 7 jours)
hdfs dfs -ls /raw/transactions/ | \
  awk '{print $8}' | \
  while read dir; do
    date_dir=$(echo "$dir" | grep -oP '\d{4}-\d{2}-\d{2}')
    if [[ $(date -d "$date_dir" +%s) -lt $(date -d "-7 days" +%s) ]]; then
      hdfs dfs -rm -r "$dir"
    fi
  done

# Purge staging logs (> 3 jours)
find /staging/logs/ -name "*.json" -mtime +3 -delete
```

Planification recommandée : cron quotidien à 02h00.

### 4.3 Rotation des logs Spark / Airflow

```bash
# Logs YARN (applications terminées > 7 jours)
yarn logs --applicationId <appId> > /archive/yarn_logs/
# Nettoyage automatique via yarn.log-aggregation.retain-seconds=604800 (yarn-site.xml)

# Logs Airflow
# Dans airflow.cfg :
# log_retention_days = 30
```

---

## 5. GUIDE D'UTILISATION PAR ÉQUIPE

### Équipe Data Science

**Accès aux données** : `/production/transactions/` (agrégats) pour les statistiques bancaires, `/production/logs/` pour les patterns d'erreurs.

**Lecture PySpark recommandée** :
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DS-Analysis").getOrCreate()

# Toujours filtrer par partition en premier (performance)
df_jan = spark.read.parquet("/production/transactions/") \
              .filter((col("annee") == 2024) & (col("mois") == 1))

# Données de fraude pour entraînement modèle
df_fraude = df_jan.select("code_banque", "annee", "mois", "jour",
                           "nb_fraudes", "nb_transactions",
                           (col("nb_fraudes") / col("nb_transactions")).alias("taux_fraude"))
```

**Note** : Les données sont pseudonymisées. Les colonnes `iban` et `client_id` originaux ne sont pas disponibles en production (conformité RGPD). Pour les besoins de traçabilité, contacter le service Conformité & Audit.

---

### Cellule B.I. (PowerBI)

**Connexion PowerBI** : utiliser le connecteur "Azure HDInsight" ou "Parquet" en pointant vers le chemin HDFS exposé via WebHDFS (`http://namenode:9870/webhdfs/v1/production/transactions/`).

**Tables disponibles** :
- `transactions_agg` : agrégats quotidiens par code_banque (volume, montant moyen, nb fraudes)
- `logs_erreurs` : comptages horaires d'erreurs par service

**Rafraîchissement** : planifier le refresh PowerBI à 08h00 (pipeline terminé à 07h30 au plus tard).

---

### Service Conformité & Audit

**Rejouer l'historique** : les données de production sont partitionnées et conservées 5 ans. Pour rejouer une période :

```python
df_audit = spark.read.parquet("/production/transactions/") \
                .filter((col("annee") >= 2020) & (col("annee") <= 2024))
```

**Traçabilité** : chaque fichier Parquet embarque les métadonnées `date_traitement`, `version_job`, `taux_source` (LIVE ou FALLBACK). En cas de demande RGPD, les `iban_hash` permettent de vérifier la présence d'un IBAN spécifique sans exposer les données brutes.

**Accès au sel de hachage** : sur demande formalisée au RSSI, via le coffre-fort HashiCorp Vault (production).

---

## 6. DEUX AMÉLIORATIONS PRIORITAIRES (6 PROCHAINS MOIS)

### Amélioration 1 — Passage au streaming temps réel pour la détection de fraude (Flux 1)

**Problématique actuelle** : Le pipeline batch quotidien génère un délai de 12 à 24h entre une transaction frauduleuse et sa détection par l'équipe Data Science. Or, la réglementation DSP2 impose une réponse en temps réel pour les alertes de fraude.

**Solution proposée** : Intégrer Apache Kafka entre PostgreSQL et Spark :
1. **Debezium** (CDC — Change Data Capture) capture chaque nouvelle transaction PostgreSQL en temps réel et la publie dans un topic Kafka `findata.transactions`.
2. **Spark Structured Streaming** remplace le job batch, consommant le topic Kafka et appliquant un modèle de scoring de fraude ML (MLlib) sur des fenêtres glissantes de 5 secondes.
3. Les transactions suspectes sont publiées dans un topic Kafka `findata.alerts` et consommées par le service de notification.

**Impact attendu** : réduction du délai de détection de fraude de **24h à < 5 secondes**. Conformité DSP2 renforcée. Le pipeline batch existant (Flux 1) est maintenu pour les agrégats B.I. quotidiens.

**Effort estimé** : 6 semaines (1 data engineer + 1 data scientist pour le modèle ML streaming).

---

### Amélioration 2 — Mise en place d'un Data Quality Framework (Great Expectations)

**Problématique actuelle** : Les règles de qualité (montant > 0, statut non NULL, sources autorisées) sont codées en dur dans les jobs Spark. Toute nouvelle règle métier nécessite une modification du code, un déploiement et un redémarrage du pipeline. De plus, aucune **mesure systématique** de la qualité des données (taux de rejet, dérive statistique) n'est disponible pour les équipes métier.

**Solution proposée** : Intégrer **Great Expectations (GX)** comme couche de Data Quality :
1. Définir des "expectation suites" pour chaque flux (ex: `montant.min > 0`, `statut.not_null_percentage > 99%`, `devise.values_in_set ["EUR", "USD", ...]`).
2. Exécuter les validations GX **avant** les jobs Spark (en early warning) et **après** (en data contract).
3. Publier les résultats de validation dans un **Data Docs** (portail HTML statique) accessible aux équipes métier.
4. Intégrer GX dans le DAG Airflow comme `PythonOperator` avec échec automatique si une expectation critique n'est pas respectée.

**Impact attendu** : visibilité complète sur la qualité des données pour toutes les équipes, détection précoce des anomalies (ex: source SFTP qui commence à envoyer des devises inconnues), réduction du temps de debug de 70%.

**Effort estimé** : 3 semaines (1 data engineer).

---

## 7. RÉPONSES AUX QUESTIONS DE RÉFLEXION (S4)

### Question 1 — Passage au temps réel pour le Flux 1 (détection de fraude < 5 secondes)

**Réponse :**

Deux composants majeurs devraient être **remplacés** :

**1. Apache Sqoop → Apache Kafka + Debezium (CDC)**

Sqoop est un outil de transfert batch (extraction ponctuelle). Pour un traitement temps réel, il faut capturer chaque transaction **au moment de son insertion dans PostgreSQL**, sans attendre la fin de la journée. Debezium est un connecteur Kafka Connect qui lit le Write-Ahead Log (WAL) de PostgreSQL et publie chaque `INSERT/UPDATE` de la table `transactions` dans un topic Kafka `findata.transactions.raw` en quelques millisecondes.

**2. Spark Batch (spark-submit ponctuel) → Spark Structured Streaming**

Le job `spark-submit` actuel est exécuté une fois par jour. Spark Structured Streaming remplace ce mécanisme par un job **continu** qui consomme le topic Kafka en micro-batches (toutes les secondes) ou en mode continu. Le job applique les mêmes transformations (filtrage, pseudonymisation, conversion devises) sur des micro-batches, puis alimente un topic Kafka `findata.alerts` pour les transactions classées FRAUD.

**Ce qui peut rester inchangé** : HDFS pour le stockage historique, le référentiel de cours boursiers (enrichissement), les jobs Flux 2 et 3 (pas de contrainte temps réel), Airflow pour les agrégats B.I. quotidiens.

**Architecture cible** :
```
PostgreSQL → Debezium → Kafka (topic: transactions.raw)
           → Spark Structured Streaming (scoring ML < 1s)
           → Kafka (topic: fraud.alerts) → Service de notification
```

---

### Question 2 — Garantir la disponibilité continue du pipeline en cas de panne d'un nœud Hadoop

**Réponse :**

La haute disponibilité d'un cluster Hadoop 3.x repose sur plusieurs mécanismes complémentaires à activer :

**1. HDFS NameNode HA (High Availability)**

Le NameNode est le point unique de défaillance d'HDFS. Avec HDFS HA :
- **2 NameNodes** : un actif (Active NN) et un standby (Standby NN)
- **3 JournalNodes** : synchronisent les edit logs entre les deux NameNodes en temps réel
- **Apache ZooKeeper** : détecte la panne du NN actif (heartbeat) et déclenche le failover automatique vers le standby en **< 30 secondes**

```xml
<!-- hdfs-site.xml -->
<property><name>dfs.nameservices</name><value>findata-cluster</value></property>
<property><name>dfs.ha.automatic-failover.enabled.findata-cluster</name><value>true</value></property>
<property><name>dfs.replication</name><value>3</value></property>
```

**2. HDFS DataNode — Réplication × 3**

Chaque bloc HDFS est répliqué sur 3 DataNodes différents (réplication factor = 3, défaut). En cas de panne d'un DataNode, HDFS redirige automatiquement les lectures vers les autres répliques. Le NameNode détecte l'absence de heartbeat du DataNode en 10 minutes et lance une re-réplication des blocs sous-répliqués.

**3. YARN ResourceManager HA**

```xml
<!-- yarn-site.xml -->
<property><name>yarn.resourcemanager.ha.enabled</name><value>true</value></property>
<property><name>yarn.resourcemanager.ha.rm-ids</name><value>rm1,rm2</value></property>
<property><name>yarn.resourcemanager.recovery.enabled</name><value>true</value></property>
```

2 ResourceManagers (actif/standby) avec ZooKeeper pour le failover automatique. Les NodeManagers continuent d'exécuter les containers en cours si le RM tombe.

**4. Spark — Tolérance aux pannes des executors**

```
spark.task.maxFailures = 4       # Retry 4x une tâche échouée
spark.speculation      = true    # Lancer des copies des tâches lentes
```

Si un executor tombe en cours de job, YARN libère le container et Spark redemande un executor de remplacement. Les tâches en cours sur l'executor tombé sont relancées automatiquement sur un autre executor.

**5. Airflow — Retry des DAG tasks**

Les tâches Airflow sont configurées avec `retries=2, retry_delay=10min`. Si un job Spark échoue (panne transitoire d'un nœud), Airflow retentera automatiquement sans intervention humaine.

**Résultat** : avec ces mécanismes activés, la panne d'un DataNode ou d'un NodeManager est **transparente** pour le pipeline — aucune donnée n'est perdue et le job continue sur les nœuds disponibles. Seule la panne simultanée de la majorité des nœuds (> 50% des DataNodes) nécessite une intervention manuelle.
