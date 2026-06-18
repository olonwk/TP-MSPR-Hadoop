# Configuration Apache NiFi — Flux 1 : Ingestion PostgreSQL → HDFS
## FinData Solutions | DEPE855

Ce document décrit la configuration du flow NiFi remplaçant Apache Sqoop pour l'ingestion
des transactions bancaires depuis PostgreSQL vers HDFS.

---

## Flow NiFi — Vue d'ensemble

```
[QueryDatabaseTableRecord] → [UpdateAttribute] → [PutHDFS]
         ↓ (failure)
   [LogAttribute] → [PutFile (dead-letter)]
```

---

## Processor 1 — QueryDatabaseTableRecord

**Rôle** : Exécute une requête JDBC sur PostgreSQL et produit un FlowFile au format Parquet.

| Propriété | Valeur |
|-----------|--------|
| Database Connection Pooling Service | `DBCPConnectionPool-PostgreSQL` |
| Database Type | PostgreSQL |
| Table Name | `transactions` |
| Columns to Return | `transaction_id, client_id, iban, montant, devise, date_heure, code_banque, statut` |
| Additional WHERE clause | `DATE(date_heure) = '${literal(${now():format('yyyy-MM-dd')}):urlDecode()}'` |
| Record Writer | `ParquetRecordSetWriter` |
| Fetch Size | `10000` |
| Max Rows Per Flow File | `100000` |
| Scheduling | Run Schedule: `0 30 6 * * ?` (CRON — tous les jours à 06h30) |

### DBCPConnectionPool — Configuration JDBC

| Propriété | Valeur |
|-----------|--------|
| Database Connection URL | `jdbc:postgresql://pg-findata.internal:5432/findata_db` |
| Database Driver Class Name | `org.postgresql.Driver` |
| Database Driver Location(s) | `/opt/nifi/lib/postgresql-42.7.0.jar` |
| Database User | `findata_nifi_reader` |
| Password | *(via NiFi Sensitive Properties Key ou HashiCorp Vault)* |
| Max Total Connections | `8` |

### ParquetRecordSetWriter — Configuration

| Propriété | Valeur |
|-----------|--------|
| Schema Access Strategy | `Infer Schema` |
| Compression | `SNAPPY` |

---

## Processor 2 — UpdateAttribute

**Rôle** : Nomme le fichier de sortie avec la date de traitement.

| Propriété | Valeur |
|-----------|--------|
| `filename` | `transactions_${now():format('yyyy-MM-dd')}_${UUID()}.parquet` |
| `hdfs.target.dir` | `/raw/transactions/date=${now():format('yyyy-MM-dd')}/` |

---

## Processor 3 — PutHDFS

**Rôle** : Écrit le fichier Parquet dans HDFS.

| Propriété | Valeur |
|-----------|--------|
| Hadoop Configuration Resources | `/etc/hadoop/conf/core-site.xml, /etc/hadoop/conf/hdfs-site.xml` |
| Directory | `${hdfs.target.dir}` |
| Conflict Resolution Strategy | `replace` |
| Block Size | `134217728` (128 Mo) |
| Replication | `3` |

---

## Processor 4 (failure) — LogAttribute + PutFile

En cas d'échec de `PutHDFS` ou `QueryDatabaseTableRecord` :
- `LogAttribute` : enregistre le FlowFile en erreur dans les logs NiFi
- `PutFile` : écrit le FlowFile dans `/var/nifi/dead-letter/flux1/` pour investigation
- Une alerte Grafana est déclenchée si le compteur `FlowFiles Failed` > 0

---

## Commandes de vérification post-ingestion

```bash
# Vérifier que les fichiers sont bien dans HDFS
DATE=$(date +%Y-%m-%d)
hdfs dfs -ls /raw/transactions/date=${DATE}/

# Compter le nombre de lignes (via Spark)
spark-shell --master yarn -e "
  spark.read.parquet('/raw/transactions/date=${DATE}/')
    .count()
    .toString
    .foreach(println)
"

# Vérifier l'intégrité du schéma
hdfs dfs -cat /raw/transactions/date=${DATE}/*.parquet | \
  python3 -c "import sys; import pyarrow.parquet as pq; \
              print(pq.read_schema(sys.stdin.buffer))"
```

---

## Différences clés vs Sqoop

| Aspect | Sqoop (ancien) | NiFi (nouveau) |
|--------|---------------|----------------|
| Lancement | `spark-submit` CLI / cron | Scheduler CRON intégré NiFi |
| Monitoring | Logs YARN uniquement | Interface NiFi + métriques Prometheus |
| Gestion erreurs | Exit code, relance manuelle | Dead Letter Queue, retry configurable |
| Parallélisme | `-m 4` mappers YARN | Connexions JDBC poolées |
| Sécurité | Password en fichier | NiFi Vault / HashiCorp Vault intégré |
| Format sortie | Parquet via `--as-parquetfile` | `ParquetRecordSetWriter` |
