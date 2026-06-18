# Réponses aux questions d'analyse — TP Apache NiFi
## FinData Solutions | DEPE855

---

## Question 1 — Rôle du processeur `ExecuteSQL`

`ExecuteSQL` exécute une requête SQL arbitraire sur une base de données relationnelle via une connexion JDBC, puis produit un FlowFile contenant les résultats au format **Apache Avro** (format binaire, schéma inclus dans le FlowFile).

**Fonctionnement dans notre flux :**
```
ExecuteSQL
  └── SQL Select Query : SELECT * FROM transactions WHERE DATE(date_heure) = '${literal(${now():format('yyyy-MM-dd')})}'
  └── Database Connection Pooling Service : DBCPConnectionPool (JDBC → PostgreSQL)
  └── Sortie : FlowFile au format Avro (success) ou FlowFile vide (failure)
```

**Limites à connaître :** `ExecuteSQL` ré-exécute la requête entière à chaque déclenchement (pas d'incrémental). Pour un suivi d'état (dernière ligne lue), il faut préférer `QueryDatabaseTableRecord` qui maintient un "maximum value column" entre deux exécutions.

---

## Question 2 — Pourquoi utiliser `DBCPConnectionPool` ?

Le `DBCPConnectionPool` (Database Connection Pool) est un **service controller NiFi** qui mutualise les connexions JDBC à la base de données. Sans pooling, chaque déclenchement d'`ExecuteSQL` devrait créer une nouvelle connexion TCP → authentification → handshake SSL, ce qui est coûteux (~200 ms par connexion).

**Avantages concrets :**

| Sans pool | Avec DBCPConnectionPool |
|-----------|------------------------|
| 1 connexion créée et détruite par requête | N connexions maintenues en vie et réutilisées |
| Latence élevée (handshake à chaque fois) | Latence faible (connexion déjà ouverte) |
| Risque de saturation du serveur PostgreSQL | Max Total Connections configurable (ex : 8) |
| Mot de passe en clair dans le processeur | Centralisé et chiffré dans le service controller |

**Configuration dans notre projet :**
```
Database Connection URL    : jdbc:postgresql://postgres:5432/bankdb
Database Driver Class Name : org.postgresql.Driver
Database Driver Location   : /opt/nifi/drivers/postgresql-42.7.0.jar
Database User              : admin
Password                   : admin123
Max Total Connections      : 8
```

---

## Question 3 — Différence entre NiFi et Sqoop

| Critère | Apache Sqoop | Apache NiFi |
|---------|-------------|-------------|
| **Type** | Outil CLI batch spécialisé Hadoop | Plateforme généraliste d'intégration de flux |
| **Maintenance** | Archivé depuis 2021, plus maintenu | Actif, Apache TLP, release 1.28 en 2024 |
| **Interface** | Ligne de commande uniquement | Interface graphique web (HTTPS:8443) |
| **Monitoring** | Logs YARN uniquement | Métriques temps réel, Data Provenance intégrée |
| **Sources** | Uniquement SGBDR → Hadoop | SGBDR, fichiers, API REST, Kafka, SFTP, S3... |
| **Gestion d'erreurs** | Sortie en erreur → relance manuelle | Dead Letter Queue, retry automatique configurable |
| **Sécurité** | Password en fichier `.properties` | Chiffrement via Sensitive Properties Key ou Vault |
| **Planification** | Cron externe (crontab/Airflow) | Scheduler CRON intégré au processeur |
| **Format sortie** | Parquet, Avro, Text (flag CLI) | Piloté par `RecordSetWriter` (Parquet, CSV, JSON, Avro) |

**Conclusion :** Sqoop était acceptable pour Hadoop 2.x, mais son arrêt de maintenance et l'absence de GUI en font un mauvais choix pour un environnement de production moderne. NiFi offre la même fonctionnalité d'ingestion PostgreSQL → HDFS avec monitoring, retry et sécurité intégrés.

---

## Question 4 — Avantages de NiFi par rapport à un script Python

Un script Python (`psycopg2` + `pyarrow` + `hdfs3`) peut réaliser la même opération technique, mais NiFi apporte plusieurs avantages structurels :

**1. Zéro code à maintenir**
La logique est déclarative (configuration de processeurs), pas impérative. Pas de bugs Python à corriger, pas de dépendances à mettre à jour.

**2. Monitoring natif sans instrumentation**
NiFi affiche en temps réel les FlowFiles traités, en erreur, en attente — sans écrire un seul `print()` ou intégrer Prometheus.

**3. Data Provenance intégrée**
NiFi trace chaque FlowFile : quand il a été créé, transformé, envoyé, par quel processeur — niveau d'audit RGPD impossible à reproduire facilement en Python.

**4. Retry et Dead Letter Queue**
En cas d'échec HDFS (NameNode indisponible), NiFi met le FlowFile en "Penalty" et réessaie automatiquement selon une politique configurable. Un script Python crashe.

**5. Scheduling intégré**
Cron `0 30 6 * * ?` directement dans le processeur, pas besoin d'Airflow ou crontab pour le déclenchement de l'ingestion seule.

**6. Scalabilité horizontale**
NiFi Cluster (plusieurs nœuds) pour absorber de gros volumes. Impossible sans refonte d'un script mono-processus.

**Cas où un script Python reste préférable :** transformations complexes (ML, dédoublonnage multi-sources), logique métier riche non modélisable en processeurs NiFi, ou contexte où NiFi n'est pas disponible.

---

## Question 5 — Comment intégrer Kafka après NiFi ?

Pour envoyer les données vers Kafka au lieu d'un fichier ou d'HDFS, il faut remplacer le processeur `PutFile` (ou `PutHDFS`) par **`PublishKafkaRecord_2_6`**.

**Flow NiFi modifié :**
```
[ExecuteSQL] → [ConvertRecord] → [PublishKafkaRecord_2_6]
     ↓ failure                         ↓ failure
[LogAttribute] → [PutFile (DLQ)]  [LogAttribute]
```

**Configuration de `PublishKafkaRecord_2_6` :**

| Propriété | Valeur |
|-----------|--------|
| Kafka Brokers | `kafka:9092` |
| Topic Name | `findata.logs.raw` |
| Record Reader | `AvroReader` (sortie de ConvertRecord) |
| Record Writer | `JsonRecordSetWriter` (Kafka attend du JSON lisible par Spark) |
| Use Transactions | `false` (pour performance en batch) |
| Delivery Guarantee | `Replicated` |

**Schéma global Flux 3 :**
```
Application logs
    → NiFi TailFile
    → PublishKafkaRecord_2_6 → Topic findata.logs.raw
    → Spark Structured Streaming (flux3_logs_streaming.py)
    → HDFS /production/logs/ (Parquet partitionné)
    → Hive table findata.logs_erreurs
```

---

## Question 6 — Comment alimenter HDFS au lieu d'un fichier CSV ?

Pour écrire dans HDFS au lieu du système de fichiers local (`PutFile`), il faut remplacer `PutFile` par **`PutHDFS`**.

**Flow NiFi modifié :**
```
[ExecuteSQL] → [ConvertRecord] → [UpdateAttribute] → [PutHDFS]
                 (Avro → Parquet)   (nom du fichier)
```

**Configuration de `ConvertRecord` (Avro → Parquet) :**

| Propriété | Valeur |
|-----------|--------|
| Record Reader | `AvroReader` |
| Record Writer | `ParquetRecordSetWriter` (compression: SNAPPY) |

**Configuration de `PutHDFS` :**

| Propriété | Valeur |
|-----------|--------|
| Hadoop Configuration Resources | `/etc/hadoop/conf/core-site.xml, /etc/hadoop/conf/hdfs-site.xml` |
| Directory | `/raw/transactions/date=${now():format('yyyy-MM-dd')}/` |
| Conflict Resolution Strategy | `replace` |
| Block Size | `134217728` (128 Mo) |
| Replication Factor | `3` |

**Prérequis :**
1. Le conteneur NiFi doit avoir accès réseau au NameNode Hadoop
2. Les fichiers `core-site.xml` et `hdfs-site.xml` doivent être montés dans le conteneur NiFi
3. Si Kerberos est activé sur le cluster, configurer `Kerberos Principal` et `Kerberos Keytab`

**Vérification post-ingestion :**
```bash
# Depuis un nœud Hadoop ou le conteneur client
hdfs dfs -ls /raw/transactions/date=$(date +%Y-%m-%d)/
# Attendu : fichiers .parquet créés par NiFi
```
