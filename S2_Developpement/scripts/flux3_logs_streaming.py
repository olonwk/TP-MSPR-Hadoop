"""
flux3_logs_streaming.py
Spark Structured Streaming — Flux 3 : Logs applicatifs via Kafka
FinData Solutions | DEPE855

Architecture :
  JSON Logs → Apache NiFi (PublishKafka) → Kafka topic: findata.logs.raw
            → Spark Structured Streaming (ce job) → HDFS /production/logs/
            → Hive table findata.logs_erreurs (partition auto)

Ce job est un service LONG-RUNNING (continu 24h/24).
Il n'est PAS orchestré par Airflow en tant que tâche ponctuelle.
Il est démarré comme un service YARN avec --deploy-mode cluster.

Règles métier appliquées en streaming :
  1. Filtre : ERROR et CRITICAL uniquement
  2. Extraction : codes métier via regex ERR-[0-9]{4}
  3. Fenêtre glissante 1 heure : comptage des erreurs par service et heure
  4. Écriture HDFS partitionné par service / date_log (micro-batch toutes les 30s)

Lancement :
  spark-submit --master yarn --deploy-mode cluster \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=2 \
    --conf spark.dynamicAllocation.maxExecutors=10 \
    --conf spark.dynamicAllocation.initialExecutors=4 \
    --conf spark.shuffle.service.enabled=true \
    flux3_logs_streaming.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, regexp_extract,
    to_timestamp, hour, to_date,
    window, count, lit, when
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)


KAFKA_BROKERS   = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC     = "findata.logs.raw"
HDFS_PROD_LOGS  = "/production/logs/"
CHECKPOINT_DIR  = "/checkpoints/logs/"
MICRO_BATCH_SEC = "30 seconds"
NIVEAUX_CIBLES  = ["ERROR", "CRITICAL"]
PATTERN_ERREUR  = r"ERR-[0-9]{4}"

# Schéma JSON des messages Kafka (champs variables → StringType pour robustesse)
LOG_SCHEMA = StructType([
    StructField("timestamp", StringType(),  True),
    StructField("service",   StringType(),  True),
    StructField("level",     StringType(),  True),
    StructField("message",   StringType(),  True),
    StructField("user_id",   StringType(),  True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("FinData-Flux3-LogsStreaming")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "2")
        .config("spark.dynamicAllocation.maxExecutors", "10")
        .config("spark.dynamicAllocation.initialExecutors", "4")
        .config("spark.shuffle.service.enabled", "true")
        # Active le metastore Hive pour écrire les partitions automatiquement
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"[Flux3-Streaming] Connexion Kafka : {KAFKA_BROKERS} | Topic : {KAFKA_TOPIC}")

    # ── Lecture depuis Kafka ────────────────────────────────────────────────
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50000")   # max 50K messages par micro-batch
        .load()
    )

    # ── Désérialisation JSON du champ value (bytes → struct) ────────────────
    df_parsed = (
        df_kafka
        .select(
            from_json(col("value").cast("string"), LOG_SCHEMA).alias("log"),
            col("timestamp").alias("kafka_ts"),     # timestamp Kafka
            col("partition"),
            col("offset")
        )
        .select("log.*", "kafka_ts", "partition", "offset")
    )

    # ── Règle 1 : Filtrage niveau ERROR et CRITICAL ─────────────────────────
    df_filtres = df_parsed.filter(col("level").isin(NIVEAUX_CIBLES))

    # ── Règle 2 : Extraction des codes d'erreur métier ─────────────────────
    df_codes = (
        df_filtres
        .withColumn("_code_raw", regexp_extract(col("message"), PATTERN_ERREUR, 0))
        .withColumn(
            "code_erreur",
            when(col("_code_raw") == "", lit("UNKNOWN")).otherwise(col("_code_raw"))
        )
        .drop("_code_raw")
    )

    # ── Dimensions temporelles ──────────────────────────────────────────────
    df_temps = (
        df_codes
        .withColumn("timestamp_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withColumn("heure",    hour(col("timestamp_ts")))
        .withColumn("date_log", to_date(col("timestamp_ts")))
    )

    # ── Règle 3 : Agrégation par fenêtre temporelle de 1h ──────────────────
    # Fenêtre glissante de 1h avec watermark de 5 min (tolérance aux messages tardifs)
    df_agg = (
        df_temps
        .withWatermark("timestamp_ts", "5 minutes")
        .groupBy(
            window(col("timestamp_ts"), "1 hour"),
            col("service"),
            col("date_log"),
            col("heure"),
            col("code_erreur"),
            col("level")
        )
        .agg(count("*").alias("nb_erreurs"))
        .select(
            col("service"),
            col("date_log"),
            col("heure"),
            col("code_erreur"),
            col("level"),
            col("nb_erreurs"),
            col("window.start").alias("fenetre_debut"),
            col("window.end").alias("fenetre_fin")
        )
    )

    # ── Règle 4 : Écriture HDFS partitionné par service et date_log ────────
    # Mode "append" : chaque micro-batch ajoute de nouvelles lignes
    # checkpointLocation : garantit l'exactement-une-fois (exactly-once) en cas de redémarrage
    query = (
        df_agg
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_PROD_LOGS)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .partitionBy("service", "date_log")
        .trigger(processingTime=MICRO_BATCH_SEC)
        .start()
    )

    print(f"[Flux3-Streaming] Job démarré. Micro-batch : {MICRO_BATCH_SEC}")
    print(f"[Flux3-Streaming] Écriture HDFS : {HDFS_PROD_LOGS}")
    print(f"[Flux3-Streaming] Checkpoint    : {CHECKPOINT_DIR}")

    # ── Mise à jour des partitions Hive après chaque micro-batch ────────────
    def update_hive_partitions(batch_df, batch_id):
        """Appelé après chaque micro-batch pour notifier Hive des nouvelles partitions."""
        if batch_df.count() > 0:
            try:
                spark.sql("MSCK REPAIR TABLE findata.logs_erreurs")
                print(f"[Flux3-Streaming] Batch {batch_id} — partitions Hive mises à jour")
            except Exception as e:
                print(f"[Flux3-Streaming] Warning Hive repair : {e}")

    # Pour activer la mise à jour Hive, remplacer le writeStream ci-dessus par :
    # df_agg.writeStream.foreachBatch(update_hive_partitions).start()
    # (Voir hive_tables.sql pour la création des tables)

    query.awaitTermination()


if __name__ == "__main__":
    main()
