"""
flux3_logs.py
PySpark Job — Transformation Flux 3 : Logs applicatifs JSON
FinData Solutions | DEPE855

Règles métier appliquées :
  1. Filtrage : conserver uniquement les niveaux ERROR et CRITICAL
  2. Extraction : codes d'erreur métier via regex ERR-[0-9]{4} depuis le champ message
  3. Comptage : nombre d'erreurs par service et par heure
  4. Partitionnement HDFS : /production/logs/service=XXX/date_log=AAAA-MM-JJ/

Ce job est indépendant des Flux 1 et 2 (pas de dépendance de données).

Usage :
  spark-submit --master yarn --deploy-mode cluster \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=2 \
    --conf spark.dynamicAllocation.maxExecutors=10 \
    --conf spark.dynamicAllocation.initialExecutors=4 \
    --conf spark.shuffle.service.enabled=true \
    flux3_logs.py 2024-01-15
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, to_timestamp,
    hour, to_date, count, lit
)


NIVEAUX_CIBLES   = ["ERROR", "CRITICAL"]
PATTERN_ERREUR   = r"ERR-[0-9]{4}"

HDFS_STAGING_LOGS     = "/staging/logs/"
HDFS_PRODUCTION_LOGS  = "/production/logs/"


def main(date_traitement: str):
    spark = (
        SparkSession.builder
        .appName(f"FinData-Flux3-LogsApplicatifs-{date_traitement}")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "2")
        .config("spark.dynamicAllocation.maxExecutors", "10")
        .config("spark.dynamicAllocation.initialExecutors", "4")
        .config("spark.shuffle.service.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ── Chargement des logs JSON depuis la zone de staging ─────────────────
    # Le schéma est semi-structuré : certains champs peuvent être absents
    df_raw = spark.read.json(HDFS_STAGING_LOGS)
    nb_brut = df_raw.count()
    print(f"[Flux3] Logs bruts chargés : {nb_brut}")

    # ── Règle 1 : Filtrage — ERROR et CRITICAL uniquement ──────────────────
    df_filtres = df_raw.filter(col("level").isin(NIVEAUX_CIBLES))
    nb_filtres = df_filtres.count()
    print(f"[Flux3] Logs ERROR/CRITICAL : {nb_filtres} (filtrés : {nb_brut - nb_filtres})")

    # ── Règle 2 : Extraction des codes d'erreur métier ─────────────────────
    # regexp_extract retourne "" si le pattern n'est pas trouvé
    df_codes = df_filtres.withColumn(
        "code_erreur",
        regexp_extract(col("message"), PATTERN_ERREUR, 0)
    )

    # ── Ajout dimensions temporelles ───────────────────────────────────────
    df_temps = (
        df_codes
        .withColumn(
            "timestamp_ts",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        .withColumn("heure",    hour(col("timestamp_ts")))
        .withColumn("date_log", to_date(col("timestamp_ts")))
    )

    # ── Règle 3 : Comptage des erreurs par service, heure et code ──────────
    df_comptage = (
        df_temps
        .groupBy("service", "date_log", "heure", "code_erreur", "level")
        .agg(count("*").alias("nb_erreurs"))
        .orderBy("service", "date_log", "heure")
    )

    nb_output = df_comptage.count()
    print(f"[Flux3] Groupes service×heure×code produits : {nb_output}")

    # ── Règle 4 : Écriture HDFS partitionné par service et date_log ────────
    (
        df_comptage
        .write
        .mode("overwrite")
        .partitionBy("service", "date_log")
        .parquet(HDFS_PRODUCTION_LOGS)
    )

    print(f"[Flux3] Données écrites dans {HDFS_PRODUCTION_LOGS}")
    print(f"[Flux3] Résumé — Entrées : {nb_brut} | ERROR/CRITICAL : {nb_filtres} | Groupes : {nb_output}")
    spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"
    main(date)
