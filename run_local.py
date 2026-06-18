"""
run_local.py
Runner local — Pipeline FinData Solutions (mode PySpark local[*])
DEPE855 | Simule les 3 flux sans cluster Hadoop ni Docker.

Prérequis : python generate_datasets.py --date 2024-01-15
Résultats : local_output/  (structure miroir de HDFS)

Usage : python run_local.py [--date AAAA-MM-JJ]
"""

import argparse
import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, concat, lit,
    to_date, to_timestamp,
    year, month, dayofmonth,
    sum as _sum, avg, count, when,
    dense_rank, row_number,
    regexp_extract, hour,
    window, desc
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, DoubleType
)
from pyspark.sql.window import Window


# ── Chemins locaux ──────────────────────────────────────────────────────────

BASE_DIR  = Path(__file__).parent
TEST_DATA = BASE_DIR / "S2_Developpement" / "test_data"
OUT       = BASE_DIR / "local_output"

SALT_IBAN      = "FINDATA_IBAN_SALT_2024_EPSI"
SOURCES_OK     = ["ECB", "Bloomberg", "Reuters"]
NIVEAUX_CIBLES = ["ERROR", "CRITICAL"]
PATTERN_ERR    = r"ERR-[0-9]{4}"

SEPARATOR = "=" * 65


def init_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ── FLUX 2 : Cours boursiers ─────────────────────────────────────────────────

def run_flux2(spark: SparkSession, date: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  FLUX 2 — Cours boursiers ({date})")
    print(SEPARATOR)
    t0 = time.time()

    csv_path = str(TEST_DATA / f"courses_{date}.csv")
    out_path = str(OUT / "ref" / "cours")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )
    nb_brut = df_raw.count()
    print(f"  Lignes brutes lues  : {nb_brut}")

    # Règle 1 : filtrer les sources non autorisées
    df_propre = df_raw.filter(col("source").isin(SOURCES_OK))
    nb_ok = df_propre.count()
    print(f"  Sources valides     : {nb_ok} (rejetés : {nb_brut - nb_ok})")

    # Règle 2 : conserver uniquement le cours de clôture (dernier horodatage par devise)
    fenetre_cloture = Window.partitionBy("devise").orderBy(desc("horodatage"))
    df_cloture = (
        df_propre
        .withColumn("_rn", row_number().over(fenetre_cloture))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .withColumn("date_partition", to_date(col("horodatage")))
    )
    nb_out = df_cloture.count()
    print(f"  Cours de clôture    : {nb_out} devises")
    df_cloture.show(5, truncate=False)

    # Écriture Parquet partitionné
    (
        df_cloture.write
        .mode("overwrite")
        .partitionBy("date_partition")
        .parquet(out_path)
    )
    print(f"  Écrit dans         : {out_path}")
    print(f"  Durée Flux 2       : {time.time() - t0:.1f}s")


# ── FLUX 1 : Transactions ────────────────────────────────────────────────────

def run_flux1(spark: SparkSession, date: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  FLUX 1 — Transactions bancaires ({date})")
    print(SEPARATOR)
    t0 = time.time()

    csv_path  = str(TEST_DATA / f"transactions_{date}.csv")
    cours_path = str(OUT / "ref" / "cours")
    out_path   = str(OUT / "production" / "transactions")

    # Chargement transactions brutes
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )
    nb_brut = df_raw.count()
    print(f"  Lignes brutes       : {nb_brut:,}")

    # Chargement référentiel devises (résultat Flux 2)
    df_cours = (
        spark.read.parquet(cours_path)
        .filter(col("date_partition") == date)
        .select(col("devise"), col("taux_eur").alias("taux_conversion"))
    )
    nb_devises = df_cours.count()
    if nb_devises == 0:
        print("  ERREUR : référentiel devises vide. Lancer Flux 2 d'abord.")
        return
    print(f"  Devises disponibles : {nb_devises}")

    # Règle 1 : filtrage qualité
    df_clean = df_raw.filter(
        (col("montant") > 0) & col("statut").isNotNull()
    )
    nb_rejetes = nb_brut - df_clean.count()
    print(f"  Lignes rejetées     : {nb_rejetes:,} (montant<=0 ou statut NULL)")

    # Règle 2a : pseudonymisation IBAN → SHA-256
    df_pseudo = (
        df_clean
        .withColumn("iban_hash", sha2(concat(lit(SALT_IBAN), col("iban")), 256))
        .drop("iban")
    )

    # Règle 2b : anonymisation client_id → rang séquentiel
    fenetre_rank = Window.orderBy("client_id")
    df_pseudo = (
        df_pseudo
        .withColumn("client_id_anon", dense_rank().over(fenetre_rank))
        .drop("client_id")
    )

    # Règle 3 : conversion en EUR
    df_eur = (
        df_pseudo
        .join(df_cours, on="devise", how="left")
        .withColumn(
            "montant_eur",
            when(col("devise") == "EUR", col("montant"))
            .otherwise(col("montant") / col("taux_conversion"))
        )
        .drop("taux_conversion")
    )

    # Colonnes de partitionnement
    df_date = (
        df_eur
        .withColumn("date_ts", to_timestamp(col("date_heure"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("annee", year(col("date_ts")))
        .withColumn("mois",  month(col("date_ts")))
        .withColumn("jour",  dayofmonth(col("date_ts")))
    )

    # Règle 4 : agrégation
    df_agg = (
        df_date
        .groupBy("code_banque", "annee", "mois", "jour")
        .agg(
            _sum("montant_eur").alias("volume_total_eur"),
            avg("montant_eur").alias("montant_moyen_eur"),
            count("*").alias("nb_transactions"),
            count(when(col("statut") == "FRAUD", True)).alias("nb_fraudes")
        )
    )

    nb_out = df_agg.count()
    print(f"  Agrégats produits   : {nb_out} (code_banque × jour)")
    print("\n  Aperçu des 10 premières banques :")
    (
        df_agg
        .orderBy(desc("volume_total_eur"))
        .show(10, truncate=False)
    )

    # Calcul taux de fraude
    from pyspark.sql.functions import round as _round
    df_fraude = (
        df_agg
        .groupBy("code_banque")
        .agg(
            _sum("nb_transactions").alias("total_txn"),
            _sum("nb_fraudes").alias("total_fraudes"),
            _round(_sum("nb_fraudes") * 100.0 / _sum("nb_transactions"), 2).alias("taux_fraude_pct"),
            _round(_sum("volume_total_eur"), 2).alias("volume_eur")
        )
        .orderBy(desc("taux_fraude_pct"))
    )
    print("  Taux de fraude par banque :")
    df_fraude.show(truncate=False)

    # Écriture Parquet partitionné
    (
        df_agg.write
        .mode("overwrite")
        .partitionBy("annee", "mois", "jour")
        .parquet(out_path)
    )
    print(f"  Écrit dans          : {out_path}")
    print(f"  Durée Flux 1        : {time.time() - t0:.1f}s")


# ── FLUX 3 : Logs applicatifs (batch local) ──────────────────────────────────

def run_flux3(spark: SparkSession, date: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  FLUX 3 — Logs applicatifs ({date})")
    print(SEPARATOR)
    t0 = time.time()

    json_path = str(TEST_DATA / f"logs_{date}.json")
    out_path  = str(OUT / "production" / "logs")

    log_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("service",   StringType(), True),
        StructField("level",     StringType(), True),
        StructField("message",   StringType(), True),
        StructField("user_id",   StringType(), True),
    ])

    df_raw = spark.read.schema(log_schema).json(json_path)
    nb_brut = df_raw.count()
    print(f"  Logs lus            : {nb_brut:,}")

    # Règle 1 : filtrer ERROR et CRITICAL
    df_err = df_raw.filter(col("level").isin(NIVEAUX_CIBLES))
    nb_err = df_err.count()
    print(f"  Erreurs/Critiques   : {nb_err:,} ({nb_err*100//nb_brut}% du total)")

    # Règle 2 : extraire les codes ERR-XXXX
    df_codes = df_err.withColumn("code_erreur", regexp_extract(col("message"), PATTERN_ERR, 0))

    # Règle 3 : dimensions temporelles
    df_temps = (
        df_codes
        .withColumn("timestamp_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withColumn("heure",    hour(col("timestamp_ts")))
        .withColumn("date_log", to_date(col("timestamp_ts")))
    )

    # Règle 4 : agrégation par heure, service, code_erreur
    df_agg = (
        df_temps
        .groupBy("service", "date_log", "heure", "code_erreur", "level")
        .agg(count("*").alias("nb_erreurs"))
        .orderBy("service", "heure", desc("nb_erreurs"))
    )

    nb_out = df_agg.count()
    print(f"  Agrégats produits   : {nb_out} (service × heure × code_erreur)")

    print("\n  Top 15 erreurs les plus fréquentes :")
    (
        df_agg
        .groupBy("service", "code_erreur")
        .agg(_sum("nb_erreurs").alias("total"))
        .orderBy(desc("total"))
        .show(15, truncate=False)
    )

    # Écriture Parquet partitionné par service/date_log
    (
        df_agg.write
        .mode("overwrite")
        .partitionBy("service", "date_log")
        .parquet(out_path)
    )
    print(f"  Écrit dans          : {out_path}")
    print(f"  Durée Flux 3        : {time.time() - t0:.1f}s")


# ── Rapport final ────────────────────────────────────────────────────────────

def rapport_final(date: str) -> None:
    print(f"\n{SEPARATOR}")
    print("  RAPPORT FINAL — Structure local_output/")
    print(SEPARATOR)

    out = OUT
    total_size = 0
    for root, dirs, files in os.walk(out):
        for f in files:
            fp = Path(root) / f
            size = fp.stat().st_size
            total_size += size
            rel = fp.relative_to(BASE_DIR)
            print(f"  {rel}  ({size//1024} Ko)")

    print(f"\n  Taille totale : {total_size // (1024*1024)} Mo")
    print(f"""
  Equivalences HDFS (sur cluster réel) :
    local_output/ref/cours/           → /ref/cours/
    local_output/production/transactions/ → /production/transactions/
    local_output/production/logs/     → /production/logs/

  Pour lancer sur cluster Hadoop :
    hdfs dfs -put local_output/ref/cours/ /ref/
    spark-submit --master yarn --deploy-mode cluster \\
      S2_Developpement/scripts/flux1_transactions.py {date}
""")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Runner local pipeline FinData")
    parser.add_argument("--date", default="2024-01-15", help="Date de traitement AAAA-MM-JJ")
    args = parser.parse_args()
    date = args.date

    csv_check = TEST_DATA / f"transactions_{date}.csv"
    if not csv_check.exists():
        print(f"ERREUR : {csv_check} introuvable.")
        print(f"Lancer d'abord : python S2_Developpement/scripts/generate_datasets.py --date {date}")
        sys.exit(1)

    print(f"\n{'='*65}")
    print(f"  PIPELINE FINDATA SOLUTIONS — Mode local PySpark")
    print(f"  Date de traitement : {date}")
    print(f"{'='*65}")

    spark = init_spark(f"FinData-Pipeline-Local-{date}")
    print(f"  Spark version : {spark.version}")
    print(f"  UI : {spark.sparkContext.uiWebUrl}")

    try:
        run_flux2(spark, date)
        run_flux1(spark, date)
        run_flux3(spark, date)
        rapport_final(date)
        print(f"\n{'='*65}")
        print("  PIPELINE TERMINÉ AVEC SUCCÈS")
        print(f"{'='*65}\n")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
