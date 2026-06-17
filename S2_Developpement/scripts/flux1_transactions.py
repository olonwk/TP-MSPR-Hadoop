"""
flux1_transactions.py
PySpark Job — Transformation Flux 1 : Transactions bancaires
FinData Solutions | DEPE855

Règles métier appliquées :
  1. Filtrage : suppression lignes montant <= 0 ou statut NULL
  2. Pseudonymisation : iban → SHA-256(sel + iban) ; client_id → identifiant séquentiel anonyme
  3. Conversion devises → EUR via référentiel /ref/cours/ (Flux 2, doit être disponible)
  4. Agrégation par code_banque et par jour : volume_total, montant_moyen, nb_transactions, nb_fraudes
  5. Partitionnement HDFS par annee/mois/jour

Prérequis : flux2_cours_boursiers.py doit avoir été exécuté pour la date_traitement.

Usage :
  spark-submit --master yarn --deploy-mode cluster \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=2 \
    --conf spark.dynamicAllocation.maxExecutors=10 \
    --conf spark.dynamicAllocation.initialExecutors=4 \
    --conf spark.shuffle.service.enabled=true \
    flux1_transactions.py 2024-01-15
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, concat, lit,
    to_date, to_timestamp,
    year, month, dayofmonth,
    sum as _sum, avg, count, when,
    monotonically_increasing_id,
    dense_rank
)
from pyspark.sql.window import Window


# Sel fixe — en production, récupérer depuis HashiCorp Vault ou variable d'environnement sécurisée
SALT_IBAN = "FINDATA_IBAN_SALT_2024_EPSI"

HDFS_RAW_TRANSACTIONS    = "/raw/transactions/"
HDFS_REF_COURS           = "/ref/cours/"
HDFS_PROD_TRANSACTIONS   = "/production/transactions/"


def pseudonymiser_iban(df, salt: str):
    """Remplace la colonne iban par son hash SHA-256 avec sel fixe."""
    return (
        df
        .withColumn("iban_hash", sha2(concat(lit(salt), col("iban")), 256))
        .drop("iban")
    )


def pseudonymiser_client_id(df):
    """
    Remplace client_id par un identifiant anonyme séquentiel.
    dense_rank() sur l'ensemble du DataFrame garantit une numérotation continue
    sans exposer le client_id d'origine.
    """
    fenetre = Window.orderBy("client_id")
    return (
        df
        .withColumn("client_id_anon", dense_rank().over(fenetre))
        .drop("client_id")
    )


def main(date_traitement: str):
    spark = (
        SparkSession.builder
        .appName(f"FinData-Flux1-Transactions-{date_traitement}")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "2")
        .config("spark.dynamicAllocation.maxExecutors", "10")
        .config("spark.dynamicAllocation.initialExecutors", "4")
        .config("spark.shuffle.service.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ── Chargement des transactions brutes ──────────────────────────────────
    df_raw = spark.read.parquet(HDFS_RAW_TRANSACTIONS)
    nb_brut = df_raw.count()
    print(f"[Flux1] Lignes brutes : {nb_brut}")

    # ── Chargement du référentiel de taux de change (Flux 2) ────────────────
    df_cours = (
        spark.read
        .parquet(HDFS_REF_COURS)
        .filter(col("date_partition") == date_traitement)
        .select(
            col("devise"),
            col("taux_eur").alias("taux_conversion")
        )
    )
    nb_devises = df_cours.count()
    if nb_devises == 0:
        raise RuntimeError(
            f"[Flux1] ERREUR CRITIQUE : référentiel de devises vide pour {date_traitement}. "
            "Vérifier que flux2_cours_boursiers.py a bien été exécuté."
        )
    print(f"[Flux1] Devises disponibles dans le référentiel : {nb_devises}")

    # ── Règle 1 : Filtrage ───────────────────────────────────────────────────
    df_clean = df_raw.filter(
        (col("montant") > 0) & col("statut").isNotNull()
    )
    nb_rejetes = nb_brut - df_clean.count()
    print(f"[Flux1] Lignes rejetées (montant <= 0 ou statut NULL) : {nb_rejetes}")

    # ── Règle 2a : Pseudonymisation IBAN ───────────────────────────────────
    df_pseudo = pseudonymiser_iban(df_clean, SALT_IBAN)

    # ── Règle 2b : Pseudonymisation client_id ──────────────────────────────
    df_pseudo = pseudonymiser_client_id(df_pseudo)

    # ── Règle 3 : Conversion de devises en EUR ──────────────────────────────
    df_converti = (
        df_pseudo
        .join(df_cours, on="devise", how="left")
        .withColumn(
            "montant_eur",
            when(col("devise") == "EUR", col("montant"))
            .otherwise(col("montant") / col("taux_conversion"))
        )
        .drop("taux_conversion")
    )

    # Ajout colonnes de partitionnement temporel
    df_date = (
        df_converti
        .withColumn("date_ts", to_timestamp(col("date_heure"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("annee",   year(col("date_ts")))
        .withColumn("mois",    month(col("date_ts")))
        .withColumn("jour",    dayofmonth(col("date_ts")))
    )

    # ── Règle 4 : Agrégation par code_banque et par jour ───────────────────
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

    nb_output = df_agg.count()
    print(f"[Flux1] Agrégats produits : {nb_output} (code_banque × jour)")

    # ── Règle 5 : Écriture HDFS partitionné par annee/mois/jour ────────────
    (
        df_agg
        .write
        .mode("overwrite")
        .partitionBy("annee", "mois", "jour")
        .parquet(HDFS_PROD_TRANSACTIONS)
    )

    print(f"[Flux1] Données écrites dans {HDFS_PROD_TRANSACTIONS}")
    print(f"[Flux1] Résumé — Entrées : {nb_brut} | Rejetées : {nb_rejetes} | Agrégats : {nb_output}")
    spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"
    main(date)
