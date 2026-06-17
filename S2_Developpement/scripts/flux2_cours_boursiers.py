"""
flux2_cours_boursiers.py
PySpark Job — Transformation Flux 2 : Cours boursiers
FinData Solutions | DEPE855

Rôle : Lire les cours bruts depuis /raw/courses/, filtrer les sources non autorisées,
       conserver uniquement le taux de clôture (dernier enregistrement par devise),
       écrire le référentiel dans /ref/cours/ partitionné par jour.

Ce job DOIT être exécuté AVANT flux1_transactions.py.

Usage :
  spark-submit --master yarn --deploy-mode cluster \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=2 \
    --conf spark.dynamicAllocation.maxExecutors=10 \
    --conf spark.dynamicAllocation.initialExecutors=4 \
    --conf spark.shuffle.service.enabled=true \
    flux2_cours_boursiers.py 2024-01-15
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, row_number, to_timestamp
from pyspark.sql.window import Window


SOURCES_AUTORISEES = ["ECB", "Bloomberg", "Reuters"]

HDFS_RAW_COURSES = "/raw/courses/{date}.csv"
HDFS_REF_COURS   = "/ref/cours/"


def main(date_traitement: str):
    spark = (
        SparkSession.builder
        .appName(f"FinData-Flux2-CoursBoursiersRef-{date_traitement}")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "2")
        .config("spark.dynamicAllocation.maxExecutors", "10")
        .config("spark.dynamicAllocation.initialExecutors", "4")
        .config("spark.shuffle.service.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    chemin_csv = HDFS_RAW_COURSES.format(date=date_traitement)

    # Chargement du fichier CSV brut
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(chemin_csv)
    )

    nb_brut = df_raw.count()
    print(f"[Flux2] Lignes brutes chargées : {nb_brut}")

    # Filtrage des sources non autorisées
    df_filtrees = df_raw.filter(col("source").isin(SOURCES_AUTORISEES))
    nb_filtrees = df_filtrees.count()
    print(f"[Flux2] Lignes après filtrage sources : {nb_filtrees} (rejetées : {nb_brut - nb_filtrees})")

    # Conversion de l'horodatage
    df_ts = df_filtrees.withColumn("horodatage_ts", to_timestamp(col("horodatage"), "yyyy-MM-dd HH:mm:ss"))

    # Conserver uniquement le taux de clôture = dernier enregistrement de la journée par devise
    # On trie par horodatage décroissant et on garde le rang 1 pour chaque devise
    fenetre = Window.partitionBy("devise").orderBy(col("horodatage_ts").desc())
    df_cloture = (
        df_ts
        .withColumn("rang", row_number().over(fenetre))
        .filter(col("rang") == 1)
        .drop("rang", "horodatage")
        .withColumnRenamed("horodatage_ts", "horodatage_cloture")
        .withColumn("date_partition", to_date(lit(date_traitement)))
    )

    nb_final = df_cloture.count()
    print(f"[Flux2] Devises en sortie (taux de clôture) : {nb_final}")

    # Écriture dans le référentiel HDFS, partitionné par date_partition
    (
        df_cloture
        .write
        .mode("overwrite")
        .partitionBy("date_partition")
        .parquet(HDFS_REF_COURS)
    )

    print(f"[Flux2] Référentiel écrit dans {HDFS_REF_COURS} — partition date_partition={date_traitement}")
    spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"
    main(date)
