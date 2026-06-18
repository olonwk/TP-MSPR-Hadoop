"""
run_local_pandas.py
Runner local — Pipeline FinData Solutions (pandas + pyarrow)
DEPE855 | Simule les 3 flux sans cluster Hadoop ni Docker.

Même logique métier que les scripts PySpark, exécutée localement.
Produit les mêmes fichiers Parquet partitionnés que sur le cluster.

Usage : python run_local_pandas.py [--date AAAA-MM-JJ]
"""

import argparse
import hashlib
import os
import re
import sys
import time
from pathlib import Path

# Forcer UTF-8 sur Windows pour les caractères spéciaux dans les prints
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


BASE_DIR  = Path(__file__).parent
TEST_DATA = BASE_DIR / "S2_Developpement" / "test_data"
OUT       = BASE_DIR / "local_output"

SALT_IBAN      = "FINDATA_IBAN_SALT_2024_EPSI"
SOURCES_OK     = {"ECB", "Bloomberg", "Reuters"}
NIVEAUX_CIBLES = {"ERROR", "CRITICAL"}
PATTERN_ERR    = re.compile(r"ERR-[0-9]{4}")
SEP            = "=" * 65


def sep(title: str):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def sha256_iban(iban: str) -> str:
    return hashlib.sha256((SALT_IBAN + str(iban)).encode()).hexdigest()


def write_parquet_partitioned(df: pd.DataFrame, base_path: Path, partition_cols: list):
    """Écrit un DataFrame en Parquet partitionné (mimique partitionBy de Spark)."""
    for keys, group in df.groupby(partition_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)
        # Construire le chemin de partition (ex: annee=2024/mois=1/jour=15)
        parts = "/".join(f"{col}={val}" for col, val in zip(partition_cols, keys))
        part_dir = base_path / parts
        part_dir.mkdir(parents=True, exist_ok=True)
        data = group.drop(columns=partition_cols)
        table = pa.Table.from_pandas(data, preserve_index=False)
        pq.write_table(table, part_dir / "data.parquet",
                       compression="snappy")
    print(f"  Parquet partitionné : {base_path}")


# ── FLUX 2 : Cours boursiers ─────────────────────────────────────────────────

def run_flux2(date: str) -> pd.DataFrame:
    sep(f"FLUX 2 — Cours boursiers ({date})")
    t0 = time.time()

    csv_path = TEST_DATA / f"courses_{date}.csv"
    out_path = OUT / "ref" / "cours"

    df = pd.read_csv(csv_path)
    nb_brut = len(df)
    print(f"  Lignes brutes        : {nb_brut}")

    # Règle 1 : filtrer les sources non autorisées
    df = df[df["source"].isin(SOURCES_OK)].copy()
    print(f"  Sources valides      : {len(df)} (rejetés : {nb_brut - len(df)})")

    # Règle 2 : cours de clôture = dernier horodatage par devise
    df["horodatage"] = pd.to_datetime(df["horodatage"])
    df = (
        df.sort_values("horodatage", ascending=False)
          .groupby("devise", as_index=False)
          .first()
    )
    df["date_partition"] = date
    print(f"  Cours de clôture     : {len(df)} devises")
    print()
    print(df[["devise", "taux_eur", "source", "horodatage"]].to_string(index=False))

    write_parquet_partitioned(df, out_path, ["date_partition"])
    print(f"  Durée Flux 2         : {time.time() - t0:.1f}s")
    return df


# ── FLUX 1 : Transactions ────────────────────────────────────────────────────

def run_flux1(date: str, df_cours: pd.DataFrame) -> None:
    sep(f"FLUX 1 — Transactions bancaires ({date})")
    t0 = time.time()

    csv_path = TEST_DATA / f"transactions_{date}.csv"
    out_path = OUT / "production" / "transactions"

    print(f"  Chargement de {csv_path.name}...")
    df = pd.read_csv(csv_path, dtype={"client_id": str, "iban": str})
    nb_brut = len(df)
    print(f"  Lignes brutes        : {nb_brut:,}")

    # Règle 1 : filtrage qualité
    df = df[(df["montant"] > 0) & df["statut"].notna()].copy()
    nb_rejetes = nb_brut - len(df)
    print(f"  Lignes rejetées      : {nb_rejetes:,} (montant<=0 ou statut NULL)")

    # Règle 2a : pseudonymisation IBAN → SHA-256 avec sel
    print("  Pseudonymisation IBAN (SHA-256 + sel)...")
    df["iban_hash"] = df["iban"].apply(sha256_iban)
    df.drop(columns=["iban"], inplace=True)

    # Règle 2b : anonymisation client_id → rang séquentiel
    ids_uniques = {cid: rank+1 for rank, cid in enumerate(sorted(df["client_id"].unique()))}
    df["client_id_anon"] = df["client_id"].map(ids_uniques)
    df.drop(columns=["client_id"], inplace=True)
    print(f"  Clients anonymisés   : {len(ids_uniques):,} identifiants uniques → rangs 1..{len(ids_uniques)}")

    # Règle 3 : conversion en EUR via référentiel Flux 2
    taux_map = dict(zip(df_cours["devise"], df_cours["taux_eur"]))
    taux_map["EUR"] = 1.0
    df["taux"] = df["devise"].map(taux_map)
    df["montant_eur"] = df.apply(
        lambda r: r["montant"] if r["devise"] == "EUR" else r["montant"] / r["taux"]
        if pd.notna(r["taux"]) else None, axis=1
    )
    non_convertis = df["montant_eur"].isna().sum()
    if non_convertis > 0:
        print(f"  AVERTISSEMENT : {non_convertis:,} transactions sans taux (devise inconnue)")
    df.drop(columns=["taux"], inplace=True)

    # Colonnes de partitionnement
    df["date_heure"] = pd.to_datetime(df["date_heure"])
    df["annee"] = df["date_heure"].dt.year
    df["mois"]  = df["date_heure"].dt.month
    df["jour"]  = df["date_heure"].dt.day

    # Règle 4 : agrégation par code_banque et par jour
    df_agg = (
        df.groupby(["code_banque", "annee", "mois", "jour"])
          .agg(
              volume_total_eur  = ("montant_eur", "sum"),
              montant_moyen_eur = ("montant_eur", "mean"),
              nb_transactions   = ("montant_eur", "count"),
              nb_fraudes        = ("statut", lambda x: (x == "FRAUD").sum())
          )
          .reset_index()
    )

    nb_out = len(df_agg)
    print(f"  Agrégats produits    : {nb_out} (code_banque × jour)")

    # Taux de fraude par banque
    fraude = (
        df_agg.groupby("code_banque")
              .agg(
                  total_txn    = ("nb_transactions", "sum"),
                  total_fraudes= ("nb_fraudes", "sum"),
                  volume_eur   = ("volume_total_eur", "sum")
              )
              .reset_index()
    )
    fraude["taux_fraude_pct"] = (fraude["total_fraudes"] / fraude["total_txn"] * 100).round(2)
    fraude = fraude.sort_values("taux_fraude_pct", ascending=False)
    fraude["volume_eur"] = fraude["volume_eur"].apply(lambda x: f"{x:,.0f}")

    print()
    print("  Taux de fraude par banque :")
    print(fraude[["code_banque", "total_txn", "total_fraudes", "taux_fraude_pct", "volume_eur"]]
          .to_string(index=False))

    write_parquet_partitioned(df_agg, out_path, ["annee", "mois", "jour"])
    print(f"  Durée Flux 1         : {time.time() - t0:.1f}s")


# ── FLUX 3 : Logs applicatifs ────────────────────────────────────────────────

def run_flux3(date: str) -> None:
    sep(f"FLUX 3 — Logs applicatifs ({date})")
    t0 = time.time()

    json_path = TEST_DATA / f"logs_{date}.json"
    out_path  = OUT / "production" / "logs"

    df = pd.read_json(json_path, lines=True)
    nb_brut = len(df)
    print(f"  Logs lus             : {nb_brut:,}")

    # Règle 1 : filtrer ERROR et CRITICAL
    df_err = df[df["level"].isin(NIVEAUX_CIBLES)].copy()
    nb_err = len(df_err)
    print(f"  Erreurs/Critiques    : {nb_err:,} ({nb_err*100//nb_brut}% du total)")

    # Distribution par niveau
    dist = df["level"].value_counts()
    print("\n  Distribution des niveaux :")
    for level, cnt in dist.items():
        bar = "█" * (cnt * 40 // nb_brut)
        print(f"    {level:<12} {cnt:>6,}  {bar}")

    # Règle 2 : extraire les codes ERR-XXXX
    df_err["code_erreur"] = df_err["message"].apply(
        lambda m: match.group() if (match := PATTERN_ERR.search(str(m))) else ""
    )

    # Règle 3 : dimensions temporelles
    df_err["timestamp_ts"] = pd.to_datetime(df_err["timestamp"], utc=True, errors="coerce")
    df_err["heure"]    = df_err["timestamp_ts"].dt.hour
    df_err["date_log"] = df_err["timestamp_ts"].dt.date.astype(str)

    # Règle 4 : agrégation par fenêtre horaire
    df_agg = (
        df_err
        .groupby(["service", "date_log", "heure", "code_erreur", "level"])
        .size()
        .reset_index(name="nb_erreurs")
    )
    nb_out = len(df_agg)
    print(f"\n  Agrégats produits    : {nb_out} (service × heure × code_erreur)")

    # Top erreurs
    top = (
        df_agg.groupby(["service", "code_erreur"])["nb_erreurs"]
              .sum()
              .reset_index()
              .sort_values("nb_erreurs", ascending=False)
              .head(15)
    )
    top = top[top["code_erreur"] != ""]
    print("\n  Top erreurs les plus fréquentes :")
    print(top.to_string(index=False))

    write_parquet_partitioned(df_agg, out_path, ["service", "date_log"])
    print(f"\n  Durée Flux 3         : {time.time() - t0:.1f}s")


# ── Rapport final ─────────────────────────────────────────────────────────────

def rapport_final(date: str) -> None:
    sep("RAPPORT FINAL — Fichiers produits (local_output/)")

    total = 0
    for root, dirs, files in os.walk(OUT):
        for f in sorted(files):
            if f.endswith(".parquet"):
                fp = Path(root) / f
                size = fp.stat().st_size
                total += size
                rel = fp.relative_to(BASE_DIR)
                print(f"  {rel}  ({size // 1024} Ko)")

    print(f"\n  Total produit : {total // (1024*1024)} Mo")
    print(f"""
  Récapitulatif métier ({date}) :
    Flux 2 → /ref/cours/          : taux de clôture 14 devises (ECB/Bloomberg/Reuters)
    Flux 1 → /production/transactions/ : agrégats par banque, pseudonymisés RGPD
    Flux 3 → /production/logs/    : comptages horaires erreurs par microservice

  Sur cluster Hadoop, les mêmes transformations s'exécutent via :
    spark-submit --master yarn flux2_cours_boursiers.py {date}
    spark-submit --master yarn flux1_transactions.py {date}
    spark-submit --master yarn --deploy-mode cluster flux3_logs_streaming.py
""")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2024-01-15")
    args = parser.parse_args()
    date = args.date

    csv_check = TEST_DATA / f"transactions_{date}.csv"
    if not csv_check.exists():
        print(f"Erreur : {csv_check} introuvable.")
        print(f"Lancer : python S2_Developpement/scripts/generate_datasets.py --date {date}")
        sys.exit(1)

    print(f"\n{SEP}")
    print(f"  PIPELINE FINDATA SOLUTIONS — Mode pandas local")
    print(f"  pandas {pd.__version__} | pyarrow {pa.__version__}")
    print(f"  Date de traitement : {date}")
    print(SEP)

    t_global = time.time()

    df_cours = run_flux2(date)
    run_flux1(date, df_cours)
    run_flux3(date)
    rapport_final(date)

    print(f"{SEP}")
    print(f"  PIPELINE TERMINE — Durée totale : {time.time() - t_global:.1f}s")
    print(f"{SEP}\n")


if __name__ == "__main__":
    main()
