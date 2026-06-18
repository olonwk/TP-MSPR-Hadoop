#!/bin/bash
# =============================================================================
# ingestion_flux1_sqoop.sh
# Ingestion Flux 1 : Transactions bancaires PostgreSQL → HDFS
# FinData Solutions | DEPE855
#
# Usage : ./ingestion_flux1_sqoop.sh [AAAA-MM-JJ]
#
# Variables d'environnement attendues :
#   PG_HOST      : hôte PostgreSQL (défaut : postgres)
#   PG_PORT      : port PostgreSQL (défaut : 5432)
#   PG_DB        : nom de la base (défaut : bankdb)
#   PG_USER      : utilisateur (défaut : admin)
#   PG_PASSWORD  : mot de passe (défaut : admin123)
# =============================================================================

set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-bankdb}"
DB_USER="${PG_USER:-admin}"
DB_PASSWORD="${PG_PASSWORD:-admin123}"
JDBC_URL="jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DB}"
TARGET_HDFS="/raw/transactions/date=${DATE}"
NUM_MAPPERS=4

echo "=========================================="
echo " Ingestion Flux 1 — Transactions"
echo " Date       : ${DATE}"
echo " Source     : ${JDBC_URL}"
echo " Cible HDFS : ${TARGET_HDFS}"
echo "=========================================="

# Vérification que la cible n'existe pas déjà (éviter les doublons)
if hdfs dfs -test -d "${TARGET_HDFS}" 2>/dev/null; then
    echo "[WARN] Répertoire cible existe déjà. Suppression avant réingestion..."
    hdfs dfs -rm -r -f "${TARGET_HDFS}"
fi

# Ingestion Sqoop avec export au format Parquet
sqoop import \
    --connect "${JDBC_URL}" \
    --username "${DB_USER}" \
    --password "${DB_PASSWORD}" \
    --table transactions \
    --where "DATE(date_heure) = '${DATE}'" \
    --target-dir "${TARGET_HDFS}" \
    --as-parquetfile \
    --compress \
    --compression-codec snappy \
    --num-mappers ${NUM_MAPPERS} \
    --split-by transaction_id \
    --fetch-size 10000 \
    --mapreduce-job-name "findata-sqoop-flux1-${DATE}"

EXIT_CODE=$?

if [ ${EXIT_CODE} -ne 0 ]; then
    echo "[ERREUR] Sqoop import échoué (code ${EXIT_CODE})"
    exit ${EXIT_CODE}
fi

# Vérification du nombre de fichiers et de la taille dans HDFS
FICHIERS=$(hdfs dfs -count "${TARGET_HDFS}" 2>/dev/null | awk '{print $2}' || echo "N/A")
TAILLE=$(hdfs dfs -du -s -h "${TARGET_HDFS}" 2>/dev/null | awk '{print $1}' || echo "N/A")

echo "------------------------------------------"
echo " Ingestion Flux 1 terminée avec succès"
echo " Répertoire HDFS  : ${TARGET_HDFS}"
echo " Fichiers Parquet : ${FICHIERS}"
echo " Taille           : ${TAILLE}"
echo "------------------------------------------"
