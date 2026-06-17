#!/bin/bash
# =============================================================================
# ingestion_flux1_sqoop.sh
# Ingestion Flux 1 : Transactions bancaires PostgreSQL → HDFS
# FinData Solutions | DEPE855
#
# Usage : ./ingestion_flux1_sqoop.sh [AAAA-MM-JJ]
# =============================================================================

set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
JDBC_URL="jdbc:postgresql://pg-findata.internal:5432/findata_db"
DB_USER="findata_sqoop_reader"
DB_PASSWORD_FILE="/secure/credentials/sqoop_pg_password"
TARGET_HDFS="/raw/transactions/date=${DATE}"
NUM_MAPPERS=4

echo "=========================================="
echo " Ingestion Flux 1 — Transactions"
echo " Date       : ${DATE}"
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
    --password-file "${DB_PASSWORD_FILE}" \
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

echo "Vérification du nombre de lignes chargées dans HDFS..."
LIGNES=$(hdfs dfs -cat "${TARGET_HDFS}/*.parquet" 2>/dev/null | wc -l || echo "N/A")
TAILLE=$(hdfs dfs -du -s -h "${TARGET_HDFS}" 2>/dev/null | awk '{print $1}' || echo "N/A")

echo "------------------------------------------"
echo " Ingestion Flux 1 terminée avec succès"
echo " Répertoire HDFS : ${TARGET_HDFS}"
echo " Taille           : ${TAILLE}"
echo "------------------------------------------"
