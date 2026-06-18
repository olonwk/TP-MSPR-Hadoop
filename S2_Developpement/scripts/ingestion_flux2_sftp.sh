#!/bin/bash
# =============================================================================
# ingestion_flux2_sftp.sh
# Ingestion Flux 2 : Cours boursiers SFTP CSV → HDFS
# FinData Solutions | DEPE855
#
# Usage : ./ingestion_flux2_sftp.sh [AAAA-MM-JJ]
# Ce script DOIT réussir AVANT l'exécution de flux1_transactions.py
# =============================================================================

set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
SFTP_HOST="sftp.findata.internal"
SFTP_USER="sftp_reader"
SFTP_KEY="/secure/keys/sftp_rsa_key"
REMOTE_FILE="/data/courses/${DATE}.csv"
LOCAL_TMP="/tmp/courses_${DATE}.csv"
HDFS_TARGET="/raw/courses/${DATE}.csv"
CHECKSUM_FILE="/tmp/courses_${DATE}.md5"

echo "=========================================="
echo " Ingestion Flux 2 — Cours Boursiers"
echo " Date       : ${DATE}"
echo " SFTP source: ${SFTP_HOST}:${REMOTE_FILE}"
echo " Cible HDFS : ${HDFS_TARGET}"
echo "=========================================="

# Nettoyage fichier temporaire éventuel
rm -f "${LOCAL_TMP}" "${CHECKSUM_FILE}"

# Téléchargement depuis SFTP avec vérification de l'intégrité
echo "Téléchargement du fichier CSV depuis SFTP..."
sftp -i "${SFTP_KEY}" -o StrictHostKeyChecking=no \
    "${SFTP_USER}@${SFTP_HOST}" <<EOF
get ${REMOTE_FILE} ${LOCAL_TMP}
get ${REMOTE_FILE}.md5 ${CHECKSUM_FILE}
bye
EOF

if [ ! -f "${LOCAL_TMP}" ]; then
    echo "[ERREUR] Téléchargement SFTP échoué : fichier non disponible pour ${DATE}"
    echo "FALLBACK : Airflow doit déclencher la tâche use_previous_day_cours"
    exit 1
fi

# Vérification intégrité MD5 (si fichier de checksum disponible)
if [ -f "${CHECKSUM_FILE}" ]; then
    EXPECTED_MD5=$(cat "${CHECKSUM_FILE}" | awk '{print $1}')
    ACTUAL_MD5=$(md5sum "${LOCAL_TMP}" | awk '{print $1}')
    if [ "${EXPECTED_MD5}" != "${ACTUAL_MD5}" ]; then
        echo "[ERREUR] Échec de vérification MD5 — fichier corrompu"
        rm -f "${LOCAL_TMP}" "${CHECKSUM_FILE}"
        exit 2
    fi
    echo "Intégrité MD5 vérifiée : OK"
fi

# Vérification que le fichier n'est pas vide
NB_LIGNES=$(wc -l < "${LOCAL_TMP}")
if [ "${NB_LIGNES}" -lt 5 ]; then
    echo "[ERREUR] Fichier CSV trop court (${NB_LIGNES} lignes) — suspect"
    exit 3
fi

# Chargement dans HDFS
echo "Chargement vers HDFS..."
hdfs dfs -put -f "${LOCAL_TMP}" "${HDFS_TARGET}"

# Vérification de la présence dans HDFS
if ! hdfs dfs -test -f "${HDFS_TARGET}"; then
    echo "[ERREUR] Fichier non trouvé dans HDFS après put"
    exit 4
fi

TAILLE_HDFS=$(hdfs dfs -du -h "${HDFS_TARGET}" | awk '{print $1}')
echo "------------------------------------------"
echo " Ingestion Flux 2 terminée avec succès"
echo " Fichier HDFS  : ${HDFS_TARGET}"
echo " Taille        : ${TAILLE_HDFS}"
echo " Lignes CSV    : ${NB_LIGNES}"
echo "------------------------------------------"

rm -f "${LOCAL_TMP}" "${CHECKSUM_FILE}"
exit 0
