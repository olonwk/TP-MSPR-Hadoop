-- =============================================================================
-- hive_tables.sql
-- Création des tables externes Apache Hive sur les données HDFS de production
-- FinData Solutions | DEPE855
--
-- Exécution :
--   beeline -u "jdbc:hive2://hiveserver2:10000" -f hive_tables.sql
--   ou : hive -f hive_tables.sql
--
-- Ces tables sont EXTERNES : leur suppression ne supprime pas les données HDFS.
-- Les partitions sont découvertes automatiquement via MSCK REPAIR TABLE.
-- =============================================================================


-- =============================================================================
-- BASE DE DONNÉES
-- =============================================================================

CREATE DATABASE IF NOT EXISTS findata
  COMMENT 'Base de données FinData Solutions — Pipeline Big Data Hadoop'
  LOCATION '/production/';

USE findata;


-- =============================================================================
-- TABLE 1 : TRANSACTIONS AGRÉGÉES (Flux 1)
-- Source HDFS : /production/transactions/annee=AAAA/mois=MM/jour=JJ/
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS findata.transactions_agg (
    code_banque         STRING          COMMENT 'Code de la banque (BNP, SG, CA, ...)',
    volume_total_eur    DOUBLE          COMMENT 'Somme des montants en EUR sur la journée',
    montant_moyen_eur   DOUBLE          COMMENT 'Montant moyen des transactions en EUR',
    nb_transactions     BIGINT          COMMENT 'Nombre total de transactions',
    nb_fraudes          BIGINT          COMMENT 'Nombre de transactions avec statut FRAUD'
)
COMMENT 'Agrégats quotidiens des transactions bancaires par banque et par jour'
PARTITIONED BY (
    annee   INT     COMMENT 'Année de la transaction',
    mois    INT     COMMENT 'Mois de la transaction',
    jour    INT     COMMENT 'Jour de la transaction'
)
STORED AS PARQUET
LOCATION '/production/transactions/'
TBLPROPERTIES (
    'parquet.compress'        = 'SNAPPY',
    'creator'                 = 'FinData-Pipeline-Flux1',
    'classification'          = 'CONFIDENTIEL',
    'data.retention.days'     = '1825'
);

-- Découverte automatique des partitions HDFS existantes
MSCK REPAIR TABLE findata.transactions_agg;


-- =============================================================================
-- TABLE 2 : COURS BOURSIERS (Flux 2 — Référentiel)
-- Source HDFS : /ref/cours/date_partition=AAAA-MM-JJ/
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS findata.cours_boursiers (
    devise              STRING      COMMENT 'Code devise ISO 4217 (USD, GBP, JPY, ...)',
    taux_eur            DOUBLE      COMMENT 'Taux de conversion : 1 EUR = X unités devise',
    horodatage_cloture  TIMESTAMP   COMMENT 'Horodatage du dernier enregistrement de la journée',
    source              STRING      COMMENT 'Source du taux : ECB, Bloomberg, Reuters'
)
COMMENT 'Taux de change de clôture quotidiens par devise — référentiel Flux 1'
PARTITIONED BY (
    date_partition  DATE    COMMENT 'Date du cours boursier'
)
STORED AS PARQUET
LOCATION '/ref/cours/'
TBLPROPERTIES (
    'parquet.compress'    = 'SNAPPY',
    'creator'             = 'FinData-Pipeline-Flux2',
    'classification'      = 'INTERNE'
);

MSCK REPAIR TABLE findata.cours_boursiers;


-- =============================================================================
-- TABLE 3 : LOGS D'ERREURS APPLICATIFS (Flux 3 — Streaming)
-- Source HDFS : /production/logs/service=XXX/date_log=AAAA-MM-JJ/
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS findata.logs_erreurs (
    heure           INT         COMMENT 'Heure de la fenêtre (0-23)',
    code_erreur     STRING      COMMENT 'Code d erreur métier extrait (ERR-XXXX) ou vide si absent',
    level           STRING      COMMENT 'Niveau de log : ERROR ou CRITICAL',
    nb_erreurs      BIGINT      COMMENT 'Nombre d occurrences dans la fenêtre horaire',
    fenetre_debut   TIMESTAMP   COMMENT 'Début de la fenêtre temporelle Spark',
    fenetre_fin     TIMESTAMP   COMMENT 'Fin de la fenêtre temporelle Spark'
)
COMMENT 'Comptages horaires des erreurs applicatives par service — alimentation streaming Kafka'
PARTITIONED BY (
    service     STRING  COMMENT 'Nom du microservice applicatif',
    date_log    DATE    COMMENT 'Date des logs'
)
STORED AS PARQUET
LOCATION '/production/logs/'
TBLPROPERTIES (
    'parquet.compress'    = 'SNAPPY',
    'creator'             = 'FinData-Pipeline-Flux3-Streaming',
    'classification'      = 'INTERNE'
);

-- Note : pour le streaming, les partitions sont ajoutées dynamiquement.
-- Exécuter MSCK REPAIR régulièrement ou via foreachBatch dans le job Spark.
MSCK REPAIR TABLE findata.logs_erreurs;


-- =============================================================================
-- REQUÊTES DE VALIDATION DES TABLES
-- =============================================================================

-- Vérifier que les tables sont bien créées
SHOW TABLES IN findata;

-- Vérifier les partitions disponibles
SHOW PARTITIONS findata.transactions_agg;
SHOW PARTITIONS findata.cours_boursiers;
SHOW PARTITIONS findata.logs_erreurs;

-- Exemple de requête B.I. — Taux de fraude par banque (janvier 2024)
SELECT
    code_banque,
    SUM(nb_transactions)                        AS total_transactions,
    SUM(nb_fraudes)                             AS total_fraudes,
    ROUND(SUM(nb_fraudes) * 100.0
          / NULLIF(SUM(nb_transactions), 0), 2) AS taux_fraude_pct,
    ROUND(SUM(volume_total_eur), 2)             AS volume_total_eur
FROM findata.transactions_agg
WHERE annee = 2024 AND mois = 1
GROUP BY code_banque
ORDER BY taux_fraude_pct DESC;


-- Exemple de requête Data Science — Top erreurs de la semaine
SELECT
    service,
    code_erreur,
    SUM(nb_erreurs) AS total_erreurs,
    COUNT(DISTINCT date_log) AS nb_jours
FROM findata.logs_erreurs
WHERE date_log BETWEEN '2024-01-09' AND '2024-01-15'
  AND code_erreur != ''
GROUP BY service, code_erreur
ORDER BY total_erreurs DESC
LIMIT 20;


-- Exemple de requête Conformité — Volume de transactions pour audit DSP2
SELECT
    annee, mois,
    SUM(nb_transactions)    AS transactions_mois,
    SUM(volume_total_eur)   AS volume_mois_eur,
    SUM(nb_fraudes)         AS fraudes_mois
FROM findata.transactions_agg
WHERE annee BETWEEN 2020 AND 2024
GROUP BY annee, mois
ORDER BY annee, mois;
