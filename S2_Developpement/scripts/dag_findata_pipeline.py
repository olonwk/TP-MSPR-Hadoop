"""
dag_findata_pipeline.py
Apache Airflow DAG — Orchestration du pipeline FinData Solutions
DEPE855 | Exécution quotidienne à 06h00 UTC

Séquence :
  1. ingest_flux2_sftp        → SFTP CSV vers HDFS raw
  2. transform_flux2           → PySpark : cours bruts → /ref/cours/
  3. ingest_flux1_sqoop        → Sqoop PostgreSQL vers HDFS raw (parallèle avec étape 1+2)
  4. transform_flux1           → PySpark : transactions → /production/transactions/ (dépend de 2+3)
  5. ingest_flux3_nifi_check   → Vérifie que NiFi a alimenté /staging/logs/
  6. transform_flux3           → PySpark : logs → /production/logs/
  7. notify_success            → Notification Slack en cas de succès total

Dépendances critiques :
  transform_flux1 attend transform_flux2 ET ingest_flux1_sqoop
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

SCRIPTS_DIR = "/opt/findata/scripts"
SPARK_CONF = {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "10",
    "spark.dynamicAllocation.initialExecutors": "4",
    "spark.shuffle.service.enabled": "true",
}

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-engineers@findata.fr"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="findata_pipeline_quotidien",
    default_args=default_args,
    description="Pipeline Big Data FinData Solutions — 3 flux quotidiens",
    schedule_interval="0 6 * * *",     # Chaque jour à 06h00 UTC
    start_date=datetime(2024, 1, 15),
    catchup=False,
    max_active_runs=1,
    tags=["findata", "production", "big-data"],
) as dag:

    # ── FLUX 2 : Ingestion SFTP ──────────────────────────────────────────────
    ingest_flux2 = BashOperator(
        task_id="ingest_flux2_sftp",
        bash_command=f"bash {SCRIPTS_DIR}/ingestion_flux2_sftp.sh {{{{ ds }}}}",
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # ── FLUX 2 : Transformation Spark ───────────────────────────────────────
    transform_flux2 = SparkSubmitOperator(
        task_id="transform_flux2_cours",
        application=f"{SCRIPTS_DIR}/flux2_cours_boursiers.py",
        application_args=["{{ ds }}"],
        name="findata-flux2-{{ ds }}",
        conn_id="spark_default",
        conf=SPARK_CONF,
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
    )

    # ── FLUX 1 : Ingestion Sqoop ─────────────────────────────────────────────
    ingest_flux1 = BashOperator(
        task_id="ingest_flux1_sqoop",
        bash_command=f"bash {SCRIPTS_DIR}/ingestion_flux1_sqoop.sh {{{{ ds }}}}",
    )

    # ── FLUX 1 : Transformation Spark (dépend de Flux 2 + ingestion Flux 1) ─
    transform_flux1 = SparkSubmitOperator(
        task_id="transform_flux1_transactions",
        application=f"{SCRIPTS_DIR}/flux1_transactions.py",
        application_args=["{{ ds }}"],
        name="findata-flux1-{{ ds }}",
        conn_id="spark_default",
        conf=SPARK_CONF,
        executor_cores=4,
        executor_memory="8g",
        driver_memory="4g",
    )

    # ── FLUX 3 : Vérification NiFi staging ──────────────────────────────────
    check_flux3_staging = BashOperator(
        task_id="check_flux3_staging_nifi",
        bash_command="hdfs dfs -test -d /staging/logs/ && echo 'Staging OK' || (echo 'Staging vide' && exit 1)",
    )

    # ── FLUX 3 : Transformation Spark ───────────────────────────────────────
    transform_flux3 = SparkSubmitOperator(
        task_id="transform_flux3_logs",
        application=f"{SCRIPTS_DIR}/flux3_logs.py",
        application_args=["{{ ds }}"],
        name="findata-flux3-{{ ds }}",
        conn_id="spark_default",
        conf=SPARK_CONF,
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
    )

    # ── Notification succès ──────────────────────────────────────────────────
    notify_success = BashOperator(
        task_id="notify_pipeline_success",
        bash_command=(
            "curl -s -X POST $SLACK_WEBHOOK_URL "
            "-H 'Content-type: application/json' "
            "--data '{\"text\":\"Pipeline FinData {{ ds }} terminé avec succès\"}'"
        ),
        trigger_rule="all_success",
    )

    # ── Définition des dépendances ───────────────────────────────────────────
    #
    #  ingest_flux2 → transform_flux2 ──┐
    #                                   ├──→ transform_flux1 ──┐
    #  ingest_flux1 ────────────────────┘                      │
    #                                                          ├──→ notify_success
    #  check_flux3_staging → transform_flux3 ──────────────────┘

    ingest_flux2 >> transform_flux2
    [transform_flux2, ingest_flux1] >> transform_flux1
    check_flux3_staging >> transform_flux3
    [transform_flux1, transform_flux3] >> notify_success
