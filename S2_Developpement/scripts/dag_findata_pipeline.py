"""
dag_findata_pipeline.py
Apache Airflow DAG — Orchestration du pipeline FinData Solutions (v2)
DEPE855 | Exécution quotidienne à 06h00 UTC

Changements v2 :
  - Sqoop remplacé par NiFi pour le Flux 1 (check de présence HDFS post-NiFi)
  - Flux 3 : job Spark Structured Streaming CONTINU (démarré comme service YARN séparé)
              → le DAG vérifie uniquement que le topic Kafka est actif
  - Ajout tâche MSCK REPAIR Hive pour mettre à jour les partitions

Séquence batch (Airflow) :
  1. check_nifi_flux2_sftp          → vérifie le fichier CSV SFTP dans HDFS raw
  2. transform_flux2                → PySpark : cours bruts → /ref/cours/  + Hive REPAIR
  3. check_nifi_flux1_hdfs          → vérifie que NiFi a déposé les transactions dans HDFS
  4. transform_flux1                → PySpark : transactions → /production/ + Hive REPAIR
  5. check_kafka_flux3              → vérifie que le topic Kafka est actif et peuplé
  6. repair_hive_logs               → MSCK REPAIR TABLE findata.logs_erreurs
  7. notify_success

Service continu (hors DAG) :
  flux3_logs_streaming.py → démarré comme service YARN long-running
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.hdfs_sensor import HdfsSensor

SCRIPTS_DIR = "/opt/findata/scripts"
SPARK_CONF = {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "10",
    "spark.dynamicAllocation.initialExecutors": "4",
    "spark.shuffle.service.enabled": "true",
    "spark.sql.catalogImplementation": "hive",
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


def fallback_cours_j_minus_1(**context):
    """
    Fallback : si le fichier CSV SFTP n'est pas disponible,
    copie le référentiel de la veille avec tag FALLBACK.
    Déclenché par Airflow si le sensor HDFS timeout.
    """
    import subprocess
    ds = context["ds"]             # date du jour YYYY-MM-DD
    ds_prev = context["prev_ds"]   # date de la veille
    cmd = (
        f"hadoop fs -cp /ref/cours/date_partition={ds_prev}/ "
        f"/ref/cours/date_partition={ds}/ && "
        f"echo 'FALLBACK: taux {ds_prev} utilisés pour {ds}'"
    )
    subprocess.run(cmd, shell=True, check=True)


with DAG(
    dag_id="findata_pipeline_quotidien_v2",
    default_args=default_args,
    description="Pipeline Big Data FinData v2 — NiFi + Kafka Streaming + Hive",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    max_active_runs=1,
    tags=["findata", "production", "v2"],
) as dag:

    # ── FLUX 2 : Vérification SFTP → HDFS (NiFi a déposé le fichier) ────────
    check_flux2_hdfs = HdfsSensor(
        task_id="check_flux2_csv_dans_hdfs",
        filepath="/raw/courses/{{ ds }}.csv",
        hdfs_conn_id="hdfs_default",
        timeout=7200,
        poke_interval=300,
        mode="reschedule",
        soft_fail=True,   # Si absent → déclenche le fallback sans bloquer
    )

    fallback_flux2 = PythonOperator(
        task_id="fallback_cours_veille",
        python_callable=fallback_cours_j_minus_1,
        trigger_rule="one_failed",   # S'exécute si check_flux2 échoue
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
        trigger_rule="none_failed_min_one_success",
    )

    # Mise à jour des partitions Hive après Flux 2
    repair_hive_cours = BashOperator(
        task_id="repair_hive_cours_boursiers",
        bash_command=(
            "beeline -u 'jdbc:hive2://hiveserver2:10000' "
            "-e 'MSCK REPAIR TABLE findata.cours_boursiers;'"
        ),
    )

    # ── FLUX 1 : Check que NiFi a bien déposé les données HDFS ──────────────
    # NiFi tourne en continu et dépose les fichiers à 06h30 automatiquement
    check_flux1_hdfs = HdfsSensor(
        task_id="check_flux1_nifi_dans_hdfs",
        filepath="/raw/transactions/date={{ ds }}/",
        hdfs_conn_id="hdfs_default",
        timeout=3600,
        poke_interval=120,
        mode="reschedule",
    )

    # ── FLUX 1 : Transformation Spark ───────────────────────────────────────
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

    # Mise à jour des partitions Hive après Flux 1
    repair_hive_transactions = BashOperator(
        task_id="repair_hive_transactions_agg",
        bash_command=(
            "beeline -u 'jdbc:hive2://hiveserver2:10000' "
            "-e 'MSCK REPAIR TABLE findata.transactions_agg;'"
        ),
    )

    # ── FLUX 3 : Vérification que le job Kafka Streaming est actif ──────────
    # Le job flux3_logs_streaming.py tourne comme service YARN continu.
    # On vérifie juste qu'il est vivant et que le topic Kafka reçoit des messages.
    check_kafka_flux3 = BashOperator(
        task_id="check_kafka_flux3_actif",
        bash_command=(
            "kafka-consumer-groups.sh --bootstrap-server ${KAFKA_BROKERS} "
            "--describe --group findata-spark-streaming-logs 2>&1 | "
            "grep -q 'findata.logs.raw' && echo 'Kafka streaming OK' || "
            "(echo 'WARN: streaming inactif — relancer flux3_logs_streaming.py' && exit 0)"
        ),
        env={"KAFKA_BROKERS": "kafka:9092"},
    )

    # Mise à jour des partitions Hive logs (le streaming ajoute des partitions en continu)
    repair_hive_logs = BashOperator(
        task_id="repair_hive_logs_erreurs",
        bash_command=(
            "beeline -u 'jdbc:hive2://hiveserver2:10000' "
            "-e 'MSCK REPAIR TABLE findata.logs_erreurs;'"
        ),
    )

    # ── Notification succès ──────────────────────────────────────────────────
    notify_success = BashOperator(
        task_id="notify_pipeline_success",
        bash_command=(
            "if [ -n \"${SLACK_WEBHOOK_URL:-}\" ]; then "
            "curl -s -X POST \"$SLACK_WEBHOOK_URL\" "
            "-H 'Content-type: application/json' "
            "--data '{\"text\":\"Pipeline FinData v2 {{ ds }} terminé. Hive partitions mises à jour.\"}'; "
            "else echo 'Notification Slack ignorée (SLACK_WEBHOOK_URL non définie)'; fi"
        ),
        trigger_rule="all_success",
    )

    # ── Définition des dépendances ───────────────────────────────────────────
    #
    #  check_flux2_hdfs ──(fail)──→ fallback_flux2 ──┐
    #  check_flux2_hdfs ──(ok)──────────────────────┘
    #                              ↓
    #                         transform_flux2 → repair_hive_cours ──┐
    #                                                                ↓
    #  check_flux1_hdfs ────────────────────────────→ transform_flux1 → repair_hive_trans ──┐
    #                                                                                        ├→ notify_success
    #  check_kafka_flux3 ────────────────────────────────────────→ repair_hive_logs ─────────┘

    check_flux2_hdfs >> [transform_flux2, fallback_flux2]
    fallback_flux2 >> transform_flux2
    transform_flux2 >> repair_hive_cours
    repair_hive_cours >> transform_flux1
    check_flux1_hdfs >> transform_flux1
    transform_flux1 >> repair_hive_transactions
    check_kafka_flux3 >> repair_hive_logs
    [repair_hive_transactions, repair_hive_logs] >> notify_success
