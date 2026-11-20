from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="sla_daily_report",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "zainab", "retries": 1,
                  "retry_delay": timedelta(minutes=5)},
) as dag:

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command="echo 'Qualité : vérifier doublons ticket_id et valeurs vides'",
    )

    sla_report = BashOperator(
        task_id="sla_report",
        bash_command="echo 'Rapport SLA : MTTR + taux de respect + dépassements'",
    )

    quality_check >> sla_report
