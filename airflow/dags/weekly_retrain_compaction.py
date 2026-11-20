from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="weekly_retrain_compaction",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"owner": "zainab", "retries": 1,
                  "retry_delay": timedelta(minutes=5)},
) as dag:

    retrain_nlp = BashOperator(
        task_id="retrain_nlp",
        bash_command="echo 'Réentraînement du modèle NLP (script Ayoub)'",
    )

    hudi_compaction = BashOperator(
        task_id="hudi_compaction",
        bash_command="echo 'Compaction/purge Hudi (job Mohamed)'",
    )

    retrain_nlp >> hudi_compaction
