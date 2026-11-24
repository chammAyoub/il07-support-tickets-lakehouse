from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def quality_check():
    """Vérifie la qualité des données Gold. Échoue si un problème est trouvé."""
    hook = PostgresHook(postgres_conn_id="kpi_db")

    nulls = hook.get_first(
        "SELECT count(*) FROM gold_tickets "
        "WHERE category IS NULL OR status IS NULL OR priority_pred IS NULL"
    )[0]
    bad_status = hook.get_first(
        "SELECT count(*) FROM gold_tickets "
        "WHERE status NOT IN ('OPEN','IN_PROGRESS','RESOLVED','CLOSED')"
    )[0]
    negative = hook.get_first(
        "SELECT count(*) FROM gold_tickets WHERE resolution_hours < 0"
    )[0]

    print(f"Valeurs vides: {nulls} | Statuts invalides: {bad_status} | Délais négatifs: {negative}")
    if nulls or bad_status or negative:
        raise ValueError("Contrôle qualité échoué : voir les compteurs ci-dessus")


def sla_report():
    """Calcule MTTR, taux SLA et dépassements, puis les enregistre par jour."""
    hook = PostgresHook(postgres_conn_id="kpi_db")

    hook.run("""
        CREATE TABLE IF NOT EXISTS sla_report_daily (
            report_date     DATE PRIMARY KEY,
            resolved_count  INT,
            mttr_hours      NUMERIC,
            sla_rate_pct    NUMERIC,
            breaches        INT
        );
    """)

    hook.run("""
        INSERT INTO sla_report_daily
        SELECT CURRENT_DATE,
               count(*),
               round(avg(resolution_hours), 1),
               round(100.0 * count(*) FILTER (WHERE resolution_hours <= sla_target_hours) / count(*), 1),
               count(*) FILTER (WHERE resolution_hours > sla_target_hours)
        FROM gold_tickets
        WHERE resolution_hours IS NOT NULL
        ON CONFLICT (report_date) DO UPDATE SET
            resolved_count = EXCLUDED.resolved_count,
            mttr_hours     = EXCLUDED.mttr_hours,
            sla_rate_pct   = EXCLUDED.sla_rate_pct,
            breaches       = EXCLUDED.breaches;
    """)
    print("Rapport SLA du jour enregistré dans sla_report_daily")


with DAG(
    dag_id="sla_daily_report",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "zainab", "retries": 1,
                  "retry_delay": timedelta(minutes=5)},
) as dag:

    t_quality = PythonOperator(task_id="quality_check", python_callable=quality_check)
    t_report = PythonOperator(task_id="sla_report", python_callable=sla_report)

    t_quality >> t_report
