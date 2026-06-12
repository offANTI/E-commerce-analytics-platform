from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="E-commerce ETL + dbt pipeline",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "etl"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            "--project-dir /usr/app/dbt/ecommerce_analytics "
            "--profiles-dir /usr/app/dbt "
            "--log-path /tmp/dbt_logs "
            "--target-path /tmp/dbt_target"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            "--project-dir /usr/app/dbt/ecommerce_analytics "
            "--profiles-dir /usr/app/dbt "
            "--log-path /tmp/dbt_logs "
            "--target-path /tmp/dbt_target"
        ),
    )

    dbt_run >> dbt_test