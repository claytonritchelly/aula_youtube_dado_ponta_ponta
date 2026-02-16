from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "clayton",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_flights_klax_docker",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual por enquanto
    catchup=False,
    tags=["flights", "etl"],
) as dag:

    extract = BashOperator(
        task_id="extract_bronze",
        bash_command="""
        export AIRFLOW_HOME=/opt/airflow
        python /opt/airflow/src/extract_bronze_flights.py
        """,
    )

    transform = BashOperator(
        task_id="transform_silver",
        bash_command="""
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
        export PATH=$JAVA_HOME/bin:$PATH
        python /opt/airflow/src/transform_silver_flights.py
        """,
    )

    load = BashOperator(
        task_id="load_gold",
        bash_command="""
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
        export PATH=$JAVA_HOME/bin:$PATH
        python /opt/airflow/src/load_gold_flights.py
        """,
    )

    extract >> transform >> load
