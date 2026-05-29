from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperatordefault_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='etl_datalake_hive_iceberg',
    default_args=default_args,
    description='ETL pipeline for Data Lakehouse (Bronze → Silver → Gold)',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 17),
    catchup=False,
    tags=['spark', 'iceberg', 'hdfs', 'hive'],
) as dag:

    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command="""
        DOCKER_API_VERSION=1.44 docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Load_Bronze.py
        """,
    )

    clean_silver = BashOperator(
        task_id='clean_silver',
        bash_command="""
        DOCKER_API_VERSION=1.44 docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Clean_Silver.py
        """,
    )

    build_gold = BashOperator(
        task_id='build_gold',
        bash_command="""
        DOCKER_API_VERSION=1.44 docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Build_Gold.py
        """,
    )

    #  Workflow Dependencies
    # ===============================
    load_bronze >> clean_silver >> build_gold
