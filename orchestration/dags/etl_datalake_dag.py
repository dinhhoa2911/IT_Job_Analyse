from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ===============================
# DAG Configuration
# ===============================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alert@datateam.local'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='etl_datalake_hive_iceberg',
    default_args=default_args,
    description='ETL pipeline for Data Lakehouse (Bronze → Silver → Gold)',
    schedule_interval='@daily',  # hoặc '0 2 * * *' để chạy mỗi 2h sáng
    start_date=datetime(2025, 10, 17),
    catchup=False,
    tags=['spark', 'iceberg', 'hdfs', 'hive'],
) as dag:

    # ===============================
    #  Load Bronze Layer
    # ===============================
    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Load_Bronze.py
        """,
    )

    # ===============================
    #  Clean Silver Layer
    # ===============================
    clean_silver = BashOperator(
        task_id='clean_silver',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Clean_Silver.py
        """,
    )

    # ===============================
    #  Build Gold Layer
    # ===============================
    build_gold = BashOperator(
        task_id='build_gold',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Build_Gold.py
        """,
    )

    # ===============================
    #  Workflow Dependencies
    # ===============================
    load_bronze >> clean_silver >> build_gold
