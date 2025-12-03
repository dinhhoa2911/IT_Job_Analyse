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
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='crawl_etl_itjob',
    default_args=default_args,
    description='ETL pipeline for Data Lakehouse (Crawl → Bronze → Silver → Gold)',
    schedule_interval='0 2 * * *',  # chạy mỗi ngày lúc 2h sáng
    start_date=datetime(2025, 10, 17),
    catchup=False,
    tags=['spark', 'iceberg', 'hdfs', 'hive', 'itviec'],
) as dag:

    # ===============================
    #  Task 1: Crawl ITviec Jobs
    # ===============================
    crawl_data = BashOperator(
        task_id='crawl_data',
        bash_command='docker exec spark-submit bash -c "xvfb-run -a python3 /opt/spark/scripts/Crawl_Data.py"',
    )

    # ===============================
    #  Task 2: Load Bronze Layer
    # ===============================
    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command="""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
        /opt/spark/scripts/Load_Bronze.py
        '
        """,
    )

    # ===============================
    #  Task 3: Clean Silver Layer
    # ===============================
    clean_silver = BashOperator(
        task_id='clean_silver',
        bash_command="""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
        /opt/spark/scripts/Clean_Silver.py
        '
        """,
    )

    # ===============================
    #  Task 4: Build Gold Layer
    # ===============================
    build_gold = BashOperator(
        task_id='build_gold',
        bash_command="""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
        /opt/spark/scripts/Build_Gold.py
        '
        """,
    )

    # ===============================
    #  Workflow Dependencies
    # ===============================
    crawl_data >> load_bronze >> clean_silver >> build_gold
