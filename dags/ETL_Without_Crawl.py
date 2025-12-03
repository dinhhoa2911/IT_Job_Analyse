from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ===========================================================
# DAG CONFIG
# ===========================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alert@datateam.local'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_without_crawl',
    default_args=default_args,
    description='Manual ETL: Local CSV → HDFS → Bronze → Silver → Gold',
    schedule_interval=None,  # chạy thủ công
    start_date=datetime(2025, 10, 28),
    catchup=False,
    tags=['manual', 'spark', 'iceberg', 'hdfs'],
) as dag:

    # =======================================================
    #  PUT FILE TO HDFS (local → /data_lake/raw)
    # =======================================================
    put_hdfs = BashOperator(
        task_id='put_hdfs',
        bash_command="""
        docker exec spark-submit bash -c '
        python3 /opt/spark/scripts/puthdfs.py
        '
        """,
    )

    # =======================================================
    #  LOAD BRONZE (HDFS → Iceberg.bronze)
    # =======================================================
    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command="""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --name Load_Bronze_Hive_Iceberg \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
            /opt/spark/scripts/Load_Bronze.py
        '
        """,
    )

    # =======================================================
    #  CLEAN SILVER (Iceberg.bronze → Iceberg.silver)
    # =======================================================
    clean_silver = BashOperator(
        task_id='clean_silver',
        bash_command="""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --name Clean_Silver_Hive_Iceberg \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
            /opt/spark/scripts/Clean_Silver.py
        '
        """,
    )

    # =======================================================
    #  BUILD GOLD (Iceberg.silver → Iceberg.gold)
    # =======================================================
    build_gold = BashOperator(
        task_id='build_gold',
        bash_command="""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --name Build_Gold_Hive_Iceberg \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
            /opt/spark/scripts/Build_Gold.py
        '
        """,
    )

    # =======================================================
    # WORKFLOW DEPENDENCIES
    # =======================================================
    put_hdfs >> load_bronze >> clean_silver >> build_gold
