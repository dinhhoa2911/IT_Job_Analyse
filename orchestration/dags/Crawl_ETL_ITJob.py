from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ===============================
# CONFIGURATION
# ===============================
# Định nghĩa chung các packages cần thiết cho Spark để kết nối MinIO & Iceberg
# Lưu ý: Bắt buộc phải có hadoop-aws và aws-java-sdk-bundle cho mọi Job đọc/ghi S3
SPARK_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hoa856856@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='crawl_etl_itjob',
    default_args=default_args,
    description='ETL pipeline for Data Lakehouse (Crawl -> Bronze -> Silver -> Gold)',
    schedule_interval='0 2 * * *',  # Chạy mỗi ngày lúc 2h sáng
    start_date=datetime(2025, 10, 17),
    catchup=False,
    tags=['spark', 'iceberg', 'minio', 'itviec'],
) as dag:

    # ===============================
    #  Task 1: Crawl ITviec Jobs
    # ===============================
    # Task này chạy Selenium nên cần xvfb-run
    crawl_data = BashOperator(
        task_id='crawl_data',
        bash_command='docker exec spark-submit bash -c "xvfb-run -a python3 /opt/spark/scripts/Crawl_Data.py"',
    )

    # ===============================
    #  Task 2: Load Bronze Layer
    # ===============================
    # FIX: Thêm 'docker exec' để chạy lệnh trong container spark-submit
    # FIX: Thêm đầy đủ packages AWS S3
    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command=f"""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages {SPARK_PACKAGES} \
        /opt/spark/scripts/Load_Bronze.py
        '
        """,
    )

    # ===============================
    #  Task 3: Clean Silver Layer
    # ===============================
    # FIX: Thêm packages AWS S3 (vì Silver cần đọc Bronze từ MinIO)
    clean_silver = BashOperator(
        task_id='clean_silver',
        bash_command=f"""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages {SPARK_PACKAGES} \
        /opt/spark/scripts/Clean_Silver.py
        '
        """,
    )

    # ===============================
    #  Task 4: Build Gold Layer
    # ===============================
    # FIX: Thêm packages AWS S3 (vì Gold cần đọc Silver từ MinIO)
    build_gold = BashOperator(
        task_id='build_gold',
        bash_command=f"""
        docker exec spark-submit bash -c '
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --packages {SPARK_PACKAGES} \
        /opt/spark/scripts/Build_Gold.py
        '
        """,
    )

    # ===============================
    #  Workflow Dependencies
    # ===============================
    crawl_data >> load_bronze >> clean_silver >> build_gold