from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# DAG Configuration
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
    dag_id='system_dag',
    default_args=default_args,
    description='Full pipeline for System include ETL pipeline and ML Optimization Revenue',
    schedule_interval='@daily',  # hoặc '0 2 * * *' để chạy mỗi 2h sáng
    start_date=datetime(2025, 10, 17),
    catchup=False,
    tags=['spark', 'iceberg', 'hdfs', 'hive'],
) as dag:

    
    #  Load Bronze Layer   
    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Load_Bronze.py
        """,
    )
    
    #  Clean Silver Layer
    clean_silver = BashOperator(
        task_id='clean_silver',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Clean_Silver.py
        """,
    )

    #  Build Gold Layer
    build_gold = BashOperator(
        task_id='build_gold',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/Build_Gold.py
        """,
    )

    #  Features Engineering
    extract_features_spark = BashOperator(
        task_id='extract_features_spark',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/extract_features_spark.py
        """,
    )

    #  Train model LightGBM
    train_lightGBM = BashOperator(
        task_id='train_lightGBM',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/train_lightGBM.py
        """,
    )

    #  Predict model LightBGM
    price_optimize = BashOperator(
        task_id='price_optimize',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/price_optimize.py
        """,
    )

    #  Separate customer cluster with Guassian Mixture
    cluster_customers = BashOperator(
        task_id='cluster_customers',
        bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/airflow/scripts/cluster_customers.py
        """,
    )

    #  Workflow Dependencies
    # load_bronze >> clean_silver >> [build_gold, extract_features_spark, cluster_customers]
    # extract_features_spark >> train_lightGBM >> price_optimize
    load_bronze >> clean_silver >> build_gold >> extract_features_spark >> train_lightGBM >> price_optimize >> cluster_customers