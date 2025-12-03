FROM apache/airflow:2.7.1

USER airflow

# cài provider/trino client để dùng TrinoOperator / hook
RUN pip install --no-cache-dir --upgrade "apache-airflow-providers-openlineage>=1.8.0"
