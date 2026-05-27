FROM apache/spark:3.5.3

USER root

# Cài đặt system dependencies
RUN apt-get update && apt-get install -y \
    wget curl unzip gnupg ca-certificates \
    libxss1 libappindicator3-1 libasound2 fonts-liberation libnss3 xdg-utils lsb-release \
    xvfb \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Google Chrome (Giữ nguyên để chạy Crawl Data)
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb

# Setup thư mục HOME và thư mục chứa Models
RUN mkdir -p /home/spark && \
    mkdir -p /opt/spark/models && \
    mkdir -p /opt/spark/jars-extra

# --- CẬP NHẬT PHẦN NÀY ---
# Download Jars cho Iceberg, Postgres và AWS S3 (Quan trọng cho MinIO)
# Spark 3.5.3 sử dụng Hadoop 3.3.4, nên ta cần hadoop-aws 3.3.4 và aws-java-sdk-bundle tương thích (1.12.x)

RUN wget -q -O /opt/spark/jars-extra/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar

RUN wget -q -O /opt/spark/jars-extra/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Thêm JARs để Spark nói chuyện với MinIO
RUN wget -q -O /opt/spark/jars-extra/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN wget -q -O /opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN cp /opt/spark/jars-extra/*.jar $SPARK_HOME/jars/
#  Cài đặt Python Libraries ()
RUN pip install --no-cache-dir \
    pandas \
    selenium \
    undetected-chromedriver \
    hdfs \
    pyarrow \
    numpy \
    scikit-learn \
    matplotlib \
    joblib \
    prophet \
    openpyxl \
    pymilvus \
    sentence-transformers

#  Cấu hình Permissions & Environment
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV HOME=/home/spark

COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

# Cấp quyền cho user spark
# Cấp quyền cho thư mục models để code Python có thể ghi file vào
RUN chown -R spark:spark /opt/spark && \
    chown -R spark:spark /home/spark && \
    chmod -R 755 /opt/spark/models

USER spark