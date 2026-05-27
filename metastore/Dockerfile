FROM apache/hive:3.1.3

USER root

# Cài curl
RUN apt-get update && apt-get install -y curl

# 1. Tải Postgres Driver
RUN curl -L -o /opt/hive/lib/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# 2. Tải thư viện AWS S3
RUN curl -L -o /opt/hive/lib/hadoop-aws-3.1.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar

# 3. Tải AWS SDK Bundle
RUN curl -L -o /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar

# --- THÊM PHẦN NÀY ĐỂ DÙNG SCRIPT CỦA BẠN ---
COPY start-metastore.sh /usr/local/bin/start-metastore.sh
RUN chmod +x /usr/local/bin/start-metastore.sh

# Chuyển về user hive
USER hive

# Chạy script tùy biến thay vì script mặc định
ENTRYPOINT ["/usr/local/bin/start-metastore.sh"]