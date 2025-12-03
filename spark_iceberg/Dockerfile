FROM apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y wget curl unzip && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars-extra

# Download Iceberg Spark runtime
RUN wget -q -O /opt/spark/jars-extra/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar

# Download postgres JDBC driver
RUN wget -q -O /opt/spark/jars-extra/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

ENV PATH="$SPARK_HOME/bin:$PATH"

RUN cp /opt/spark/jars-extra/*.jar $SPARK_HOME/jars/

COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

USER spark
