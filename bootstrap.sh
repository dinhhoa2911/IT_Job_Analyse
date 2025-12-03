#!/usr/bin/env bash
set -euo pipefail

NETWORK=system_network

# Hàm chờ service sẵn sàng
wait_for_service() {
  local host=$1
  local port=$2
  echo "⏳ Waiting for $host:$port..."
  while ! (echo > /dev/tcp/$host/$port) >/dev/null 2>&1; do
    sleep 2
  done
  echo "✅ $host:$port is ready!"
}

# 1. ensure docker network
if ! docker network ls | grep -qw "$NETWORK"; then
  echo "Creating docker network $NETWORK"
  docker network create "$NETWORK"
fi

# 2. grant exec permission for script.sh
chmod +x hadoop-cluster/init-datanode.sh || true
chmod +x hadoop-cluster/start-hdfs.sh || true
chmod +x metastore/start-metastore.sh || true
chmod +x orchestration/deploy/airflow/start-airflow.sh || true

# 3. bring up modules theo thứ tự
echo "🚀 Starting Hadoop cluster..."
(cd hadoop-cluster && docker compose up -d)

echo "🚀 Starting Metastore..."
(cd ../metastore && docker compose up -d --build)

# chờ PostgreSQL trong metastore (ví dụ service tên là metastore-db, port 5432)
wait_for_service metastore-db 5432

echo "🚀 Starting Spark/Iceberg..."
(cd ../spark_iceberg && docker compose up -d --build)

echo "🚀 Starting Trino..."
(cd ../trino && docker compose up -d --build)

# chờ Trino coordinator
wait_for_service trino 8080

echo "🚀 Starting Superset..."
(cd ../superset && docker compose up -d --build)

# chờ Superset
wait_for_service superset 8088

echo "🚀 Starting Airflow..."
(cd ../airflow && docker compose up -d --build)

# chờ Postgres của Airflow
wait_for_service airflow-db 5432

# 4. initializations (superset, airflow)
cd ../superset
echo "⚙ Initializing Superset..."
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin \
    --username admin --firstname Admin --lastname User \
    --email admin@example.com --password admin
docker exec -it superset superset init

cd ../airflow
echo "⚙ Initializing Airflow..."
docker compose -f airflow/docker-compose.airflow.yml run --rm airflow-init

echo "🎉 All services up and initialized!"
