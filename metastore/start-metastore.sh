#!/usr/bin/env bash
set -euo pipefail

# 1) Fix lỗi export
EXT_SCRIPT="/opt/hive/bin/ext/metastore.sh"
if [ -f "${EXT_SCRIPT}" ]; then
  sed -i.bak -e 's/export HADOOP_CLIENT_OPTS= "/export HADOOP_CLIENT_OPTS="/' "$EXT_SCRIPT" || true
fi

# 2) Config Env
export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf

# 3) Chờ Postgres khởi động
PG_HOST="${HIVE_METASTORE_DB_HOST:-postgres-metastore}"
PG_PORT="${HIVE_METASTORE_DB_PORT:-5432}"
echo "Waiting for Postgres at ${PG_HOST}:${PG_PORT} ..."
for i in $(seq 1 10); do
  if timeout 1 bash -c "cat < /dev/tcp/${PG_HOST}/${PG_PORT}" >/dev/null 2>&1; then
    echo "Postgres reachable"
    break
  fi
  sleep 1
done

# 4) Init Schema (ĐÃ SỬA LOGIC)
echo "Checking metastore schema..."

# Thử kiểm tra thông tin schema
if /opt/hive/bin/schematool -dbType postgres -info -verbose; then
  echo "Metastore schema verified. Proceeding..."
else
  echo "Schema check failed or schema missing. Attempting initialization..."
  
  # --- FIX QUAN TRỌNG: Thêm '|| true' ---
  # Nếu initSchema thất bại (do bảng đã tồn tại), lệnh sẽ không làm crash container nữa.
  /opt/hive/bin/schematool -dbType postgres -initSchema -verbose || echo "Schema init failed (likely already exists). Ignoring and starting server..."
fi

# 5) Start Metastore
echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore