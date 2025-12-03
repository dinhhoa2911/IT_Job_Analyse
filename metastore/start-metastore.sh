#!/usr/bin/env bash
set -euo pipefail

# 1) Patch known buggy export line in ext/metastore.sh (idempotent)
EXT_SCRIPT="/opt/hive/bin/ext/metastore.sh"
if [ -f "${EXT_SCRIPT}" ]; then
  # replace 'export HADOOP_CLIENT_OPTS= " ... "'  -> 'export HADOOP_CLIENT_OPTS=" ... "'
  sed -i.bak -e 's/export HADOOP_CLIENT_OPTS= "/export HADOOP_CLIENT_OPTS="/' "$EXT_SCRIPT" || true
fi

# 2) Ensure HIVE_CONF_DIR present
export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf

# 3) Wait for Postgres to be reachable (simple TCP check)
PG_HOST="${HIVE_METASTORE_DB_HOST:-postgres-metastore}"
PG_PORT="${HIVE_METASTORE_DB_PORT:-5432}"
echo "Waiting for Postgres at ${PG_HOST}:${PG_PORT} ..."
# loop until TCP port open (bash /dev/tcp)
for i in $(seq 1 10); do
  if timeout 1 bash -c "cat < /dev/tcp/${PG_HOST}/${PG_PORT}" >/dev/null 2>&1; then
    echo "Postgres reachable"
    break
  fi
  echo "Postgres not ready yet ($i/10) — sleeping 1s"
  sleep 1
done

# 4) Init schema if needed (try info first); if fails, initSchema
echo "Checking metastore schema..."
if /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "Metastore schema already initialized"
else
  echo "Initializing metastore schema (Postgres)..."
  /opt/hive/bin/schematool -dbType postgres -initSchema -verbose
fi

# 5) Start metastore (replace process)
echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore
