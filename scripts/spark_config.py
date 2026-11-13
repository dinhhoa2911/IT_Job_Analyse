"""Cấu hình driver Spark/tiện ích trong scripts, chỉ dùng standard library.

Không import Spark, không đọc .env tự động, không kết nối dịch vụ.
Compose truyền scripts/.env vào container spark-submit; local dùng environment.
"""

from dataclasses import dataclass, field
import os
from typing import Mapping, Optional


@dataclass(frozen=True)
class SparkSettings:
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = field(default="", repr=False)
    minio_secret_key: str = field(default="", repr=False)
    warehouse_path: str = "s3a://warehouse/iceberg_data"
    hive_metastore_uri: str = "thrift://hive-metastore:9083"
    weekly_warehouse_path: str = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"
    cluster_warehouse_path: str = "hdfs://tuankiet170-master:9000/data_lake/warehouse"
    hdfs_host: str = "dinhhoa-master"
    hdfs_port: int = 9000
    hdfs_user: str = "root"
    milvus_host: str = "milvus-standalone"
    milvus_port: str = "19530"
    embedding_batch_size: int = 500
    openai_api_key: str = field(default="", repr=False)


def load_settings(
    environ: Optional[Mapping[str, str]] = None, *,
    require_minio: bool = False, require_openai: bool = False,
) -> SparkSettings:
    """Environment > defaults; chỉ yêu cầu credentials của nhánh được gọi."""
    env = os.environ if environ is None else environ
    required = []
    if require_minio:
        required.extend(("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"))
    if require_openai:
        required.append("OPENAI_API_KEY")
    missing = [name for name in required if not env.get(name, "").strip()]
    if missing:
        raise ValueError("Missing required configuration: " + ", ".join(missing))

    def positive_int(name, default):
        try:
            value = int(env.get(name, str(default)))
        except ValueError:
            raise ValueError(f"{name} must be a positive integer") from None
        if value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        return value

    defaults = SparkSettings()
    return SparkSettings(
        minio_endpoint=env.get("SPARK_MINIO_ENDPOINT", defaults.minio_endpoint),
        minio_access_key=env.get("MINIO_ACCESS_KEY", ""),
        minio_secret_key=env.get("MINIO_SECRET_KEY", ""),
        warehouse_path=env.get("SPARK_WAREHOUSE_PATH", defaults.warehouse_path),
        hive_metastore_uri=env.get("HIVE_METASTORE_URI", defaults.hive_metastore_uri),
        weekly_warehouse_path=env.get("SPARK_WEEKLY_WAREHOUSE_PATH", defaults.weekly_warehouse_path),
        cluster_warehouse_path=env.get("SPARK_CLUSTER_WAREHOUSE_PATH", defaults.cluster_warehouse_path),
        hdfs_host=env.get("HDFS_HOST", defaults.hdfs_host),
        hdfs_port=positive_int("HDFS_PORT", defaults.hdfs_port),
        hdfs_user=env.get("HDFS_USER", defaults.hdfs_user),
        milvus_host=env.get("MILVUS_HOST", defaults.milvus_host),
        milvus_port=env.get("MILVUS_PORT", defaults.milvus_port),
        embedding_batch_size=positive_int("EMBEDDING_BATCH_SIZE", defaults.embedding_batch_size),
        openai_api_key=env.get("OPENAI_API_KEY", ""),
    )
