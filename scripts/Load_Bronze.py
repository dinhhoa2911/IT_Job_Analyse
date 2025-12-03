from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
#  Cấu hình hệ thống
# ===========================================================
HIVE_METASTORE = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"
INPUT_PATH = "hdfs://dinhhoa-master:9000/data_lake/raw/itviec_jobs_full.csv"  # file CSV trong container
DATABASE = "bronze"
TABLE_NAME = "it_jobs_raw"

# ===========================================================
#  Tạo SparkSession với Iceberg + Hive
# ===========================================================
spark = (
    SparkSession.builder
    .appName("Load_Bronze_Hive_Iceberg")
    .config("hive.metastore.uris", HIVE_METASTORE)
    .enableHiveSupport()
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", HIVE_METASTORE)
    .config("spark.sql.catalog.hive_catalog.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# ===========================================================
#  Đọc file CSV gốc
# ===========================================================
df_raw = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(INPUT_PATH)
)

print(" Record loaded from CSV:", df_raw.count())
df_raw.printSchema()

# ===========================================================
#  Thêm metadata cho Bronze Layer
# ===========================================================
df_bronze_new = (
    df_raw
    .withColumn("ingest_time", F.current_timestamp())
    .withColumn("source", F.lit("ITviec"))
)

# ===========================================================
#  Tạo database bronze nếu chưa có
# ===========================================================
spark.sql(f"CREATE DATABASE IF NOT EXISTS hive_catalog.{DATABASE}")

# ===========================================================
#  Append dữ liệu mới (tránh ghi đè)
# ===========================================================
if not spark.catalog.tableExists(f"hive_catalog.{DATABASE}.{TABLE_NAME}"):
    # Tạo mới bảng nếu chưa tồn tại
    (
        df_bronze_new.writeTo(f"hive_catalog.{DATABASE}.{TABLE_NAME}")
        .using("iceberg")
        .option("write-format", "parquet")
        .create()
    )
else:
    # Nếu bảng đã tồn tại → chỉ thêm record mới chưa có job_link
    df_existing = spark.table(f"hive_catalog.{DATABASE}.{TABLE_NAME}")
    df_to_append = df_bronze_new.join(
        df_existing.select("job_link"), on="job_link", how="left_anti"
    )

    print(f" New records to append: {df_to_append.count()}")

    if df_to_append.count() > 0:
        (
            df_to_append.writeTo(f"hive_catalog.{DATABASE}.{TABLE_NAME}")
            .append()
        )
    else:
        print(" No new records to append.")

print(f" Loaded into: hive_catalog.{DATABASE}.{TABLE_NAME}")

spark.stop()
