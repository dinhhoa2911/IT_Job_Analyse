from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# ===========================================================
#  Cấu hình hệ thống (MinIO Data Lake)
# ===========================================================
# Thông tin kết nối MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Đường dẫn dữ liệu (Lưu ý dùng giao thức s3a://)
# Warehouse: Nơi Iceberg lưu file data/metadata thực tế
WAREHOUSE_PATH = "s3a://warehouse/iceberg_data" 
# Input: File CSV bạn vừa fix lỗi font và upload lên MinIO
INPUT_PATH = "s3a://warehouse/raw/itviec_jobs_full.csv"

HIVE_METASTORE = "thrift://hive-metastore:9083"
CATALOG_NAME = "iceberg" # Đặt tên catalog ngắn gọn là iceberg
DATABASE = "bronze"
TABLE_NAME = "it_jobs_raw"

# ===========================================================
#  Tạo SparkSession với cấu hình MinIO & Iceberg
# ===========================================================
spark = (
    SparkSession.builder
    .appName("Load_Bronze_MinIO_Iceberg")
    # --- Cấu hình kết nối MinIO (S3) ---
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    # --- Cấu hình Iceberg Catalog (sử dụng Hive Metastore) ---
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", HIVE_METASTORE)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)
    
    # Kích hoạt Iceberg Extensions
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# ===========================================================
#  Đọc file CSV từ MinIO
# ===========================================================
print(f"Reading data from: {INPUT_PATH}")
df_raw = (
    spark.read
    .option("header", True)
    .option("multiLine", True) # Hỗ trợ dòng mới trong ô (description)
    .option("quote", '"')
    .option("escape", '"')
    .option("inferSchema", True)
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
    # Ép kiểu date_only về DateType chuẩn để Iceberg tối ưu Partition (nếu cần sau này)
    # Giả sử format trong CSV là YYYY-MM-DD
    .withColumn("date_only", F.to_date(F.col("date_only"), "yyyy-MM-dd"))
)

# ===========================================================
#  Tạo database bronze nếu chưa có
# ===========================================================
# Lưu ý: Lệnh này tạo namespace trong Hive Metastore
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE}")

# ===========================================================
#  Ghi dữ liệu vào Iceberg (Deduplication)
# ===========================================================
FULL_TABLE_NAME = f"{CATALOG_NAME}.{DATABASE}.{TABLE_NAME}"

if not spark.catalog.tableExists(FULL_TABLE_NAME):
    print(f"Table {FULL_TABLE_NAME} does not exist. Creating new...")
    (
        df_bronze_new.writeTo(FULL_TABLE_NAME)
        .using("iceberg")
        .tableProperty("format-version", "2") # Sử dụng Iceberg V2 (hỗ trợ update/delete tốt hơn)
        .create()
    )
    print("Create table success.")
else:
    print(f"Table {FULL_TABLE_NAME} exists. Checking for new records...")
    
    # Logic: Chỉ thêm những job_link chưa tồn tại trong bảng Bronze
    df_existing = spark.table(FULL_TABLE_NAME)
    
    # Left Anti Join: Lấy những dòng bên TRÁI (new) KHÔNG có bên PHẢI (existing)
    df_to_append = df_bronze_new.join(
        df_existing.select("job_link"), 
        on="job_link", 
        how="left_anti"
    )

    count_new = df_to_append.count()
    print(f" New records to append: {count_new}")

    if count_new > 0:
        (
            df_to_append.writeTo(FULL_TABLE_NAME)
            .append()
        )
        print("Append success.")
    else:
        print("No new records found. Skip writing.")

print(f"Process finished. Data available at: {FULL_TABLE_NAME}")

spark.stop()