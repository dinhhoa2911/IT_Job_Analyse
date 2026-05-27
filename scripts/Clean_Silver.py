from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
# 1. Cấu hình hệ thống (MinIO & Iceberg)
# ===========================================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Đường dẫn Warehouse trên S3 (MinIO)
WAREHOUSE_PATH = "s3a://warehouse/iceberg_data"
HIVE_METASTORE = "thrift://hive-metastore:9083"

CATALOG_NAME = "iceberg"
DATABASE_BRONZE = "bronze"
TABLE_BRONZE = "it_jobs_raw"
DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"

spark = (
    SparkSession.builder
    .appName("Silver_Etl_Cleaning_Merge") 
    # --- Cấu hình MinIO (S3) ---
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    # --- Cấu hình Iceberg & Hive Metastore ---
    .config("hive.metastore.uris", HIVE_METASTORE)
    .enableHiveSupport()
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", HIVE_METASTORE)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# ===========================================================
# 2. Watermark & Reading Data
# ===========================================================
FULL_SILVER_TABLE = f"{CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}"
FULL_BRONZE_TABLE = f"{CATALOG_NAME}.{DATABASE_BRONZE}.{TABLE_BRONZE}"

last_processed_time = None

# Kiểm tra bảng Silver đã tồn tại chưa để lấy Watermark
if spark.catalog.tableExists(FULL_SILVER_TABLE):
    try:
        # Lấy thời gian ingest_time lớn nhất đã xử lý trong Silver
        # Lưu ý: ingest_time này phải được bảo toàn từ Bronze sang Silver
        result = spark.sql(f"SELECT MAX(ingest_time) FROM {FULL_SILVER_TABLE}").collect()
        if result and result[0][0]:
            last_processed_time = result[0][0]
    except Exception as e:
        print(f"Warning: Could not get watermark ({e})")

print(f">>> Watermark (Last Processed Time): {last_processed_time}")

# Đọc bảng Bronze
df_bronze = spark.table(FULL_BRONZE_TABLE)

if last_processed_time:
    print(">>> INCREMENTAL LOAD: Reading only new data...")
    # Lọc dữ liệu mới hơn watermark
    df_new_records = df_bronze.filter(F.col("ingest_time") > last_processed_time)
else:
    print(">>> FULL LOAD: Reading all data...")
    df_new_records = df_bronze

count_new = df_new_records.count()
print(f">>> Processing {count_new} records...")

# ===========================================================
# 3. Transformation & Null Handling
# ===========================================================
if count_new > 0:
    df_clean = (
        df_new_records
        # Chuẩn hóa tên cột: trim khoảng trắng, chữ thường, thay space bằng _
        .toDF(*[c.strip().lower().replace(" ", "_") for c in df_new_records.columns])

        # --- A. Basic Cleaning ---
        .withColumn("job_title", F.trim(F.col("job_title"))) 
        .withColumn("company_name", F.upper(F.trim(F.col("company_name"))))
        
        # --- B. XỬ LÝ NULL QUAN TRỌNG (CRITICAL NULL HANDLING) ---
        .filter(
            (F.col("job_title").isNotNull()) & (F.col("job_title") != "") &
            (F.col("company_name").isNotNull()) & (F.col("company_name") != "") &
            (F.col("job_link").isNotNull()) & (F.col("job_link") != "")
        )

        # --- C. Work Mode Handling ---
        .withColumn("work_mode", F.trim(F.lower(F.col("work_mode"))))
        .withColumn(
            "work_mode",
            F.when(F.col("work_mode").isNull() | (F.col("work_mode") == ""), "At Office")
             .when(F.col("work_mode").rlike("(?i)office|onsite|at office|at the office"), "At Office")
             .when(F.col("work_mode").rlike("(?i)hybrid|mix|partly|part-time"), "Hybrid")
             .when(F.col("work_mode").rlike("(?i)remote|home|work from home|wfh"), "Remote")
             .otherwise("At Office")
        )
        
        # --- D. Location Array Handling ---
        .withColumn("location", F.coalesce(F.col("location"), F.lit("Unknown")))
        .withColumn("location_array", F.split(F.col("location"), " - "))
        .withColumn(
            "location",
            F.expr("""
                transform(location_array, x -> 
                    CASE 
                        WHEN lower(trim(x)) LIKE '%hcm%' OR lower(trim(x)) LIKE '%hochiminh%' OR lower(trim(x)) LIKE '%tp.hcm%' THEN 'Ho Chi Minh'
                        WHEN lower(trim(x)) LIKE '%hanoi%' OR lower(trim(x)) LIKE '%ha noi%' THEN 'Ha Noi'
                        WHEN lower(trim(x)) LIKE '%da nang%' THEN 'Da Nang'
                        ELSE trim(x)
                    END
                )
            """)
        )
        .drop("location_array")

        # --- E. Skills Handling ---
        .withColumn("skills_required", F.regexp_replace(F.col("skills_required"), r"[\[\]'\"()]", "")) 
        .withColumn("skills_required", F.coalesce(F.col("skills_required"), F.lit("")))
        .withColumn("skills_required", F.split(F.col("skills_required"), ","))
        .withColumn(
            "skills_required",
            F.expr("""
                array_distinct(
                    filter(
                        transform(skills_required, x -> upper(trim(x))),
                        x -> x is not null and x != ''
                    )
                )
            """)
        )

        # --- F. Chuyển đổi kiểu dữ liệu ---
        .withColumn("date_posted", F.to_date("date_posted")) # Spark tự động cast chuỗi chuẩn YYYY-MM-DD
        
        # Deduplicate trong batch hiện tại trước khi merge
        .dropDuplicates(["job_link"]) 
        .withColumn("clean_time", F.current_timestamp())
    )

    # ===========================================================
    # 4. Ghi dữ liệu (MERGE INTO - UPSERT)
    # ===========================================================
    print(f">>> Writing cleaned data to Silver...")

    # Tạo Database nếu chưa có
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_SILVER}")

    if not spark.catalog.tableExists(FULL_SILVER_TABLE):
        print(f">>> Table {FULL_SILVER_TABLE} not found. Creating new...")
        # Lần đầu tiên: Tạo bảng
        (
            df_clean.writeTo(FULL_SILVER_TABLE)
            .using("iceberg")
            .tableProperty("format-version", "2") # Bắt buộc dùng V2 để hỗ trợ Merge/Update
            .create()
        )
        print(">>> SUCCESS: Table Created.")
    else:
        print(f">>> Table exists. Performing MERGE (Upsert)...")
        
        # Tạo Temp View cho DataFrame nguồn
        df_clean.createOrReplaceTempView("source_data")
        
        # MERGE INTO:
        # - Nếu job_link trùng -> Update lại toàn bộ thông tin (xử lý trường hợp tin tuyển dụng cập nhật)
        # - Nếu không trùng -> Insert mới
        merge_query = f"""
        MERGE INTO {FULL_SILVER_TABLE} AS target
        USING source_data AS source
        ON target.job_link = source.job_link
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        spark.sql(merge_query)
        print(">>> SUCCESS: Data Merged (Upsert).")

else:
    print(">>> No new data to process.")

spark.stop()