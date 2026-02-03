from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
# 1. Cấu hình hệ thống
# ===========================================================
HIVE_METASTORE = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"
DATABASE_BRONZE = "bronze"
TABLE_BRONZE = "it_jobs_raw"
DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"

spark = (
    SparkSession.builder
    .appName("Silver_Etl_Null_Handling_Optimized") 
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
# 2. Watermark & Reading Data
# ===========================================================
last_processed_time = None
if spark.catalog.tableExists(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}"):
    try:
        result = spark.sql(f"SELECT MAX(ingest_time) FROM hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}").collect()
        if result and result[0][0]:
            last_processed_time = result[0][0]
    except Exception:
        pass

print(f">>> Watermark: {last_processed_time}")

df_bronze = spark.table(f"hive_catalog.{DATABASE_BRONZE}.{TABLE_BRONZE}")

if last_processed_time:
    print(">>> INCREMENTAL LOAD: Reading only new data...")
    df_new_records = df_bronze.filter(F.col("ingest_time") > last_processed_time)
else:
    print(">>> FULL LOAD: Reading all data...")
    df_new_records = df_bronze

print(f">>> Processing {df_new_records.count()} records...")

# ===========================================================
# 3. Transformation & Null Handling
# ===========================================================
if df_new_records.count() > 0:
    df_clean = (
        df_new_records
        .toDF(*[c.strip().lower().replace(" ", "_") for c in df_new_records.columns])

        # --- A. Basic Cleaning ---
        .withColumn("job_title", F.trim(F.col("job_title"))) 
        .withColumn("company_name", F.upper(F.trim(F.col("company_name"))))
        
        # --- B. XỬ LÝ NULL QUAN TRỌNG (CRITICAL NULL HANDLING) ---
        # 1. Loại bỏ dòng nếu Job Title hoặc Company Name bị Null hoặc Rỗng
        .filter(
            (F.col("job_title").isNotNull()) & (F.col("job_title") != "") &
            (F.col("company_name").isNotNull()) & (F.col("company_name") != "") &
            (F.col("job_link").isNotNull()) & (F.col("job_link") != "")
        )

        # --- C. Work Mode Handling (Gán mặc định nếu Null) ---
        .withColumn("work_mode", F.trim(F.lower(F.col("work_mode"))))
        .withColumn(
            "work_mode",
            F.when(F.col("work_mode").isNull() | (F.col("work_mode") == ""), "At Office") # Default value
             .when(F.col("work_mode").rlike("(?i)office|onsite|at office|at the office"), "At Office")
             .when(F.col("work_mode").rlike("(?i)hybrid|mix|partly|part-time"), "Hybrid")
             .when(F.col("work_mode").rlike("(?i)remote|home|work from home|wfh"), "Remote")
             .otherwise("At Office")
        )
        
        # --- D. Location Array Handling ---
        # Nếu location null, gán 'Unknown' trước khi split để tránh lỗi
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

        # --- E. Skills Handling (Tránh Null Array) ---
        .withColumn("skills_required", F.regexp_replace(F.col("skills_required"), r"[\[\]'\"()]", "")) 
        # Nếu skill null, trả về chuỗi rỗng để split ra mảng rỗng [] thay vì null
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

        .withColumn("date_posted", F.to_date("date_posted"))
        .select("*") 
        .dropDuplicates(["job_link"]) 
        .withColumn("clean_time", F.current_timestamp())
    )

    # ===========================================================
    # 4. Ghi dữ liệu
    # ===========================================================
    print(f">>> Writing {df_clean.count()} records to Silver...")
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS hive_catalog.{DATABASE_SILVER}")

    if not spark.catalog.tableExists(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}"):
        # Bảng chưa có -> Tạo mới
        (
            df_clean.writeTo(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}")
            .using("iceberg")
            .option("write-format", "parquet")
            .create() 
        )
        print(">>> SUCCESS: Table Created.")
    else:
        (
            df_clean.writeTo(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}")
            .append()
        )
        print(">>> SUCCESS: Data Appended.")

else:
    print(">>> No new data.")

spark.stop()