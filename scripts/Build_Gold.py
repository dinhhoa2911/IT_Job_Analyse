from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
# 1. Cấu hình hệ thống (MinIO & Iceberg)
# ===========================================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Đường dẫn Warehouse trên S3
WAREHOUSE_PATH = "s3a://warehouse/iceberg_data"
HIVE_METASTORE = "thrift://hive-metastore:9083"

# Tên Catalog thống nhất với các bước trước
CATALOG_NAME = "iceberg" 

DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"
DATABASE_GOLD = "gold"

spark = (
    SparkSession.builder
    .appName("Build_Gold_Layer_Star_Schema")
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
# 2. Chuẩn bị Gold Namespace & Cleanup (Full Refresh)
# ===========================================================
# Tạo Database Gold nếu chưa có
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_GOLD}")

print(">>> Dropping existing GOLD tables for Full Refresh...")
# Thứ tự drop quan trọng: Drop Fact trước -> Drop Dim sau
gold_tables = [
    "fact_job_posting",
    "dim_skill",
    "dim_location",
    "dim_company",
    "dim_date",
    "dim_work_mode",
    "dim_job_category"
]

for tbl in gold_tables:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_GOLD}.{tbl}")

# Đọc dữ liệu từ Silver
print(f">>> Reading Silver Data from {CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}...")
try:
    df_silver = spark.table(f"{CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}")
    print(f">>> Loaded Silver Data: {df_silver.count()} records.")
except Exception as e:
    print(f"ERROR: Could not read Silver table. Ensure Clean_Silver.py ran successfully. Error: {e}")
    spark.stop()
    exit(1)

# ===========================================================
# 3. Build Dimensions
# ===========================================================

# --- A. DIM_SKILL ---
print(">>> Building DIM_SKILL...")
df_dim_skill = (
    df_silver
    .select(F.explode("skills_required").alias("skill_name"))
    .filter(F.col("skill_name").isNotNull() & (F.col("skill_name") != ""))
    .dropDuplicates(["skill_name"])
    .withColumn("skill_id", F.abs(F.xxhash64("skill_name")).cast("bigint"))
    .withColumn(
        "skill_group",
        F.when(F.col("skill_name").rlike("(?i)PYTHON|JAVA|NET|PHP|GO|NODE|RUBY"), "Backend")
         .when(F.col("skill_name").rlike("(?i)REACT|VUE|ANGULAR|JS|HTML|CSS"), "Frontend")
         .when(F.col("skill_name").rlike("(?i)SQL|DATA|SPARK|HADOOP|KAFKA|AWS|AZURE"), "Data & Cloud")
         .otherwise("Other")
    )
    .select("skill_id", "skill_name", "skill_group")
)
df_dim_skill.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.dim_skill").using("iceberg").create()

# --- B. DIM_LOCATION ---
print(">>> Building DIM_LOCATION...")
df_dim_location = (
    df_silver
    .select(F.explode("location").alias("city_name"))
    .filter(F.col("city_name").isNotNull())
    .dropDuplicates(["city_name"])
    .withColumn("location_id", F.abs(F.xxhash64("city_name")).cast("bigint"))
    .withColumn(
        "region",
        F.when(F.col("city_name").isin("Ha Noi", "Bac Ninh", "Hung Yen"), "North")
         .when(F.col("city_name").isin("Da Nang", "Hue", "Nghe An"), "Central")
         .when(F.col("city_name").isin("Ho Chi Minh", "Binh Duong", "Can Tho"), "South")
         .otherwise("Other")
    )
    .select("location_id", "city_name", "region")
)
df_dim_location.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.dim_location").using("iceberg").create()

# --- C. DIM_COMPANY ---
print(">>> Building DIM_COMPANY...")
df_dim_company = (
    df_silver
    .select("company_name")
    .filter(F.col("company_name").isNotNull())
    .dropDuplicates(["company_name"])
    .withColumn("company_id", F.abs(F.xxhash64("company_name")).cast("bigint"))
    .select("company_id", "company_name")
)
df_dim_company.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.dim_company").using("iceberg").create()

# --- D. DIM_WORK_MODE ---
print(">>> Building DIM_WORK_MODE...")
df_dim_work_mode = (
    df_silver
    .select("work_mode")
    .filter(F.col("work_mode").isNotNull())
    .dropDuplicates(["work_mode"])
    .withColumn("mode_id", F.abs(F.xxhash64("work_mode")).cast("bigint"))
    .select("mode_id", "work_mode")
)
df_dim_work_mode.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.dim_work_mode").using("iceberg").create()

# --- E. DIM_DATE ---
print(">>> Building DIM_DATE...")
df_dim_date = (
    df_silver
    .select("date_posted")
    .filter(F.col("date_posted").isNotNull())
    .dropDuplicates(["date_posted"])
    .withColumn("date_id", F.date_format("date_posted", "yyyyMMdd").cast("int"))
    .withColumn("full_date", F.col("date_posted"))
    .withColumn("day", F.dayofmonth("date_posted"))
    .withColumn("month", F.month("date_posted"))
    .withColumn("year", F.year("date_posted"))
    .withColumn("quarter", F.quarter("date_posted"))
    .withColumn("day_of_week", F.date_format("date_posted", "EEEE"))
    .select("date_id", "full_date", "day", "month", "year", "quarter", "day_of_week")
)
df_dim_date.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.dim_date").using("iceberg").create()

# --- F. DIM_JOB_CATEGORY ---
print(">>> Building DIM_JOB_CATEGORY...")
df_dim_category = (
    df_silver
    .select("job_title")
    .distinct()
    .withColumn("title_lower", F.lower(F.col("job_title")))
    .withColumn(
        "category_name",
        F.when(F.col("title_lower").rlike("data|analytics|bi |ai |machine learning"), "Data & AI")
         .when(F.col("title_lower").rlike("tester|qa|qc|test"), "Testing & QA")
         .when(F.col("title_lower").rlike("devops|cloud|sre|system"), "DevOps & Infra")
         .when(F.col("title_lower").rlike("frontend|mobile|android|ios|react|vue"), "Frontend & Mobile")
         .when(F.col("title_lower").rlike("backend|java|net|php|golang|python|ruby"), "Backend")
         .when(F.col("title_lower").rlike("fullstack|software engineer|developer"), "Software Engineering")
         .when(F.col("title_lower").rlike("manager|lead|head|director|cto"), "Management")
         .when(F.col("title_lower").rlike("ba |business analyst|product owner|product manager"), "Product & BA")
         .otherwise("Other")
    )
    .select("category_name")
    .dropDuplicates(["category_name"])
    .withColumn("category_id", F.abs(F.xxhash64("category_name")).cast("bigint"))
)
df_dim_category.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.dim_job_category").using("iceberg").create()

# ===========================================================
# 4. Build Fact Table (FACT_JOB_POSTING)
# ===========================================================
print(">>> Building FACT_JOB_POSTING...")

# 1. Chuẩn bị base data (Explode để nhân dòng theo Location và Skill)
# Lưu ý: Một Job có 3 skill và 2 location sẽ sinh ra 3*2 = 6 dòng Fact
df_fact_base = (
    df_silver
    .withColumn("loc_single", F.explode("location"))
    .withColumn("skill_single", F.explode("skills_required"))
    
    # Tính toán lại category tạm thời để join
    .withColumn("title_lower", F.lower(F.col("job_title")))
    .withColumn(
        "derived_category",
        F.when(F.col("title_lower").rlike("data|analytics|bi |ai |machine learning"), "Data & AI")
         .when(F.col("title_lower").rlike("tester|qa|qc|test"), "Testing & QA")
         .when(F.col("title_lower").rlike("devops|cloud|sre|system"), "DevOps & Infra")
         .when(F.col("title_lower").rlike("frontend|mobile|android|ios|react|vue"), "Frontend & Mobile")
         .when(F.col("title_lower").rlike("backend|java|net|php|golang|python|ruby"), "Backend")
         .when(F.col("title_lower").rlike("fullstack|software engineer|developer"), "Software Engineering")
         .when(F.col("title_lower").rlike("manager|lead|head|director|cto"), "Management")
         .when(F.col("title_lower").rlike("ba |business analyst|product owner|product manager"), "Product & BA")
         .otherwise("Other")
    )
)

# 2. Join với Dimensions để lấy ID
df_fact = (
    df_fact_base.alias("f")
    .join(df_dim_company.alias("c"), F.col("f.company_name") == F.col("c.company_name"), "left")
    .join(df_dim_location.alias("l"), F.col("f.loc_single") == F.col("l.city_name"), "left")
    .join(df_dim_skill.alias("s"), F.col("f.skill_single") == F.col("s.skill_name"), "left")
    .join(df_dim_work_mode.alias("w"), F.col("f.work_mode") == F.col("w.work_mode"), "left")
    .join(df_dim_category.alias("cat"), F.col("f.derived_category") == F.col("cat.category_name"), "left")
    
    # Tạo Date ID
    .withColumn("date_id", F.date_format("f.date_posted", "yyyyMMdd").cast("int"))
)

# 3. Chọn cột ID và Metrics
df_fact_final = (
    df_fact
    .select(
        F.col("f.job_link"),
        F.col("f.job_title"),
        F.col("c.company_id"),
        F.col("l.location_id"),
        F.col("s.skill_id"),
        F.col("w.mode_id"),
        F.col("cat.category_id"),
        F.col("date_id"),
        F.lit(1).alias("one_posting") # Metric đếm số lượng
    )
    # Tạo Fact ID duy nhất cho mỗi dòng
    .withColumn(
        "fact_id", 
        F.abs(F.xxhash64("job_link", "location_id", "skill_id")).cast("bigint")
    )
)

# 4. Ghi Fact Table (Partition theo ngày để tối ưu truy vấn)
(
    df_fact_final.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.fact_job_posting")
    .using("iceberg")
    .partitionedBy("date_id")
    .create()
)

print(f">>> GOLD Layer Built Successfully. Fact records: {df_fact_final.count()}")

spark.stop()