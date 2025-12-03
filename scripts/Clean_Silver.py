from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
#  System Configuration
# ===========================================================
HIVE_METASTORE = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"
DATABASE_BRONZE = "bronze"
TABLE_BRONZE = "it_jobs_raw"
DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"

# ===========================================================
#  Create SparkSession with Iceberg + Hive
# ===========================================================
spark = (
    SparkSession.builder
    .appName("Incremental_Clean_Silver_Hive_Iceberg")
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
#  Read Bronze Table
# ===========================================================
df_bronze = spark.table(f"hive_catalog.{DATABASE_BRONZE}.{TABLE_BRONZE}")
print(" Bronze record count:", df_bronze.count())

# ===========================================================
#  Detect new records (not yet in Silver)
# ===========================================================
if spark.catalog.tableExists(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}"):
    df_silver = spark.table(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}")
    df_bronze = df_bronze.join(
        df_silver.select("job_link"), on="job_link", how="left_anti"
    )
    print(f" New records to process: {df_bronze.count()}")
else:
    print(" First time load — processing all Bronze records.")

# ===========================================================
#  Data Cleaning and Standardization
# ===========================================================
df_clean = (
    df_bronze
    # Chuẩn hóa tên cột
    .toDF(*[c.strip().lower().replace(" ", "_") for c in df_bronze.columns])

    # Làm sạch text cơ bản
    .withColumn("job_title", F.initcap(F.trim(F.col("job_title"))))
    .withColumn("company_name", F.upper(F.trim(F.col("company_name"))))
    .withColumn("location", F.trim(F.col("location")))
    .withColumn("work_mode", F.trim(F.lower(F.col("work_mode"))))

    # Chuẩn hóa work_mode
    .withColumn(
        "work_mode",
        F.when(F.col("work_mode").rlike("(?i)office|onsite|at office|at the office"), "At Office")
         .when(F.col("work_mode").rlike("(?i)hybrid|mix|partly|part-time"), "Hybrid")
         .when(F.col("work_mode").rlike("(?i)remote|home|work from home|wfh"), "Remote")
         .otherwise("At Office")
    )

    # Chuẩn hóa location
    .withColumn("location", F.split(F.col("location"), " - "))
    .withColumn("location", F.explode(F.col("location")))
    .withColumn("location", F.trim(F.col("location")))
    .withColumn(
        "location",
        F.when(F.lower(F.col("location")).like("%hcm%"), "Ho Chi Minh")
         .when(F.lower(F.col("location")).like("%hochiminh%"), "Ho Chi Minh")
         .when(F.lower(F.col("location")).like("%tp.hcm%"), "Ho Chi Minh")
         .when(F.lower(F.col("location")).like("%hanoi%"), "Ha Noi")
         .when(F.lower(F.col("location")).like("%ha noi%"), "Ha Noi")
         .when(F.lower(F.col("location")).like("%da nang%"), "Da Nang")
         .otherwise(F.col("location"))
    )

    # Chuẩn hóa kỹ năng
    .withColumn("skills_required", F.lower(F.col("skills_required")))
    .withColumn("skills_required", F.regexp_replace(F.col("skills_required"), r"[\[\]']", ""))  # remove [] or '
    .withColumn("skills_required", F.split(F.col("skills_required"), ",\\s*"))
    # Loại bỏ phần tử rỗng, trim và viết hoa từng phần tử
    .withColumn(
        "skills_required",
        F.expr("transform(filter(transform(skills_required, x -> trim(x)), x -> x <> ''), x -> upper(x))")
    )

    # Chuẩn hóa ngày đăng
    .withColumn("date_posted", F.to_date("date_posted"))

    # Lọc null / rỗng
    .filter(
        (F.col("job_title").isNotNull()) & (F.col("job_title") != "") &
        (F.col("company_name").isNotNull()) & (F.col("company_name") != "") &
        (F.col("location").isNotNull()) & (F.col("location") != "") &
        (F.col("date_posted").isNotNull()) &
        (F.col("job_link").isNotNull()) & (F.col("job_link") != "")
    )

    # Loại trùng trong batch hiện tại
    .dropDuplicates(["job_link"])

    # Thêm metadata
    .withColumn("clean_time", F.current_timestamp())
)

print(f" Cleaned new records: {df_clean.count()}")

# ===========================================================
#  Create Silver Database if Not Exists
# ===========================================================
spark.sql(f"CREATE DATABASE IF NOT EXISTS hive_catalog.{DATABASE_SILVER}")

# ===========================================================
#  Append new cleaned data to Silver
# ===========================================================
if df_clean.count() > 0:
    (
        df_clean.writeTo(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}")
        .using("iceberg")
        .append()
    )
    print(f" Appended {df_clean.count()} new records to Silver.")
else:
    print(" No new records to append — Silver is up to date.")

spark.stop()
