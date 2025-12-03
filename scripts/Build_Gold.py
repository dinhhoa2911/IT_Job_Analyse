from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
#  System Configuration
# ===========================================================
HIVE_METASTORE = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"

DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"
DATABASE_GOLD = "gold"

# ===========================================================
#  Create SparkSession with Iceberg + Hive
# ===========================================================
spark = (
    SparkSession.builder
    .appName("Build_Gold_Layer_Star_Schema")
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
#  Ensure Gold Namespace Exists
# ===========================================================
spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_catalog.gold")

# ===========================================================
#  DROP ALL GOLD TABLES (FULL-REFRESH GOLD LAYER)
# ===========================================================
print("Dropping existing GOLD tables...")

gold_tables = [
    "fact_job_posting",
    "dim_skill",
    "dim_location",
    "dim_company",
    "dim_date",
    "dim_job_category",
    "dim_work_mode"
]

for tbl in gold_tables:
    spark.sql(f"DROP TABLE IF EXISTS hive_catalog.gold.{tbl}")

print("All GOLD tables dropped. Rebuilding...")

# ===========================================================
#  Load Silver Table
# ===========================================================
df_silver = spark.table(f"hive_catalog.{DATABASE_SILVER}.{TABLE_SILVER}")
print(" Silver record count:", df_silver.count())

# ===========================================================
#   DIM_SKILL  (UPPERCASE skill_name, ID ổn định bằng hash)
# ===========================================================
df_skill = (
    df_silver
    .select(F.explode("skills_required").alias("skill"))
    .filter(F.col("skill").isNotNull() & (F.col("skill") != ""))
    .dropDuplicates(["skill"])
    .withColumn("skill_name", F.upper(F.col("skill")))
    .withColumn("skill_id", F.abs(F.xxhash64("skill_name")).cast("bigint"))  # stable ID
    .withColumn(
        "skill_group",
        F.when(F.col("skill_name").rlike("PYTHON|DATA|SPARK|SQL"), "Data/Backend")
         .when(F.col("skill_name").rlike("AWS|DEVOPS|DOCKER|KUBERNETES"), "DevOps/Cloud")
         .when(F.col("skill_name").rlike("REACT|ANGULAR|FRONTEND|VUE"), "Frontend")
         .otherwise("Other")
    )
    .select("skill_id", "skill_name", "skill_group")
)

df_skill.writeTo("hive_catalog.gold.dim_skill").using("iceberg").create()

# ===========================================================
#   DIM_LOCATION  (rename location → city, ID ổn định bằng hash)
# ===========================================================
df_location = (
    df_silver
    .select(F.col("location").alias("city"))
    .filter(F.col("city").isNotNull())
    .dropDuplicates(["city"])
    .withColumn("location_id", F.abs(F.xxhash64("city")).cast("bigint"))
    .withColumn(
        "region",
        F.when(F.col("city") == "Ha Noi", "Bắc")
         .when(F.col("city") == "Da Nang", "Trung")
         .when(F.col("city") == "Ho Chi Minh", "Nam")
         .otherwise("Khác")
    )
    .withColumn("country", F.lit("Vietnam"))
    .select("location_id", "city", "region", "country")
)

df_location.writeTo("hive_catalog.gold.dim_location").using("iceberg").create()

# ===========================================================
#   DIM_COMPANY (ID hash theo company_name)
# ===========================================================
df_company = (
    df_silver
    .select("company_name")
    .filter(F.col("company_name").isNotNull())
    .dropDuplicates(["company_name"])
    .withColumn("company_id", F.abs(F.xxhash64("company_name")).cast("bigint"))
    .withColumn("industry", F.lit(None).cast("string"))
    .select("company_id", "company_name", "industry")
)

df_company.writeTo("hive_catalog.gold.dim_company").using("iceberg").create()

# ===========================================================
#   DIM_DATE
# ===========================================================
df_date = (
    df_silver
    .select("date_posted")
    .dropna()
    .withColumn("date_id", F.date_format("date_posted", "yyyyMMdd").cast("int"))
    .withColumn("day", F.dayofmonth("date_posted"))
    .withColumn("month", F.month("date_posted"))
    .withColumn("quarter", F.quarter("date_posted"))
    .withColumn("year", F.year("date_posted"))
    .withColumn("month_name", F.date_format("date_posted", "MMMM"))
    .withColumn("day_of_week", F.date_format("date_posted", "EEEE"))
    .dropDuplicates(["date_id"])
    .select("date_id", "date_posted", "day", "month", "quarter", "year", "month_name", "day_of_week")
)

df_date.writeTo("hive_catalog.gold.dim_date").using("iceberg").create()

# ===========================================================
#   DIM_JOB_CATEGORY (chuẩn hóa từ job_category)
# ===========================================================
df_category = (
    df_silver
    .select("job_category")
    .dropna()
    .withColumn("category_raw", F.trim(F.col("job_category")))
    .withColumn("category_name", F.initcap("category_raw"))
    .withColumn(
        "category_group",
        F.when(F.col("category_name").rlike("Data|Analytics|BI"), "Data & Analytics")
         .when(F.col("category_name").rlike("Backend|Frontend|Fullstack|Software|Engineer"), "Software Engineering")
         .when(F.col("category_name").rlike("DevOps|Cloud|SRE|SysOps"), "DevOps / Cloud / Infra")
         .when(F.col("category_name").rlike("AI|Machine Learning|Computer Vision|Deep Learning"), "AI & ML")
         .when(F.col("category_name").rlike("Security|Pentest|Cyber"), "Security")
         .when(F.col("category_name").rlike("Manager|Director|C-level|Lead"), "Management")
         .when(F.col("category_name").rlike("Tester|QA"), "QA / Testing")
         .otherwise("Other")
    )
    .dropDuplicates(["category_name"])
    .withColumn("category_id", F.abs(F.xxhash64("category_name")).cast("bigint"))
    .select("category_id", "category_name", "category_group")
)

df_category.writeTo("hive_catalog.gold.dim_job_category").using("iceberg").create()

# ===========================================================
#   DIM_WORK_MODE (nếu có)
# ===========================================================
has_work_mode = "work_mode" in df_silver.columns

if has_work_mode:
    df_workmode = (
        df_silver
        .select("work_mode")
        .dropna()
        .dropDuplicates(["work_mode"])
        .withColumn("mode_id", F.abs(F.xxhash64("work_mode")).cast("bigint"))
        .select("mode_id", "work_mode")
    )

    df_workmode.writeTo("hive_catalog.gold.dim_work_mode").using("iceberg").create()
else:
    print("  No 'work_mode' column found, skipping dim_work_mode")

# ===========================================================
#   FACT_JOB_POSTING
#   - Join với tất cả dimension
#   - Thêm snapshot_date
#   - Partition theo date_id
# ===========================================================
# Base: explode skill
df_fact_base = (
    df_silver
    .withColumn("skill", F.explode("skills_required"))
    .withColumn("skill", F.upper("skill"))
)

# Nếu có work_mode thì join luôn với dim_work_mode, lấy mode_id
if has_work_mode:
    df_fact_base = df_fact_base.join(df_workmode, "work_mode", "left")
else:
    df_fact_base = df_fact_base.withColumn("mode_id", F.lit(None).cast("bigint"))

df_fact = (
    df_fact_base
    .join(df_skill, df_fact_base.skill == df_skill.skill_name, "left")
    .join(df_location, df_fact_base.location == df_location.city, "left")
    .join(df_company, "company_name", "left")
    .join(df_date, "date_posted", "left")
    .join(
        df_category,
        F.initcap(F.trim(df_fact_base.job_category)) == df_category.category_name,
        "left"
    )
    .withColumn("snapshot_date", F.col("date_id"))  # metadata: ngày snapshot = ngày đăng
    .withColumn(
        "fact_id",
        F.abs(F.xxhash64("job_link", "skill_id", "location_id")).cast("bigint")
    )
    .select(
        "fact_id",
        "job_title",
        "company_id",
        "skill_id",
        "location_id",
        "category_id",
        "mode_id",
        "date_id",
        "snapshot_date",
        "job_link",
        F.lit(1).alias("post_count"),
        "clean_time"
    )
    .dropDuplicates(["fact_id"])
)

(
    df_fact.writeTo("hive_catalog.gold.fact_job_posting")
    .using("iceberg")
    .partitionedBy("date_id")   # nếu lỗi, có thể đổi thành: .partitionedBy(F.col("date_id"))
    .create()
)

print(" Gold layer successfully built!")
print(f" Fact record count: {df_fact.count()}")

spark.stop()
