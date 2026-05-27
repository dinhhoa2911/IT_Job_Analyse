from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
# 1. Cấu hình & Dictionaries
# ===========================================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
WAREHOUSE_PATH = "s3a://warehouse/iceberg_data"
HIVE_METASTORE = "thrift://hive-metastore:9083"

CATALOG_NAME = "iceberg" 
DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"
DATABASE_GOLD = "gold"

SKILL_GROUP_MAPPING = {
    "PYTHON": "Backend", "JAVA": "Backend", "GO": "Backend", "PHP": "Backend", 
    "RUBY": "Backend", "NODE.JS": "Backend", ".NET": "Backend",
    "REACT": "Frontend", "NEXT.JS": "Frontend", "ANGULAR": "Frontend", 
    "VUE": "Frontend", "HTML": "Frontend", "CSS": "Frontend", 
    "JAVASCRIPT": "Frontend", "TYPESCRIPT": "Frontend", "TAILWIND CSS": "Frontend", "BOOTSTRAP": "Frontend",
    "ANDROID": "Mobile", "IOS": "Mobile", "SWIFT": "Mobile", "FLUTTER": "Mobile", "REACT_NATIVE": "Mobile",
    "MYSQL": "Database", "POSTGRESQL": "Database", "SQL SERVER": "Database", 
    "MONGODB": "Database", "REDIS": "Database", "SNOWFLAKE": "Database",
    "AWS": "Cloud & DevOps", "AZURE": "Cloud & DevOps", "GOOGLE CLOUD": "Cloud & DevOps", 
    "DOCKER": "Cloud & DevOps", "KUBERNETES": "Cloud & DevOps", "JENKINS": "Cloud & DevOps", "CI_CD": "Cloud & DevOps",
    "SPARK": "Data Engineering", "HADOOP": "Data Engineering", "KAFKA": "Data Engineering", "AIRFLOW": "Data Engineering",
    "ML": "AI & ML", "DL": "AI & ML", "TENSORFLOW": "AI & ML", "PYTORCH": "AI & ML", "GENERATIVE AI": "AI & ML",
    "SELENIUM": "Testing", "AUTOMATION TEST": "Testing", "MANUAL TEST": "Testing",
    "REST API": "Software Engineering", "MICROSERVICE": "Software Engineering", "OBJECT ORIENTED PROGRAMMING": "Software Engineering"
}

CATEGORY_REGEX = {
    "Data Engineering": r"data engineer|etl|elt|big data|data platform|data pipeline|spark|hadoop|airflow|kafka",
    "AI & Machine Learning": r"data scientist|machine learning|ml engineer|ai engineer|deep learning|computer vision|nlp|llm|generative ai|genai",
    "Data Analytics": r"data analyst|business intelligence|bi analyst|analytics engineer|reporting analyst",
    "DevOps & Infrastructure": r"devops|site reliability|sre|cloud engineer|platform engineer|system engineer|infrastructure|infra engineer",
    "Cyber Security": r"security|cybersecurity|soc|penetration test|pentest|information security|application security",
    "Testing & QA": r"\bqa\b|\bqc\b|tester|test engineer|automation test|manual test|quality assurance",
    "Mobile Development": r"android|ios|flutter|react native|mobile developer|swift|kotlin",
    "Frontend Development": r"frontend|front end|react|vue|angular|javascript|typescript|next\.js|nuxt",
    "Backend Development": r"backend|back end|java developer|spring|php|laravel|golang|go developer|python developer|django|flask|fastapi|ruby on rails|node\.js|\.net",
    "Fullstack Development": r"fullstack|full stack",
    "Embedded & IoT": r"embedded|firmware|iot|hardware engineer|embedded software",
    "Game Development": r"game developer|unity|unreal engine|game engineer",
    "ERP & CRM": r"sap|erp|crm|odoo|salesforce|dynamics 365",
    "Product & Business Analysis": r"business analyst|\bba\b|product owner|product manager|scrum master|agile coach",
    "Management": r"manager|team lead|technical lead|head of|director|cto|vp engineering",
    "Software Engineering": r"software engineer|software developer|developer"
}

# ===========================================================
# 2. Các hàm tiện ích
# ===========================================================
def create_spark_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("hive.metastore.uris", HIVE_METASTORE)
        .enableHiveSupport()
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", HIVE_METASTORE)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )

def get_category_expr(col_name="job_title"):
    """Tự động sinh chuỗi F.when() rlike để tránh lặp code dài dòng"""
    title_lower = F.lower(F.trim(F.col(col_name)))
    expr = None
    for category, regex_pattern in CATEGORY_REGEX.items():
        if expr is None:
            expr = F.when(title_lower.rlike(regex_pattern), category)
        else:
            expr = expr.when(title_lower.rlike(regex_pattern), category)
    return expr.otherwise("Other")

def save_to_gold(df, table_name, partition_col=None):
    writer = df.writeTo(f"{CATALOG_NAME}.{DATABASE_GOLD}.{table_name}").using("iceberg")
    if partition_col:
        writer = writer.partitionedBy(partition_col)
    writer.create()

# ===========================================================
# 3. Main Execution
# ===========================================================
if __name__ == "__main__":
    spark = create_spark_session("Build_Gold_Layer_Star_Schema")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_GOLD}")
    print(">>> Dropping existing GOLD tables for Full Refresh...")
    
    gold_tables = ["fact_job_posting", "dim_skill", "dim_location", "dim_company", "dim_date", "dim_work_mode", "dim_job_category"]
    for tbl in gold_tables:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_GOLD}.{tbl}")

    print(f">>> Reading Silver Data from {CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}...")
    try:
        df_silver = spark.table(f"{CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}")
        print(f">>> Loaded Silver Data: {df_silver.count()} records.")
    except Exception as e:
        print(f"ERROR: Could not read Silver table. Ensure Clean_Silver.py ran successfully. Error: {e}")
        spark.stop()
        exit(1)

    # --- A. DIM_SKILL ---
    print(">>> Building DIM_SKILL...")
    group_expr = F.create_map([F.lit(x) for kv in SKILL_GROUP_MAPPING.items() for x in kv])
    df_dim_skill = (
        df_silver.select(F.explode("skills_required").alias("skill_name"))
        .filter(F.col("skill_name").isNotNull() & (F.col("skill_name") != ""))
        .dropDuplicates(["skill_name"])
        .withColumn("skill_id", F.abs(F.xxhash64("skill_name")).cast("bigint"))
        .withColumn("skill_group", F.coalesce(group_expr[F.col("skill_name")], F.lit("Other")))
        .select("skill_id", "skill_name", "skill_group")
    )
    save_to_gold(df_dim_skill, "dim_skill")

    # --- B. DIM_LOCATION ---
    print(">>> Building DIM_LOCATION...")
    df_dim_location = (
        df_silver.select(F.explode("location").alias("city_name"))
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
    save_to_gold(df_dim_location, "dim_location")

    # --- C. DIM_COMPANY ---
    print(">>> Building DIM_COMPANY...")
    df_dim_company = (
        df_silver.select("company_name").filter(F.col("company_name").isNotNull())
        .dropDuplicates(["company_name"])
        .withColumn("company_id", F.abs(F.xxhash64("company_name")).cast("bigint"))
        .select("company_id", "company_name")
    )
    save_to_gold(df_dim_company, "dim_company")

    # --- D. DIM_WORK_MODE ---
    print(">>> Building DIM_WORK_MODE...")
    df_dim_work_mode = (
        df_silver.select("work_mode").filter(F.col("work_mode").isNotNull())
        .dropDuplicates(["work_mode"])
        .withColumn("mode_id", F.abs(F.xxhash64("work_mode")).cast("bigint"))
        .select("mode_id", "work_mode")
    )
    save_to_gold(df_dim_work_mode, "dim_work_mode")

    # --- E. DIM_DATE ---
    print(">>> Building DIM_DATE...")
    df_dim_date = (
        df_silver.select("date_posted").filter(F.col("date_posted").isNotNull())
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
    save_to_gold(df_dim_date, "dim_date")

    # --- F. DIM_JOB_CATEGORY ---
    print(">>> Building DIM_JOB_CATEGORY...")
    df_dim_category = (
        df_silver.select("job_title").distinct()
        .withColumn("category_name", get_category_expr("job_title"))
        .select("category_name").dropDuplicates(["category_name"])
        .withColumn("category_id", F.abs(F.xxhash64("category_name")).cast("bigint"))
    )
    save_to_gold(df_dim_category, "dim_job_category")

    # --- G. FACT_JOB_POSTING ---
    print(">>> Building FACT_JOB_POSTING...")
    df_fact_base = (
        df_silver
        .drop("skill_groups")
        .withColumn("loc_single", F.explode("location"))
        .withColumn("skill_single", F.explode("skills_required"))
        # Sử dụng lại hàm get_category_expr thay vì viết lại cả khối when.rlike()
        .withColumn("derived_category", get_category_expr("job_title"))
    )

    df_fact_final = (
        df_fact_base.alias("f")
        .join(df_dim_company.alias("c"), F.col("f.company_name") == F.col("c.company_name"), "left")
        .join(df_dim_location.alias("l"), F.col("f.loc_single") == F.col("l.city_name"), "left")
        .join(df_dim_skill.alias("s"), F.col("f.skill_single") == F.col("s.skill_name"), "left")
        .join(df_dim_work_mode.alias("w"), F.col("f.work_mode") == F.col("w.work_mode"), "left")
        .join(df_dim_category.alias("cat"), F.col("f.derived_category") == F.col("cat.category_name"), "left")
        .withColumn("date_id", F.date_format("f.date_posted", "yyyyMMdd").cast("int"))
        .select(
            F.col("f.job_link"), F.col("f.job_title"), F.col("c.company_id"), 
            F.col("l.location_id"), F.col("s.skill_id"), F.col("w.mode_id"), 
            F.col("cat.category_id"), F.col("date_id"),
            F.lit(1).alias("one_posting")
        )
        .withColumn("fact_id", F.abs(F.xxhash64("job_link", "location_id", "skill_id")).cast("bigint"))
    )
    
    save_to_gold(df_fact_final, "fact_job_posting", partition_col="date_id")
    print(f">>> GOLD Layer Built Successfully. Fact records: {df_fact_final.count()}")

    spark.stop()