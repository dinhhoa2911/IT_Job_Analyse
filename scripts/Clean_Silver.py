from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================================================
# 1. Cấu hình hệ thống & Dictionaries
# ===========================================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
WAREHOUSE_PATH = "s3a://warehouse/iceberg_data"
HIVE_METASTORE = "thrift://hive-metastore:9083"

CATALOG_NAME = "iceberg"
DATABASE_BRONZE = "bronze"
TABLE_BRONZE = "it_jobs_raw"
DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"

FULL_SILVER_TABLE = f"{CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}"
FULL_BRONZE_TABLE = f"{CATALOG_NAME}.{DATABASE_BRONZE}.{TABLE_BRONZE}"

SKILL_MAPPING = {
    # PYTHON
    "PYTHON3": "PYTHON", "PYTHON2": "PYTHON", "PYTHON DEV": "PYTHON", "PYTHON DEVELOPER": "PYTHON",
    "DJANGO": "PYTHON", "FLASK": "PYTHON", "FASTAPI": "PYTHON", "PYSPARK": "SPARK",
    # JAVA
    "JAVA8": "JAVA", "JAVA11": "JAVA", "JAVA17": "JAVA", "SPRING": "JAVA", "SPRINGBOOT": "JAVA", 
    "SPRING BOOT": "JAVA", "J2EE": "JAVA", "JSP": "JAVA",
    # GO
    "GOLANG": "GO", "GO LANG": "GO",
    # NODEJS
    "NODEJS": "NODE.JS", "NODE JS": "NODE.JS", "NODE": "NODE.JS", "EXPRESSJS": "NODE.JS", "EXPRESS": "NODE.JS",
    # .NET
    "C#": ".NET", "ASP.NET": ".NET", ".NET CORE": ".NET", "DOTNET": ".NET", "VB.NET": ".NET",
    # PHP
    "PHP7": "PHP", "PHP8": "PHP", "LARAVEL": "PHP", "CODEIGNITER": "PHP",
    # RUBY
    "RUBY ON RAILS": "RUBY", "ROR": "RUBY",
    # FRONTEND
    "REACTJS": "REACT", "REACT.JS": "REACT", "NEXTJS": "NEXT.JS", "NEXT": "NEXT.JS", "VUEJS": "VUE", 
    "VUE.JS": "VUE", "ANGULARJS": "ANGULAR", "JS": "JAVASCRIPT", "ES6": "JAVASCRIPT", "TS": "TYPESCRIPT", 
    "HTML5": "HTML", "CSS3": "CSS", "TAILWINDCSS": "TAILWIND CSS", "BOOTSTRAP5": "BOOTSTRAP",
    # MOBILE
    "ANDROID SDK": "ANDROID", "ANDROID STUDIO": "ANDROID", "IOS DEV": "IOS", "SWIFTUI": "SWIFT", 
    "REACT NATIVE": "REACT_NATIVE", "FLUTTER DEV": "FLUTTER",
    # DATABASE
    "POSTGRES": "POSTGRESQL", "POSTGRES DB": "POSTGRESQL", "MYSQL DB": "MYSQL", "SQLSERVER": "SQL SERVER", 
    "MSSQL": "SQL SERVER", "NOSQL": "MONGODB", "REDIS CACHE": "REDIS",
    # CLOUD / DEVOPS
    "AMAZON WEB SERVICES": "AWS", "AWS CLOUD": "AWS", "GOOGLE CLOUD PLATFORM": "GOOGLE CLOUD", "GCP": "GOOGLE CLOUD", 
    "AZURE CLOUD": "AZURE", "K8S": "KUBERNETES", "KUBE": "KUBERNETES", "DOCKER CONTAINER": "DOCKER", 
    "JENKINS PIPELINE": "JENKINS", "CI/CD": "CI_CD",
    # DATA ENGINEERING
    "APACHE SPARK": "SPARK", "HADOOP ECOSYSTEM": "HADOOP", "KAFKA STREAMING": "KAFKA", 
    "AIRFLOW DAG": "AIRFLOW", "SNOWFLAKE DB": "SNOWFLAKE",
    # AI / ML
    "MACHINE LEARNING": "ML", "DEEP LEARNING": "DL", "LLM": "GENERATIVE AI", "GENAI": "GENERATIVE AI", 
    "TENSORFLOW2": "TENSORFLOW", "PYTORCH LIGHTNING": "PYTORCH",
    # TESTING
    "AUTOMATION TESTING": "AUTOMATION TEST", "MANUAL TESTING": "MANUAL TEST", "SELENIUM TEST": "SELENIUM",
    # OTHER
    "RESTFUL API": "REST API", "RESTFULAPIS": "REST API", "MICROSERVICES": "MICROSERVICE", "OOP": "OBJECT ORIENTED PROGRAMMING"
}

SKILL_GROUP_MAPPING = {
    "PYTHON": "Backend", "JAVA": "Backend", "GO": "Backend", "PHP": "Backend", "RUBY": "Backend", 
    "NODE.JS": "Backend", ".NET": "Backend",
    "REACT": "Frontend", "NEXT.JS": "Frontend", "ANGULAR": "Frontend", "VUE": "Frontend", "HTML": "Frontend", 
    "CSS": "Frontend", "JAVASCRIPT": "Frontend", "TYPESCRIPT": "Frontend", "TAILWIND CSS": "Frontend", "BOOTSTRAP": "Frontend",
    "ANDROID": "Mobile", "IOS": "Mobile", "SWIFT": "Mobile", "FLUTTER": "Mobile", "REACT_NATIVE": "Mobile",
    "MYSQL": "Database", "POSTGRESQL": "Database", "SQL SERVER": "Database", "MONGODB": "Database", 
    "REDIS": "Database", "SNOWFLAKE": "Database",
    "AWS": "Cloud & DevOps", "AZURE": "Cloud & DevOps", "GOOGLE CLOUD": "Cloud & DevOps", "DOCKER": "Cloud & DevOps", 
    "KUBERNETES": "Cloud & DevOps", "JENKINS": "Cloud & DevOps", "CI_CD": "Cloud & DevOps",
    "SPARK": "Data Engineering", "HADOOP": "Data Engineering", "KAFKA": "Data Engineering", "AIRFLOW": "Data Engineering",
    "ML": "AI & ML", "DL": "AI & ML", "TENSORFLOW": "AI & ML", "PYTORCH": "AI & ML", "GENERATIVE AI": "AI & ML",
    "SELENIUM": "Testing", "AUTOMATION TEST": "Testing", "MANUAL TEST": "Testing",
    "REST API": "Software Engineering", "MICROSERVICE": "Software Engineering", "OBJECT ORIENTED PROGRAMMING": "Software Engineering"
}

# ===========================================================
# 2. Các hàm tiện ích (Helpers)
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

def get_watermark(spark):
    if spark.catalog.tableExists(FULL_SILVER_TABLE):
        try:
            result = spark.sql(f"SELECT MAX(ingest_time) FROM {FULL_SILVER_TABLE}").collect()
            if result and result[0][0]:
                return result[0][0]
        except Exception as e:
            print(f"Warning: Could not get watermark ({e})")
    return None

def clean_and_transform(df_new_records):
    mapping_expr = F.create_map([F.lit(x) for kv in SKILL_MAPPING.items() for x in kv])
    group_expr = F.create_map([F.lit(x) for kv in SKILL_GROUP_MAPPING.items() for x in kv])

    return (
        df_new_records
        .toDF(*[c.strip().lower().replace(" ", "_") for c in df_new_records.columns])
        # Basic Cleaning & Null Filter
        .withColumn("job_title", F.trim(F.col("job_title")))
        .withColumn("company_name", F.upper(F.trim(F.col("company_name"))))
        .filter(
            (F.col("job_title").isNotNull()) & (F.col("job_title") != "") &
            (F.col("company_name").isNotNull()) & (F.col("company_name") != "") &
            (F.col("job_link").isNotNull()) & (F.col("job_link") != "")
        )
        # Work Mode
        .withColumn("work_mode", F.trim(F.lower(F.col("work_mode"))))
        .withColumn(
            "work_mode",
            F.when(F.col("work_mode").isNull() | (F.col("work_mode") == ""), "At Office")
             .when(F.col("work_mode").rlike("(?i)office|onsite"), "At Office")
             .when(F.col("work_mode").rlike("(?i)hybrid"), "Hybrid")
             .when(F.col("work_mode").rlike("(?i)remote|wfh"), "Remote")
             .otherwise("At Office")
        )
        # Location
        .withColumn("location", F.coalesce(F.col("location"), F.lit("Unknown")))
        .withColumn("location_array", F.split(F.col("location"), " - "))
        .withColumn("location", F.expr("""
            transform(location_array, x ->
                CASE
                    WHEN lower(trim(x)) LIKE '%hcm%' OR lower(trim(x)) LIKE '%hochiminh%' THEN 'Ho Chi Minh'
                    WHEN lower(trim(x)) LIKE '%ha noi%' OR lower(trim(x)) LIKE '%hanoi%' THEN 'Ha Noi'
                    WHEN lower(trim(x)) LIKE '%da nang%' THEN 'Da Nang'
                    ELSE trim(x)
                END
            )
        """)).drop("location_array")
        # Skills Cleaning
        .withColumn("skills_required", F.regexp_replace(F.col("skills_required"), r"[\[\]'\"()]", ""))
        .withColumn("skills_required", F.coalesce(F.col("skills_required"), F.lit("")))
        .withColumn("skills_required", F.split(F.col("skills_required"), ","))
        .withColumn("skills_required", F.expr("transform(skills_required, x -> upper(trim(x)))"))
        .withColumn("skills_required", F.expr("transform(skills_required, x -> regexp_replace(x, '[^A-Z0-9#.+ ]', ''))"))
        .withColumn("skills_required", F.transform("skills_required", lambda x: F.coalesce(mapping_expr[x], x)))
        .withColumn("skills_required", F.expr("filter(skills_required, x -> x is not null and x != '' and length(x) > 1)"))
        .withColumn("skills_required", F.array_distinct(F.col("skills_required")))
        .filter(F.size("skills_required") <= 20)
        # Skill Groups
        .withColumn("skill_groups", F.transform("skills_required", lambda x: F.coalesce(group_expr[x], F.lit("Other"))))
        .withColumn("skill_groups", F.array_distinct(F.col("skill_groups")))
    )

# ===========================================================
# 3. Main Execution
# ===========================================================
if __name__ == "__main__":
    spark = create_spark_session("Silver_Etl_Cleaning_Merge")
    
    last_processed_time = get_watermark(spark)
    print(f">>> Watermark: {last_processed_time}")

    df_bronze = spark.table(FULL_BRONZE_TABLE)

    if last_processed_time:
        print(">>> INCREMENTAL LOAD")
        df_new_records = df_bronze.filter(F.col("ingest_time") > last_processed_time)
    else:
        print(">>> FULL LOAD")
        df_new_records = df_bronze

    count_new = df_new_records.count()
    print(f">>> Processing {count_new} records")

    if count_new > 0:
        df_clean = clean_and_transform(df_new_records)
        
        print(">>> Writing cleaned data to Silver...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_SILVER}")

        if not spark.catalog.tableExists(FULL_SILVER_TABLE):
            print(f">>> Creating table {FULL_SILVER_TABLE}")
            df_clean.writeTo(FULL_SILVER_TABLE).using("iceberg").tableProperty("format-version", "2").create()
            print(">>> SUCCESS: Table Created")
        else:
            print(">>> MERGE INTO Silver")
            df_clean.createOrReplaceTempView("source_data")
            spark.sql(f"""
                MERGE INTO {FULL_SILVER_TABLE} AS target
                USING source_data AS source
                ON target.job_link = source.job_link
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(">>> SUCCESS: Data Merged")
    else:
        print(">>> No new data")

    spark.stop()