from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import pandas as pd
from sentence_transformers import SentenceTransformer
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

# ===========================================================
# 1. Cấu hình hệ thống (Phiên bản & Tham số)
# ===========================================================
# Lưu ý: Kiểm tra phiên bản Spark của bạn (spark-submit --version)
# Nếu là Spark 3.4 hoặc 3.5, các phiên bản dưới đây là phù hợp nhất:
HADOOP_AWS_VERSION = "3.3.4" 
ICEBERG_VERSION = "1.5.0"
SPARK_VERSION = "3.5_2.12" 

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
WAREHOUSE_PATH = "s3a://warehouse/iceberg_data"
HIVE_METASTORE = "thrift://hive-metastore:9083"

CATALOG_NAME = "iceberg"
DATABASE_SILVER = "silver"
TABLE_SILVER = "it_jobs_clean"

# Cấu hình Milvus
MILVUS_HOST = "milvus-standalone" 
MILVUS_PORT = "19530"
COLLECTION_NAME = "it_jobs_rag"
EMBEDDING_DIM = 384 # 'all-MiniLM-L6-v2' outputs 384 dimensions

# ===========================================================
# 2. Khởi tạo Spark Session (Sửa lỗi ClassNotFoundException)
# ===========================================================
print(">>> Initializing Spark Session with S3A and Iceberg support...")

spark = (
    SparkSession.builder
    .appName("Vectorize_To_Milvus")
    # TỰ ĐỘNG TẢI JAR: Đây là phần quan trọng nhất để sửa lỗi của bạn
    .config("spark.jars.packages", 
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION},"
            f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}:{ICEBERG_VERSION}")
    
    # Cấu hình kết nối MinIO
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    # Cấu hình Hive Metastore & Iceberg Catalog
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
# 3. Đọc và Tiền xử lý dữ liệu
# ===========================================================
print(f">>> Reading Data from Silver Layer: {CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}")

# Đọc bảng từ Iceberg
df_silver = spark.table(f"{CATALOG_NAME}.{DATABASE_SILVER}.{TABLE_SILVER}")

df_prepared = (
    df_silver
    # Tạo ID duy nhất cho Milvus (kiểu BigInt)
    .withColumn("job_id", F.abs(F.xxhash64("job_link")).cast("bigint"))
    
    # Xử lý mảng thành chuỗi văn bản
    .withColumn("skills_str", F.array_join(F.col("skills_required"), ", "))
    .withColumn("location_str", F.array_join(F.col("location"), ", "))
    
    # Tổng hợp nội dung để làm Embedding (Context cho RAG)
    .withColumn(
        "text_content",
        F.concat_ws(
            ". ", 
            F.concat(F.lit("Job Title: "), F.col("job_title")),
            F.concat(F.lit("Company: "), F.col("company_name")),
            F.concat(F.lit("Location: "), F.col("location_str")),
            F.concat(F.lit("Work Mode: "), F.col("work_mode")),
            F.concat(F.lit("Skills: "), F.col("skills_str"))
        )
    )
    .select("job_id", "job_link", "job_title", "text_content")
)

# Chuyển về Pandas để xử lý Embedding (phù hợp với tập dữ liệu vừa phải)
print(">>> Converting to Pandas...")
pdf = df_prepared.toPandas()

# ===========================================================
# 4. Tạo Vector Embeddings
# ===========================================================
print(">>> Generating Embeddings using Sentence-Transformers...")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Tạo vector
embeddings = model.encode(pdf['text_content'].tolist(), show_progress_bar=True)
pdf['embedding'] = embeddings.tolist()

# ===========================================================
# 5. Lưu vào Milvus
# ===========================================================
print(f">>> Connecting to Milvus at {MILVUS_HOST}:{MILVUS_PORT}...")
connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)

# Reset collection để tránh trùng lặp khi test
if utility.has_collection(COLLECTION_NAME):
    print(f"Dropping existing collection: {COLLECTION_NAME}")
    utility.drop_collection(COLLECTION_NAME)

# Định nghĩa Schema
fields = [
    FieldSchema(name="job_id", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="job_link", dtype=DataType.VARCHAR, max_length=1000),
    FieldSchema(name="job_title", dtype=DataType.VARCHAR, max_length=500),
    FieldSchema(name="text_content", dtype=DataType.VARCHAR, max_length=4000),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM)
]
schema = CollectionSchema(fields, description="IT Jobs Vector DB")
collection = Collection(name=COLLECTION_NAME, schema=schema)

# Chèn dữ liệu
print(">>> Inserting data into Milvus...")
collection.insert(pdf.to_dict('records')) # Cách insert này sạch hơn bằng list dict

# Tạo Index và Load
print(">>> Creating Index & Loading Collection...")
index_params = {
    "metric_type": "COSINE",
    "index_type": "HNSW",
    "params": {"M": 8, "efConstruction": 64}
}
collection.create_index(field_name="embedding", index_params=index_params)
collection.load()

print(">>> [SUCCESS] Pipeline hoàn tất! Dữ liệu đã sẵn sàng trong Milvus.")
spark.stop()