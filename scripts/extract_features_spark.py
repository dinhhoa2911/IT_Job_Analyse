from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, expr
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Extract_Features")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://tuankiet170-master:9000/data_lake/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

# Load dữ liệu từ Iceberg Silver table và tạo namespace nếu chưa tồn tại
df_silver = spark.table("hive_catalog.silver.ecommerce_clean")

# basic time features
df = df_silver.withColumn("year", year(col("order_date"))) \
       .withColumn("month", month(col("order_date"))) \
       .withColumn("day", dayofmonth(col("order_date"))) \
       .withColumn("dow", dayofweek(col("order_date")))

# product-level stats (agg)
prod_stats = df.groupBy("product_id") \
    .agg(
        F.sum("quantity").alias("prod_total_qty"),
        F.avg("price").alias("prod_avg_price"),
        F.countDistinct("customer_id").alias("prod_unique_buyers")
    )

# customer-level RFM basic
customer_rfm = df.groupBy("customer_id") \
    .agg(
        F.max("order_date").alias("last_purchase"),
        F.countDistinct("order_date").alias("freq_days"),
        F.sum("total_price").alias("monetary")
    )

# join features
df_feat = df.join(prod_stats, on="product_id", how="left")

# aggregate per order line (one row per transaction line)
# choose label = quantity (or total_price)
df_feat = df_feat.select(
    "customer_id","product_id","order_date","year","month","day","dow",
    "quantity","price","total_price","review_score","payment_method","city","gender","age",
    "prod_total_qty","prod_avg_price","prod_unique_buyers"
)

# optional: cast categorical to string upper-case
df_feat = df_feat.withColumn("payment_method", F.upper(F.trim(col("payment_method"))))

# write a parquet sample (for speed, sample 10%)
sample = df_feat.sample(0.10, seed=42)

sample.writeTo("hive_catalog.gold.features_sample").using("iceberg").createOrReplace()
sample.coalesce(1).write.mode("overwrite").csv("hdfs://tuankiet170-master:9000/data_lake/maximize_revenue_csv/features_sample.csv")

spark.stop()
print("Features were saved to hive_catalog- Gold Layer")
