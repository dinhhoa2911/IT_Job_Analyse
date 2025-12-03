from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import GaussianMixture
from pyspark.sql.functions import max, count, sum, datediff, lit, col

spark = (
    SparkSession.builder
    .appName("Customer_Clustering")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://tuankiet170-master:9000/data_lake/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.table("hive_catalog.silver.ecommerce_clean")
# build RFM features per customer
rfm = df.groupBy("customer_id").agg(
    max("order_date").alias("last_purchase"),
    count("*").alias("frequency"),
    sum("total_price").alias("monetary")
)
# For simplicity, compute recency in days relative to max date
max_date = df.agg(max("order_date")).collect()[0][0]
rfm = rfm.withColumn("recency", datediff(lit(max_date), col("last_purchase"))).select("customer_id","recency","frequency","monetary")

# Assemble features
assembler = VectorAssembler(inputCols=["recency","frequency","monetary"], outputCol="features_vec")
vec_df = assembler.transform(rfm).select("customer_id","features_vec")

# scale
scaler = StandardScaler(inputCol="features_vec", outputCol="scaled_features", withStd=True, withMean=True)
scaler_model = scaler.fit(vec_df)
scaled = scaler_model.transform(vec_df).select("customer_id","scaled_features")

# GaussianMixture
gmm = GaussianMixture(featuresCol="scaled_features", k=4, predictionCol="cluster", tol=1e-3)
model = gmm.fit(scaled)

pred = model.transform(scaled).select("customer_id","cluster")

# write back to Iceberg Gold customer_cluster
pred.writeTo("hive_catalog.gold.customer_cluster").using("iceberg").createOrReplace()

spark.stop()
print("Saved clusters to hive_catalog.gold.customer_cluster")
