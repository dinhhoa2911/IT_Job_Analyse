import os
import joblib
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from hdfs import InsecureClient
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Price_Optimization")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://tuankiet170-master:9000/data_lake/warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .enableHiveSupport()
    .getOrCreate()
)

hdfs_web = 'http://tuankiet170-master:9870'
hdfs_model_path = '/models/lgbm_demand_model.pkl'
hdfs_enc_path = '/models/enc_payment_method.pkl'
local_model = '/tmp/lgbm_demand_model.pkl'
local_enc = '/tmp/enc_payment_method.pkl'

client = InsecureClient(hdfs_web, user='root')

# Download model + encoder from HDFS
try:
    with client.read(hdfs_model_path) as r, open(local_model, 'wb') as f:
        f.write(r.read())
    print("Downloaded model to", local_model)
except Exception as e:
    raise RuntimeError("Failed to download model from HDFS: " + str(e))

# try:
#     with client.read(hdfs_enc_path) as r, open(local_enc, 'wb') as f:
#         f.write(r.read())
#     print("Downloaded encoder to", local_enc)
# except Exception as e:
#     print("Warning: encoder not found on HDFS:", e)
#     local_enc = None

# Load model + encoder
model = joblib.load(local_model)
print("Model loaded.")

# payment_map = None
# if local_enc:
#     try:
#         payment_map = joblib.load(local_enc)
#         print("Payment encoder loaded. #categories:", len(payment_map))
#     except Exception as e:
#         print("Failed to load encoder:", e)
#         payment_map = None

# Load data from hive_catalog
spark_df = spark.table("hive_catalog.gold.features_sample")

# Aggregate: pick representative features (first) and collect list of all prices per product
agg_exprs = [
    F.first("prod_avg_price").alias("prod_avg_price"),
    F.first("prod_total_qty").alias("prod_total_qty"),
    F.first("prod_unique_buyers").alias("prod_unique_buyers"),
    F.first("year").alias("year"),
    F.first("month").alias("month"),
    F.first("dow").alias("dow"),
    F.first("review_score").alias("review_score"),
    F.first("age").alias("age"),
    F.first("payment_method").alias("payment_method"),
    F.first("price").alias("price")
]

prod_df_spark = spark_df.groupBy("product_id").agg(*agg_exprs)

# Limit number of products
# prod_df_spark = prod_df_spark.limit(200)

# Convert to pandas
prods_pdf = prod_df_spark.toPandas()
print("Loaded prods_pdf shape:", prods_pdf.shape)

# # PREPROCESSING: apply the SAME encoding used for training
# if 'payment_method' in prods_pdf.columns:
#     prods_pdf['payment_method'] = prods_pdf['payment_method'].fillna('NA').astype(str)
#     if payment_map:
#         # map known categories; unknown -> a special code (e.g., -1 or len(payment_map))
#         unknown_code = max(payment_map.values()) + 1 if payment_map else -1
#         prods_pdf['payment_method_enc'] = prods_pdf['payment_method'].map(payment_map).fillna(unknown_code).astype(int)
#     else:
#         # fallback: factorize on the fly (NOT RECOMMENDED for production)
#         prods_pdf['payment_method_enc'], uniques = pd.factorize(prods_pdf['payment_method'])
#         print("Warning: used ad-hoc factorize for payment_method (not same as training)")

# Ensure columns exist
for col in ["payment_method","review_score","age","prod_avg_price","prod_total_qty","prod_unique_buyers","year","month","dow","price"]:
    if col not in prods_pdf.columns:
        prods_pdf[col] = 0

# fillna review_score and age
prods_pdf['review_score'] = prods_pdf['review_score'].fillna(0)
if prods_pdf['age'].isnull().all():
    prods_pdf['age'] = prods_pdf['age'].fillna(0)
else:
    prods_pdf['age'] = prods_pdf['age'].fillna(prods_pdf['age'].median())

# payment_method encode via pd.factorize
prods_pdf['payment_method'] = prods_pdf['payment_method'].fillna('NA').astype(str)
prods_pdf['payment_method_enc'], payment_uniques = pd.factorize(prods_pdf['payment_method'])

# Prepare features for model (ensure same order as training)
features_order = ["price","prod_avg_price","prod_total_qty","prod_unique_buyers",
                  "year","month","dow","review_score","age","payment_method_enc"]

# Ensure columns exist
for f in features_order:
    if f not in prods_pdf.columns:
        prods_pdf[f] = 0

# Grid search per product
results = []
for _, row in prods_pdf.iterrows():
    pid = int(row["product_id"])
    base = float(row["prod_avg_price"]) if row["prod_avg_price"] else float(row.get("price", 0.0))
    if base <= 0:
        continue
    grid = np.linspace(base*0.7, base*1.3, 15)
    best_price = base
    best_rev = -1.0
    # build static feature values from representative row to reuse
    static_vals = {f: 0.0 for f in features_order}
    for f in features_order:
        if f == 'price':
            continue
        static_vals[f] = float(row.get(f, 0.0)) if pd.notna(row.get(f, 0.0)) else 0.0

    for p in grid:
        # build input vector in correct order
        X_list = []
        for f in features_order:
            if f == 'price':
                X_list.append(float(p))
            else:
                X_list.append(static_vals[f])
        X_arr = np.array(X_list, dtype=float).reshape(1, -1)
        pred_qty = model.predict(X_arr)[0]
        rev = p * max(pred_qty, 0.0)
        if rev > best_rev:
            best_rev = rev
            best_price = p

    results.append({"product_id": pid, "best_price": float(best_price), "estimated_revenue": float(best_rev)})

res_df = pd.DataFrame(results)
print("Computed best prices for", len(res_df), "products")

# Write results to Iceberg Gold
spark_res_df = spark.createDataFrame(res_df)
spark_res_df.writeTo("hive_catalog.gold.price_recommendations").using("iceberg").createOrReplace()
print("Wrote hive_catalog.gold.price_recommendations")

# optional: also upload CSV artifact to HDFS
csv_local = "/tmp/price_recommendations.csv"
res_df.to_csv(csv_local, index=False)
# try:
#     client.upload("/models/price_recommendations.csv", csv_local, overwrite=True)
#     print("Uploaded CSV to HDFS /models/price_recommendations.csv")
# except Exception as e:
#     print("Warning: cannot upload CSV to HDFS:", e)

spark.stop()
