import os
import sys
import joblib
import pandas as pd
import numpy as np
import lightgbm as lgb
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from lightgbm import LGBMRegressor
from hdfs import InsecureClient

spark = (
    SparkSession.builder
    .appName("Train_LightGBM")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://tuankiet170-master:9000/data_lake/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

df_spark = spark.table("hive_catalog.gold.features_sample")
# Detect Large dataset to avoid OOM
count = df_spark.count()
print(f"Features rows count = {count}")
MAX_ROWS_FOR_DRIVER = 1000000
if count > MAX_ROWS_FOR_DRIVER:
    print("Large dataset detected, taking sample 10% to avoid driver OOM.")
    df_spark = df_spark.sample(0.10, seed=42)

print("Converting to pandas DataFrame")
pdf = df_spark.toPandas()
spark.stop()
print("Converted to pandas. Shape:", pdf.shape)

# Basic feature selection
# Convert categorical
for c in ["payment_method", "city", "gender"]:
    if c in pdf.columns:
        pdf[c] = pdf[c].fillna("NA").astype(str)
        pdf[c] = pd.factorize(pdf[c])[0]
    else:
        pdf[c] = 0

# fillna numeric
pdf['review_score'] = pdf.get('review_score', pd.Series([0]*len(pdf))).fillna(0)
pdf['age'] = pdf.get('age', pd.Series([np.nan]*len(pdf))).fillna(pdf['age'].median())

# # factorize and save mapping
# import joblib, os
# codes, uniques = pd.factorize(pdf['payment_method'])
# pdf['payment_method_enc'] = codes.astype(int)

# # save mapping dict: category -> code
# payment_map = {cat: int(i) for i, cat in enumerate(uniques)}
# encoders_local = "/tmp/enc_payment_method.pkl"
# joblib.dump(payment_map, encoders_local)

# # upload to HDFS
# from hdfs import InsecureClient
# client = InsecureClient('http://tuankiet170-master:9870', user='root')
# client.makedirs('/models', permission='755')
# client.upload('/models/enc_payment_method.pkl', encoders_local, overwrite=True)
# print("Saved encoder to HDFS /models/enc_payment_method.pkl")

# features and label (check columns exist)
features = [
    "price","prod_avg_price","prod_total_qty","prod_unique_buyers",
    "year","month","dow","review_score","age","payment_method"
]
# keep only columns present
features = [f for f in features if f in pdf.columns]
if "quantity" not in pdf.columns:
    print("ERROR: label column 'quantity' not found. Exiting.")
    sys.exit(1)

X = pdf[features].astype(float)
y = pdf["quantity"].astype(float)

print("Feature cols:", features)
print("Train/test split ...")
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.15, random_state=42)

# Train LGBM using sklearn API
print("Training LGBMRegressor ...")
model = LGBMRegressor(
    objective='regression',
    n_estimators=1000,
    learning_rate=0.05,
    num_leaves=31,
    random_state=42
)

# Stop training when metric on validation not improve
callbacks = [
    lgb.early_stopping(stopping_rounds=30),
    lgb.log_evaluation(period=20)
]

# sklearn fit
model.fit(
    X_train, y_train,
    eval_set=[(X_val, y_val)],
    eval_metric='rmse',
    callbacks=callbacks
)

# Save model locally
local_model_path = "/tmp/lgbm_demand_model.pkl"
joblib.dump(model, local_model_path)
print("Saved model to", local_model_path)

# Upload model to HDFS
hdfs_target = "/models/lgbm_demand_model.pkl"

# print("Trying to upload model to HDFS via pyarrow...")
# try:
#     try:
#         hdfs = fs.HadoopFileSystem(host='tuankiet170-master', port=9000, user=os.environ.get('USER','hadooptuankiet170'))
#     except TypeError:
#         # alternative constructor signature in some pyarrow versions
#         hdfs = fs.HadoopFileSystem.from_uri("hdfs://tuankiet170-master:9000")
#     # write bytes
#     with open(local_model_path, "rb") as fh:
#         data = fh.read()
#     with hdfs.open_output_stream(hdfs_target) as out:
#         out.write(data)
#     print("Uploaded model to HDFS:", hdfs_target)
# except Exception as e:
#     print("pyarrow HDFS upload failed:", e)

print("Trying to upload model to HDFS via hdfs (Python Library)...")
try:
    client = InsecureClient('http://tuankiet170-master:9870', user='root')
    hdfs_dir = '/models'
    try:
        client.status(hdfs_dir)
        print(f"Directory {hdfs_dir} already exists.")
    except Exception:
        print(f"Directory {hdfs_dir} not found, creating...")
        client.makedirs(hdfs_dir, permission='755')

    with open(local_model_path, 'rb') as f:
        client.write('/models/lgbm_demand_model.pkl', f, overwrite=True)
    print("Model uploaded successfully to HDFS via WebHDFS.")
except Exception as e:
    print("WebHDFS upload failed:", e)


