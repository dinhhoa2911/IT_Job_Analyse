from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql import functions as F
import pandas as pd
from prophet import Prophet
import joblib, os, warnings
import numpy as np

# ============================================
# CONFIGURATION
# ============================================
warnings.filterwarnings("ignore")

HIVE_METASTORE = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"
CATALOG = "iceberg"
DATABASE_GOLD = "gold"
TABLE_FACT = "fact_job_posting"
TABLE_DATE = "dim_date"
TABLE_CATEGORY = "dim_job_category"
TABLE_FORECAST = "ml_forecast_jobs"
TABLE_EVALUATION = "ml_model_evaluation"
MODEL_OUTPUT_DIR = "/opt/spark/models/prophet_jobs"

os.makedirs(MODEL_OUTPUT_DIR, exist_ok=True)

# ============================================
# SPARK SESSION
# ============================================
spark = (
    SparkSession.builder
    .appName("ML_Forecast_No_Plot_Superset_Ready")
    .config("hive.metastore.uris", HIVE_METASTORE)
    .enableHiveSupport()
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG}.uri", HIVE_METASTORE)
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# ============================================
# LOAD DATA
# ============================================
print(" Loading data from Gold Layer...")
query = f"""
SELECT
  d.year, d.month,
  c.category_name AS job_category,
  COUNT(f.fact_id) AS total_jobs
FROM {CATALOG}.{DATABASE_GOLD}.{TABLE_FACT} f
JOIN {CATALOG}.{DATABASE_GOLD}.{TABLE_DATE} d ON f.date_id = d.date_id
JOIN {CATALOG}.{DATABASE_GOLD}.{TABLE_CATEGORY} c ON f.category_id = c.category_id
GROUP BY d.year, d.month, c.category_name
ORDER BY d.year, d.month
"""
df = spark.sql(query).toPandas()

# ============================================
# PREPARATION & LOOP
# ============================================
df['ds'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
df['y'] = df['total_jobs']
df = df.sort_values('ds')

forecast_all = []
evaluation_results = []
job_categories = df['job_category'].unique().tolist()

print(f" Processing {len(job_categories)} job categories...")

for job in job_categories:
    data = df[df['job_category'] == job][['ds', 'y']]
    if len(data) < 3: continue

    # Split
    split_idx = int(len(data) * 0.8)
    train = data.iloc[:split_idx]
    test = data.iloc[split_idx:]

    # Train
    model = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
    model.fit(train)

    # Eval (MAPE Only)
    forecast_test = model.predict(test[['ds']])
    y_true = test['y'].values
    y_pred = forecast_test['yhat'].values
    
    mask = y_true != 0
    if np.sum(mask) > 0:
        mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    else:
        mape = 0.0

    evaluation_results.append({
        "job_category": job,
        "MAPE(%)": round(mape, 2),
        "DataPoints": len(data)
    })

    # Forecast Future
    future = model.make_future_dataframe(periods=3, freq='M')
    forecast = model.predict(future)
    forecast['job_category'] = job
    forecast_all.append(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'job_category']])

    # Save Model (Optional)
    model_path = os.path.join(MODEL_OUTPUT_DIR, f"prophet_{job.replace('/', '_')}.pkl")
    joblib.dump(model, model_path)



# ============================================
# SAVE MODEL EVALUATION METRICS (FIXED)
# ============================================
df_eval = pd.DataFrame(evaluation_results)
spark_eval = spark.createDataFrame(df_eval)

spark_eval = (
    spark_eval
    .withColumn("evaluated_at", F.current_timestamp())
    .withColumn("DataPoints", F.col("DataPoints").cast(IntegerType())) 
    .withColumn("MAPE(%)", F.col("MAPE(%)").cast(DoubleType()))        
    .withColumn("job_category", F.col("job_category").cast(StringType()))
)

try:
    print(f" Dropping old table {CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION} to fix schema...")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")
except Exception as e:
    print(f" Warning: Could not drop table (might not exist): {e}")


print(f" Writing evaluation metrics to: {CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")
(
    spark_eval.writeTo(f"{CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")
    .using("iceberg")
    .createOrReplace()
)

print(f" Model evaluation saved successfully!")