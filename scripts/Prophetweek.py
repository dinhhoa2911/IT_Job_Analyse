from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pandas as pd
from prophet import Prophet
import joblib, os, warnings
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

warnings.filterwarnings("ignore")

# --- CONFIGURATION ---
HIVE_METASTORE = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"
CATALOG = "iceberg"
DATABASE_GOLD = "gold"

# Input Tables
TABLE_FACT = "fact_job_posting"
TABLE_DATE = "dim_date"
TABLE_CATEGORY = "dim_job_category"

# Output Tables (ĐỔI TÊN ĐỂ KHÔNG ĐÈ LÊN BẢNG MONTHLY)
TABLE_FORECAST = "ml_forecast_jobs_weekly"      # <--- Tên bảng mới cho tuần
TABLE_EVALUATION = "ml_model_evaluation_weekly" # <--- Tên bảng đánh giá mới cho tuần

MODEL_OUTPUT_DIR = "/opt/spark/models/prophet_jobs_weekly" # Folder model riêng biệt

os.makedirs(MODEL_OUTPUT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("ML_Forecast_Weekly_Parallel_Storage")
    .config("hive.metastore.uris", HIVE_METASTORE)
    .enableHiveSupport()
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG}.uri", HIVE_METASTORE)
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE_PATH)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

print(" Loading data from Gold Layer...")

query = f"""
SELECT
  d.date_posted AS ds,
  c.category_name AS job_category,
  COUNT(f.fact_id) AS total_jobs
FROM {CATALOG}.{DATABASE_GOLD}.{TABLE_FACT} f
JOIN {CATALOG}.{DATABASE_GOLD}.{TABLE_DATE} d ON f.date_id = d.date_id
JOIN {CATALOG}.{DATABASE_GOLD}.{TABLE_CATEGORY} c ON f.category_id = c.category_id
GROUP BY d.date_posted, c.category_name
ORDER BY d.date_posted
"""

df = spark.sql(query).toPandas()
print(f" Raw data loaded: {len(df)} rows")

# --- DATA PREPARATION ---
df['ds'] = pd.to_datetime(df['ds'])
df['y'] = df['total_jobs']
df = df.sort_values(['job_category', 'ds'])

# Weekly Resample Logic
df_weekly = (
    df.set_index("ds")
      .groupby("job_category")["y"]
      .resample("W")
      .interpolate("linear") 
      .reset_index()
)

df = df_weekly.copy()
print(" Weekly datapoints generated successfully.")

# --- FORECASTING LOOP ---
forecast_all = []
evaluation_results = []

job_categories = df['job_category'].unique().tolist()
print(f" Total job categories to forecast: {len(job_categories)}")

for job in job_categories:
    data = df[df['job_category'] == job][['ds', 'y']].sort_values('ds')
    data = data.dropna(subset=['ds', 'y'])

    n = len(data)
    if n < 3:
        print(f" Skipping {job} (insufficient data: {n} points)")
        continue

    # Logic: Nếu dữ liệu ít thì dùng full để train, nhiều thì split test
    if n < 6:
        print(f" Using full dataset for {job} ({n} points)")
        model = Prophet(yearly_seasonality=False, weekly_seasonality=True)
        model.fit(data)
        
        evaluation_results.append({
            "job_category": job,
            "mape": None, 
            "data_points": n
        })

    else:
        split_idx = int(n * 0.8)
        train = data.iloc[:split_idx]
        test = data.iloc[split_idx:]

        model = Prophet(yearly_seasonality=False, weekly_seasonality=True)
        model.fit(train)

        forecast_test = model.predict(test[['ds']])
        
        # Chặn số âm
        forecast_test['yhat'] = forecast_test['yhat'].clip(lower=0)
        
        y_true = test['y'].values
        y_pred = forecast_test['yhat'].values

        mape = None
        try:
            with np.errstate(divide='ignore', invalid='ignore'):
                perc_err = np.abs((y_true - y_pred) / y_true)
                mask = ~np.isnan(perc_err) & ~np.isinf(perc_err)
                if mask.sum() > 0:
                    mape = float(np.mean(perc_err[mask]) * 100)
        except Exception as e:
            print(f" Metrics error for {job}: {e}")

        evaluation_results.append({
            "job_category": job,
            "mape": round(mape, 2) if mape is not None else None,
            "data_points": n
        })
        
        # Train lại full data trước khi dự báo tương lai
        model = Prophet(yearly_seasonality=False, weekly_seasonality=True)
        model.fit(data)

    # Dự báo 12 tuần tới
    future = model.make_future_dataframe(periods=12, freq='W') 
    forecast = model.predict(future)
    
    # CHẶN SỐ ÂM
    forecast['yhat'] = forecast['yhat'].clip(lower=0)
    forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
    forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)

    forecast['job_category'] = job
    forecast_all.append(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'job_category']])

    # Save Model
    joblib.dump(model, os.path.join(MODEL_OUTPUT_DIR, f"prophet_{job.replace('/', '_')}.pkl"))

if not forecast_all:
    print(" No categories forecasted.")
    spark.stop()
    exit()

# --- SAVE FORECAST RESULTS ---
df_forecast = pd.concat(forecast_all, ignore_index=True)
df_forecast.rename(columns={'ds': 'forecast_date', 'yhat': 'predicted_jobs', 
                            'yhat_lower': 'lower_bound', 'yhat_upper': 'upper_bound'}, inplace=True)

# Thêm cột granularity để phân biệt
df_forecast['granularity'] = 'weekly'

spark_forecast = spark.createDataFrame(df_forecast)
spark_forecast = (
    spark_forecast
    .withColumn("created_at", F.current_timestamp())
    .withColumn("predicted_jobs", F.col("predicted_jobs").cast(DoubleType()))
    .withColumn("lower_bound", F.col("lower_bound").cast(DoubleType()))
    .withColumn("upper_bound", F.col("upper_bound").cast(DoubleType()))
    .withColumn("granularity", F.col("granularity").cast(StringType()))
)

# Ghi vào bảng WEEKLY riêng
print(f" Writing forecast to: {TABLE_FORECAST}")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{DATABASE_GOLD}.{TABLE_FORECAST}")

(
    spark_forecast.writeTo(f"{CATALOG}.{DATABASE_GOLD}.{TABLE_FORECAST}")
    .using("iceberg")
    .createOrReplace()
)
print(f" Forecast saved successfully to {TABLE_FORECAST}")

# --- SAVE EVALUATION METRICS ---
df_eval = pd.DataFrame(evaluation_results)
df_eval['granularity'] = 'weekly' # Đánh dấu loại model

spark_eval = spark.createDataFrame(df_eval)

spark_eval = (
    spark_eval
    .withColumn("evaluated_at", F.current_timestamp())
    .withColumn("mape", F.col("mape").cast(DoubleType()))    
    .withColumn("data_points", F.col("data_points").cast(IntegerType()))
    .withColumn("granularity", F.col("granularity").cast(StringType()))
)

print(f" Dropping old table {TABLE_EVALUATION}...")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")

print(f" Writing evaluation to: {TABLE_EVALUATION}")
(
    spark_eval.writeTo(f"{CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")
    .using("iceberg")
    .createOrReplace()
)

print(f" Model evaluation saved to: {CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")
print("\n Done!")