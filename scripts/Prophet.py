from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from prophet import Prophet
import joblib, os, warnings
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import matplotlib.pyplot as plt

# ============================================
# CONFIGURATION
# ============================================
warnings.filterwarnings("ignore")

HIVE_METASTORE = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "hdfs://dinhhoa-master:9000/user/ndh/warehouse"
CATALOG = "iceberg"        #  Catalog chuẩn
DATABASE_GOLD = "gold"
TABLE_FACT = "fact_job_posting"
TABLE_DATE = "dim_date"
TABLE_CATEGORY = "dim_job_category"
TABLE_FORECAST = "ml_forecast_jobs"
TABLE_EVALUATION = "ml_model_evaluation"
MODEL_OUTPUT_DIR = "/opt/spark/models/prophet_jobs"

os.makedirs(MODEL_OUTPUT_DIR, exist_ok=True)

# ============================================
# SPARK SESSION (Iceberg + Hive)
# ============================================
spark = (
    SparkSession.builder
    .appName("ML_Forecast_IT_Job_Trends_With_Evaluation")
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
# LOAD DATA FROM GOLD LAYER
# ============================================
print(" Loading data from Gold Layer...")

query = f"""
SELECT
  d.year,
  d.month,
  c.category_name AS job_category,
  COUNT(f.fact_id) AS total_jobs
FROM {CATALOG}.{DATABASE_GOLD}.{TABLE_FACT} f
JOIN {CATALOG}.{DATABASE_GOLD}.{TABLE_DATE} d ON f.date_id = d.date_id
JOIN {CATALOG}.{DATABASE_GOLD}.{TABLE_CATEGORY} c ON f.category_id = c.category_id
GROUP BY d.year, d.month, c.category_name
ORDER BY d.year, d.month
"""
df = spark.sql(query).toPandas()
print(f" Raw data loaded: {len(df)} rows")

# ============================================
# DATA PREPARATION
# ============================================
df['ds'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
df['y'] = df['total_jobs']
df = df.sort_values('ds')

# ============================================
# FORECASTING + EVALUATION LOOP
# ============================================
forecast_all = []
evaluation_results = []

job_categories = df['job_category'].unique().tolist()
print(f" Total job categories to forecast: {len(job_categories)}")

for job in job_categories:
    print(f"\n Training Prophet model for job category: {job}")
    data = df[df['job_category'] == job][['ds', 'y']]

    if len(data) < 3:
        print(f" Skipping {job} (insufficient data: {len(data)} points)")
        continue

    # === Split train/test
    split_idx = int(len(data) * 0.8)
    train = data.iloc[:split_idx]
    test = data.iloc[split_idx:]

    # === Train Prophet
    model = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
    model.fit(train)

    # === Predict on test set
    forecast_test = model.predict(test[['ds']])
    forecast_test['y_true'] = test['y'].values

    # === Compute evaluation metrics
    y_true = forecast_test['y_true']
    y_pred = forecast_test['yhat']
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
    r2 = r2_score(y_true, y_pred)

    evaluation_results.append({
        "job_category": job,
        "MAE": round(mae, 2),
        "RMSE": round(rmse, 2),
        "MAPE(%)": round(mape, 2),
        "R2_Score": round(r2, 3),
        "DataPoints": len(data)
    })

    # === Forecast next 3 months
    future = model.make_future_dataframe(periods=3, freq='M')
    forecast = model.predict(future)
    forecast['job_category'] = job
    forecast_all.append(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'job_category']])

    # === Save model
    model_path = os.path.join(MODEL_OUTPUT_DIR, f"prophet_model_{job.replace('/', '_').replace(' ', '_')}.pkl")
    joblib.dump(model, model_path)
    print(f" Saved model: {model_path}")

    # === Save plot
    try:
        model.plot(forecast)
        plt.title(f"Forecast for {job}", fontsize=13)
        plt.xlabel("Date"); plt.ylabel("Job Count")
        plt.tight_layout()
        plt.savefig(os.path.join(MODEL_OUTPUT_DIR, f"forecast_plot_{job}.png"))
        plt.close()
    except Exception:
        pass

# ============================================
# SAVE FORECAST TO ICEBERG
# ============================================
if not forecast_all:
    print(" No categories had enough data.")
    spark.stop()
    exit()

df_forecast = pd.concat(forecast_all, ignore_index=True)
df_forecast.rename(columns={
    'ds': 'forecast_date',
    'yhat': 'predicted_jobs',
    'yhat_lower': 'lower_bound',
    'yhat_upper': 'upper_bound'
}, inplace=True)

spark_forecast = spark.createDataFrame(df_forecast).withColumn("created_at", F.current_timestamp())
(
    spark_forecast.writeTo(f"{CATALOG}.{DATABASE_GOLD}.{TABLE_FORECAST}")
    .using("iceberg")
    .createOrReplace()
)
print(f" Forecast results saved to: {CATALOG}.{DATABASE_GOLD}.{TABLE_FORECAST}")

# ============================================
# SAVE MODEL EVALUATION METRICS
# ============================================
df_eval = pd.DataFrame(evaluation_results)
spark_eval = spark.createDataFrame(df_eval).withColumn("evaluated_at", F.current_timestamp())
(
    spark_eval.writeTo(f"{CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")
    .using("iceberg")
    .createOrReplace()
)
print(f" Model evaluation saved to: {CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")

# ============================================
# DISPLAY EVALUATION SUMMARY
# ============================================
print("\n Model Evaluation Summary:")
print(df_eval)

spark.stop()
print("\n Forecasting + Evaluation completed successfully!")
