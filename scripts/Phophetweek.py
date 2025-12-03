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
  d.date_posted AS ds,
  c.category_name AS job_category,
  COUNT(f.fact_id) AS total_jobs
FROM iceberg.gold.fact_job_posting f
JOIN iceberg.gold.dim_date d ON f.date_id = d.date_id
JOIN iceberg.gold.dim_job_category c ON f.category_id = c.category_id
GROUP BY d.date_posted, c.category_name
ORDER BY d.date_posted;
"""

df = spark.sql(query).toPandas()
print(f" Raw data loaded: {len(df)} rows")

# ============================================
# DATA PREPARATION + WEEKLY RESAMPLING
# ============================================
df['ds'] = pd.to_datetime(df['ds'])
df['y'] = df['total_jobs']
df = df.sort_values(['job_category', 'ds'])

# -------- Weekly Resample ----------
df_weekly = (
    df.set_index("ds")
      .groupby("job_category")["y"]
      .resample("W")
      .interpolate("linear")
      .reset_index()
)

df = df_weekly.copy()
print(" Weekly datapoints generated successfully.")

# ============================================
# FORECASTING + EVALUATION LOOP
# ============================================
forecast_all = []
evaluation_results = []

job_categories = df['job_category'].unique().tolist()
print(f" Total job categories to forecast: {len(job_categories)}")

for job in job_categories:
    print(f"\n Training Prophet model for job category: {job}")

    data = df[df['job_category'] == job][['ds', 'y']].sort_values('ds')
    data = data.dropna(subset=['ds', 'y'])

    n = len(data)
    if n < 3:
        print(f" Skipping {job} (after weekly resampling only {n} valid points)")
        continue

    # ============================
    # CASE A: Very small dataset
    # ============================
    if n < 6:
        print(f" Using full dataset (no train/test split) for {job} with {n} points")

        model_small = Prophet(yearly_seasonality=True)
        model_small.fit(data)

        future = model_small.make_future_dataframe(periods=3, freq='M')
        forecast = model_small.predict(future)
        forecast['job_category'] = job

        forecast_all.append(
            forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'job_category']]
        )

        joblib.dump(
            model_small,
            os.path.join(MODEL_OUTPUT_DIR, f"prophet_model_{job.replace('/', '_').replace(' ', '_')}.pkl")
        )

        evaluation_results.append({
            "job_category": job,
            "MAE": None,
            "RMSE": None,
            "MAPE(%)": None,
            "R2_Score": None,
            "DataPoints": n
        })
        continue

    # ============================
    # CASE B: Sufficient data
    # ============================
    split_idx = int(n * 0.8)
    train = data.iloc[:split_idx]
    test = data.iloc[split_idx:]

    model_eval = Prophet(yearly_seasonality=True)
    model_eval.fit(train)

    forecast_test = model_eval.predict(test[['ds']])
    y_true = test['y'].values
    y_pred = forecast_test['yhat'].values

    mae = rmse = mape = r2 = None

    try:
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))

        with np.errstate(divide='ignore', invalid='ignore'):
            perc_err = np.abs((y_true - y_pred) / y_true)
            mask = ~np.isnan(perc_err) & ~np.isinf(perc_err)
            if mask.sum() > 0:
                mape = float(np.mean(perc_err[mask]) * 100)

        r2 = r2_score(y_true, y_pred)

    except Exception as e:
        print(f" Metrics error for {job}: {e}")

    evaluation_results.append({
        "job_category": job,
        "MAE": None if mae is None else round(mae, 2),
        "RMSE": None if rmse is None else round(rmse, 2),
        "MAPE(%)": None if mape is None else round(mape, 2),
        "R2_Score": None if r2 is None else round(r2, 3),
        "DataPoints": n
    })

    # ------------ Forecast future using full dataset --------------
    model_full = Prophet(yearly_seasonality=True)
    model_full.fit(data)

    future = model_full.make_future_dataframe(periods=3, freq='M')
    forecast = model_full.predict(future)
    forecast['job_category'] = job

    forecast_all.append(
        forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'job_category']]
    )

    joblib.dump(
        model_full,
        os.path.join(MODEL_OUTPUT_DIR, f"prophet_model_{job.replace('/', '_').replace(' ', '_')}.pkl")
    )

# ============================================
# SAVE FORECAST TO ICEBERG
# ============================================
if not forecast_all:
    print(" No categories forecasted.")
    spark.stop()
    raise SystemExit(0)

df_forecast = pd.concat(forecast_all, ignore_index=True)
df_forecast.rename(columns={
    'ds': 'forecast_date',
    'yhat': 'predicted_jobs',
    'yhat_lower': 'lower_bound',
    'yhat_upper': 'upper_bound'
}, inplace=True)

spark_forecast = (
    spark.createDataFrame(df_forecast)
         .withColumn("created_at", F.current_timestamp())
)

(
    spark_forecast.writeTo(f"{CATALOG}.{DATABASE_GOLD}.{TABLE_FORECAST}")
    .using("iceberg")
    .createOrReplace()
)

print(f" Forecast results saved to: {CATALOG}.{DATABASE_GOLD}.{TABLE_FORECAST}")

# ============================================
# SAVE MODEL EVALUATION
# ============================================
df_eval = pd.DataFrame(evaluation_results)

spark_eval = (
    spark.createDataFrame(df_eval)
         .withColumn("evaluated_at", F.current_timestamp())
)

(
    spark_eval.writeTo(f"{CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")
    .using("iceberg")
    .createOrReplace()
)

print(f" Model evaluation saved to: {CATALOG}.{DATABASE_GOLD}.{TABLE_EVALUATION}")

# ============================================
# SUMMARY
# ============================================
print("\n Model Evaluation Summary:")
print(df_eval)

spark.stop()
print("\n Forecasting + Evaluation completed successfully!")
