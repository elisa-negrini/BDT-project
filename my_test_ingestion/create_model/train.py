import os
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType
import pandas as pd
from neuralprophet import NeuralProphet
from minio import Minio # Import Minio client

def main():
    # Initialize Spark Session
    # It's good practice to get the existing SparkSession if one is already running
    # or create a new one if not.
    spark = SparkSession.builder \
        .appName("NeuralProphetStockPrediction") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{os.getenv('MINIO_ENDPOINT')}") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("✅ Spark Session initialized and MinIO configured.")

    # Define columns to read from the parquet files
    # Note: 'y1' is included as it will be our target 'y'
    columns_to_read = [
        "timestamp", "ticker", "y1",
        "price_mean_1min", "price_mean_5min", "price_std_5min",
        "price_mean_30min", "price_std_30min",
        "size_tot_1min", "size_tot_5min", "size_tot_30min",
        "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
        "sentiment_news_mean_1day", "sentiment_news_mean_3days",
        "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
        "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
        "eps", "freeCashFlow", "profit_margin", "debt_to_equity",
        "minutes_since_open", "day_of_week", "day_of_month",
        "week_of_year", "month_of_year",
        "market_open_spike_flag", "market_close_spike_flag"
    ]

    # --- 1. Load data using Spark directly from MinIO ---
    # The path 's3a://aggregated-data/*/*/*' will read all parquet files
    # within aggregated-data, respecting the ticker/year/month partitioning.
    try:
        df_spark = spark.read.parquet("s3a://aggregated-data/*/*/*").select(*columns_to_read)
        print("✅ Successfully connected to MinIO and loaded data using Spark.")
    except Exception as e:
        print(f"❌ Error connecting to MinIO or reading data with Spark: {e}")
        spark.stop()
        return

    # --- 2. Cast relevant columns to DoubleType in Spark ---
    # This is important for numerical operations and NeuralProphet compatibility
    cols_to_cast_to_double = [
        "y1", # Ensure y1 is also double
        "price_mean_1min", "price_mean_5min", "price_std_5min",
        "price_mean_30min", "price_std_30min",
        "size_tot_1min", "size_tot_5min", "size_tot_30min",
        "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
        "sentiment_news_mean_1day", "sentiment_news_mean_3days",
        "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
        "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
        "eps", "freeCashFlow", "profit_margin", "debt_to_equity"
    ]

    for colname in cols_to_cast_to_double:
        if colname in df_spark.columns:
            df_spark = df_spark.withColumn(colname, col(colname).cast(DoubleType()))

    # Ensure timestamp is of timestamp type for sorting
    df_spark = df_spark.withColumn("timestamp", to_timestamp("timestamp"))

    # --- 3. Prepare dataset in Spark before converting to Pandas ---
    # Rename 'timestamp' to 'ds' and 'y1' to 'y' for NeuralProphet
    # Drop rows where 'y' (which was 'y1') is null
    df_spark = df_spark.withColumnRenamed("timestamp", "ds") \
                       .withColumnRenamed("y1", "y") \
                       .dropna(subset=["y"])

    # Sort data, crucial for time series models, especially with multiple tickers
    df_spark = df_spark.orderBy("ticker", "ds")

    print(f"Pre-Pandas Spark DataFrame schema after casting and renaming:")
    df_spark.printSchema()
    print(f"Number of records in Spark DataFrame: {df_spark.count()}")


    # --- 4. Convert Spark DataFrame to Pandas DataFrame for NeuralProphet ---
    # This should be done only after all necessary Spark transformations
    df_pd = df_spark.toPandas()

    print("✅ Loaded data from MinIO and prepared for NeuralProphet:", df_pd.shape)
    print("Columns:", df_pd.columns.tolist())
    print("Time range:", df_pd['ds'].min(), "→", df_pd['ds'].max())

    # --- 5. Define regressors for NeuralProphet ---
    # These lists are correctly defined for NeuralProphet's add_lagged_regressor and add_regressor
    lagged_regressors = [
        "price_mean_1min", # Add price_mean_1min as a lagged regressor if it's predictive of future y
        "price_mean_5min", "price_std_5min",
        "price_mean_30min", "price_std_30min",
        "size_tot_1min", "size_tot_5min", "size_tot_30min",
        "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
        "sentiment_news_mean_1day", "sentiment_news_mean_3days",
        "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day"
    ]

    slow_regressors = [
        "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
        "eps", "freeCashFlow", "profit_margin", "debt_to_equity"
    ]

    static_regressors = [ # These are technically time-varying but not lagged features
        "minutes_since_open", "day_of_week", "day_of_month",
        "week_of_year", "month_of_year",
        "market_open_spike_flag", "market_close_spike_flag"
    ]

    # --- 6. Initialize and build NeuralProphet model ---
    print("🎯 Starting model training with n_lags=60 and n_forecasts=1 ...")
    model = NeuralProphet(
        n_lags=60,            # Using past 60 minutes of 'y'
        n_forecasts=1,        # Forecasting 1 minute ahead
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=True,
        learning_rate=0.01
    )

    # Add lagged regressors
    for reg in lagged_regressors:
        if reg in df_pd.columns:
            model.add_lagged_regressor(reg)
        else:
            print(f"Warning: Lagged regressor '{reg}' not found in DataFrame and will be skipped.")

    # Add other regressors (slow and static)
    for reg in slow_regressors + static_regressors:
        if reg in df_pd.columns:
            model.add_regressor(reg)
        else:
            print(f"Warning: Regressor '{reg}' not found in DataFrame and will be skipped.")

    # Add categorical regressor for 'ticker' if present
    if "ticker" in df_pd.columns:
        model.add_categorical_regressor("ticker")
    else:
        print("Warning: 'ticker' column not found in DataFrame. Multi-ticker modeling might not be fully effective.")


    # --- 7. Train model ---
    # Ensure 'ds' and 'y' columns are present and correctly typed in df_pd
    # NeuralProphet expects 'ds' to be datetime and 'y' to be numeric
    df_pd['ds'] = pd.to_datetime(df_pd['ds'])
    df_pd['y'] = pd.to_numeric(df_pd['y'])

    metrics = model.fit(df_pd, freq="min", progress="bar")
    print("Training complete:", metrics)
    print("✅ Training complete.")
    print("📈 Final loss (last epoch):", metrics['Loss'].values[-1])

    # --- 8. Save model ---
    model.save("model/neural_model_lags60.np")
    print("✅ Model saved to model/neural_model_lags60.np")

    # Stop Spark Session
    spark.stop()
    print("✅ Spark Session stopped.")

if __name__ == "__main__":
    main()


