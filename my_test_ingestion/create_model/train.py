#!/usr/bin/env python3
# train_model.py

import pandas as pd
import os
from neuralprophet import NeuralProphet
from minio import Minio
from io import BytesIO

def main():
    # Connect to MinIO and download parquet data from 'aggregated_historical' bucket
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False  # Set to True if using HTTPS
    )

    all_dfs = []

    # Lista tutti gli oggetti nel bucket
    objects = client.list_objects("aggregated_historical", recursive=True)

    # Filtra solo i file .parquet validi
    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            response = client.get_object("aggregated_historical", obj.object_name)
            df = pd.read_parquet(BytesIO(response.read()))
            all_dfs.append(df)

    # Concatena tutti i DataFrame
    df = pd.concat(all_dfs, ignore_index=True)

    # Shift the target variable so that each row at time t-1 has the target y from time t
    # (price_mean_1min column remains unchanged)
    df["y"] = df["price_mean_1min"].shift(-1)

    # Rename timestamp column to match NeuralProphet's expected format
    df = df.rename(columns={"timestamp": "ds"})

    # Drop the last row, which has no corresponding future y
    df = df.dropna(subset=["y"])

    # Initialize the NeuralProphet model
    model = NeuralProphet(
        n_lags=60,                # Number of past time steps (lags) to use for autoregression (60 minutes)
        n_forecasts=1,            # Number of future time steps to forecast (1 minute ahead)
        yearly_seasonality=True,  # Enable yearly seasonality component
        weekly_seasonality=True,  # Enable weekly seasonality component
        daily_seasonality=True,   # Enable daily seasonality component
        learning_rate=0.01        # Learning rate for training
    ) 

    # Add lagged regressors (additional time-dependent input features)
    regressors = [
        "price_mean_1min", "price_mean_5min", "price_std_5min",
        "price_mean_30min", "price_std_30min",
        "size_tot_1min", "size_tot_5min", "size_tot_30min",
        "sentiment_bluesky_mean_2hours", "sentiment_bluesky_mean_1day",
        "sentiment_news_mean_1day", "sentiment_news_mean_3days",
        "sentiment_general_bluesky_mean_2hours", "sentiment_general_bluesky_mean_1day",
        "gdp_real", "cpi", "ffr", "t10y", "t2y", "spread_10y_2y", "unemployment",
        "eps", "freeCashFlow", "profit_margin", "debt_to_equity"
    ]
    for reg in regressors:
        model.add_lagged_regressor(reg)

    # Add additional non-lagged numeric regressors (calendar/time-based or flags)
    model.add_regressor("minutes_since_open")
    model.add_regressor("day_of_week")
    model.add_regressor("day_of_month")
    model.add_regressor("week_of_year")
    model.add_regressor("month_of_year")
    model.add_regressor("market_open_spike_flag")
    model.add_regressor("market_close_spike_flag")

    # Add categorical variable (ticker) to allow learning different patterns per ticker
    model.add_categorical_regressor("ticker")

    # Train the model using the prepared dataset
    metrics = model.fit(df, freq="min", progress="bar")
    print("Training metrics:", metrics)

    # Save the trained model to disk
    model.save("model/neural_model.np")

if __name__ == "__main__":
    main()