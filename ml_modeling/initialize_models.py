# initialize_models.py – Previsione del prezzo_mean_1min tra 5 minuti

import pandas as pd
import joblib
from pathlib import Path
from river import compose, linear_model, preprocessing

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "GOOG",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

DATA_DIR = Path("data/minute") # da cambiare
MODEL_DIR = Path("models_multivariate")
MODEL_DIR.mkdir(exist_ok=True)

FEATURES = [
    "price_mean_1min", "price_mean_5min", "log_return_5min",
    "price_std_15min", "price_max_30min", "Size_1min", "Size_diff_1min",
    "Size_mean_15min", "Size_std_15min", "rsi_14", "ema_10", "macd_line",
    "bollinger_width", "obv", "GDP", "GDPC1", "QoQ", "CPI", "Fed_Funds_Rate",
    "Treasury_10Y", "Treasury_2Y", "Yield_Spread", "Unemployment_Rate",
    "UMich_Sentiment", "sentiment_mean_2hours", "sentiment_mean_1day",
    "sentiment_delta_2hour_1day", "posts_volume_1h", "sentiment_mean_1d",
    "sentiment_mean_3d", "sentiment_delta_1d_3d", "news_mentions_3days",
    "minutes_since_open", "sin_minute", "cos_minute", "day_of_week",
    "day_of_month", "week_of_year", "month_of_year", "is_month_end",
    "is_quarter_end", "holiday_proximity", "market_open_spike_flag",
    "market_close_spike_flag"
]

for ticker in TICKERS:
    df = pd.read_parquet(DATA_DIR / f"{ticker}.parquet").dropna()

    # Calcolo del target: valore futuro di price_mean_1min tra 5 minuti
    df["target"] = df["price_mean_1min"].shift(-5)
    df = df.dropna(subset=["target"])

    available_features = [f for f in FEATURES if f in df.columns]
    if not available_features:
        print(f"[{ticker}] Nessuna feature disponibile, skipping.")
        continue

    model = compose.Pipeline(
        preprocessing.StandardScaler(),
        linear_model.LinearRegression()
    )

    for row in df.itertuples(index=False):
        x = {f: getattr(row, f) for f in available_features if pd.notnull(getattr(row, f))}
        if len(x) == len(available_features):
            y = float(getattr(row, "target"))
            model.learn_one(x, y)

    joblib.dump(model, MODEL_DIR / f"{ticker}.model")
    print(f"[{ticker}] Modello salvato con {len(available_features)} feature.")

print("✅ Training completato: modelli multivariati per previsione prezzo salvati.")
