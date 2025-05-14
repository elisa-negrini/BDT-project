# Script 2: stream_predict_update.py

import pandas as pd
import joblib
from pathlib import Path
from river import metrics

TICKERS = [...]  # stessa lista di initialize_models.py
MODEL_DIR = Path("models")
models = {ticker: joblib.load(MODEL_DIR / f"{ticker}.model") for ticker in TICKERS}

metric = metrics.MAE()  # errore assoluto medio per regressione

def stream_generator():
    for row in pd.read_parquet("data/live_stream_simulation.parquet").itertuples(index=False):
        yield row._asdict()

for row in stream_generator():
    ticker = row['ticker']
    x = {k: v for k, v in row.items() if k not in ['timestamp', 'ticker', 'target']}
    y = float(row['target'])

    model = models[ticker]
    y_pred = model.predict_one(x)
    model.learn_one(x, y)

    metric.update(y, y_pred)
    print(f"[{ticker}] y_pred={y_pred:.4f}, y={y:.4f}, MAE={metric.get():.5f}")

