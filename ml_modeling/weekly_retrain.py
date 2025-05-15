# Script 3: weekly_retrain.py
# QUESTO FA IL RETRAIN OGNI 60 GIORNI, NON SO SE è QUELLO CHE VOGLIAMO
import pandas as pd
import joblib
from pathlib import Path
from datetime import timedelta
from river import compose, linear_model, preprocessing

TICKERS = [...]  # stessa lista
DATA_DIR = Path("data/minute")
MODEL_DIR = Path("models")
DAYS_BACK = 60

for ticker in TICKERS:
    df = pd.read_parquet(DATA_DIR / f"{ticker}.parquet")
    df = df[df['timestamp'] > df['timestamp'].max() - timedelta(days=DAYS_BACK)]
    df = df.dropna()

    # Ricalcolo del target
    df['target'] = (df['price'].shift(-5) - df['price']) / df['price']
    df = df.dropna(subset=['target'])

    model = compose.Pipeline(
        preprocessing.StandardScaler(),
        linear_model.LinearRegression()
    )

    feature_cols = [col for col in df.columns if col not in ['timestamp', 'ticker', 'target']]

    for row in df.itertuples(index=False):
        x = {col: getattr(row, col) for col in feature_cols}
        y = float(getattr(row, 'target'))
        model.learn_one(x, y)

    joblib.dump(model, MODEL_DIR / f"{ticker}.model")

print("Modelli riaddestrati con dati aggiornati.")
