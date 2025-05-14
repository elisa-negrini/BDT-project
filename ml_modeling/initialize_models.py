# Script 1: initialize_models.py
# Crea e salva un modello River di regressione per ogni ticker con dati storici

import pandas as pd
import joblib
from pathlib import Path
from river import compose, linear_model, preprocessing

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "GOOG",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

DATA_DIR = Path("data/minute")     # Directory con i dati storici (parquet)
MODEL_DIR = Path("models")         # Directory dove salvare i modelli
MODEL_DIR.mkdir(exist_ok=True)

for ticker in TICKERS:
    df = pd.read_parquet(DATA_DIR / f"{ticker}.parquet")
    df = df.dropna()

    # Calcolo target: variazione percentuale a 5 minuti
    df['target'] = (df['price'].shift(-5) - df['price']) / df['price']
    df = df.dropna(subset=['target'])  # rimuove le ultime righe con target nullo

    model = compose.Pipeline(
        preprocessing.StandardScaler(),
        linear_model.LinearRegression()
    )

    feature_cols = [col for col in df.columns if col not in ['timestamp', 'ticker', 'target']]

    for row in df.itertuples(index=False):
        x = {col: getattr(row, col) for col in feature_cols}
        y = float(getattr(row, 'target'))  # target continuo: % price change a 5 minuti
        model.learn_one(x, y)

    joblib.dump(model, MODEL_DIR / f"{ticker}.model")

print("Modelli di regressione inizializzati e salvati.")
