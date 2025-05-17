import os
import pandas as pd
from datetime import datetime
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# === CONFIG ===
ALPACA_API_KEY = "PKDIQVSRHAGVB1SMJDV1"
ALPACA_SECRET_KEY = "T7NAfaVIscGDDYT6VSW0bBaQQwF0SexEGfDNSJSA"
TICKERS = ["BRK.B", "AAPL", "TSLA", "GOOGL"]  # ← Aggiungi qui i ticker che ti servono
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 5, 15)
SAVE_DIR = "BDT-project\my_test_ingestion\historical_data"

# === Crea client Alpaca
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# === Crea cartella se non esiste
os.makedirs(SAVE_DIR, exist_ok=True)

# === Download e salvataggio
for ticker in TICKERS:
    print(f"⬇ Scarico dati per {ticker}...")
    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame.Minute,
            start=START_DATE,
            end=END_DATE
        )
        bars = client.get_stock_bars(req)
        df = bars.df.reset_index()
        df["ticker"] = ticker

        if df.empty:
            print(f"⚠ Nessun dato trovato per {ticker}")
            continue

        # Salva CSV
        save_path = os.path.join(SAVE_DIR, f"{ticker}.parquet")
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        df.to_parquet(save_path, index=False, engine="pyarrow", coerce_timestamps="ms")
        print(f"✅ Dati salvati in: {save_path}")

    except Exception as e:
        print(f"❌ Errore con {ticker}: {e}")