import os
import pandas as pd
from datetime import datetime
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# === CONFIG ===
ALPACA_API_KEY = "PKDIQVSRHAGVB1SMJDV1"
ALPACA_SECRET_KEY = "T7NAfaVIscGDDYT6VSW0bBaQQwF0SexEGfDNSJSA"

START_DATE = datetime(2025, 5, 1)
END_DATE = datetime(2025, 5, 15)
SAVE_DIR = "downloaded_stock_data"

# === Lista ticker S&P Top 30 ===
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

# === Crea client Alpaca
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# === Crea directory principale
os.makedirs(SAVE_DIR, exist_ok=True)

# === Ciclo su ogni ticker
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

        # Estrai colonna giorno
        df["day"] = pd.to_datetime(df["timestamp"]).dt.date

        # Salva partizionato per giorno
        for day, day_df in df.groupby("day"):
            day_str = day.strftime("%Y-%m-%d")
            partition_path = os.path.join(SAVE_DIR, f"ticker={ticker}", f"day={day_str}")
            os.makedirs(partition_path, exist_ok=True)
            file_path = os.path.join(partition_path, "part-0001.parquet")
            day_df.to_parquet(file_path, index=False)
            print(f"✅ Salvato: {file_path}")

    except Exception as e:
        print(f"❌ Errore con {ticker}: {e}")
