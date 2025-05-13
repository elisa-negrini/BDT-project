import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import os
import s3fs

# environment variables
ALPACA_API_KEY = os.getenv("AK6C3H9YZ2V5TIW1UVIZ")
ALPACA_SECRET_KEY = os.getenv("P4I01elSvfbV57mie90iDdRc2qVp6orGuUucToal")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin123")
S3_BUCKET = "historical"


# Connection to MinIO
fs = s3fs.S3FileSystem(
    anon=False,
    key=S3_ACCESS_KEY,
    secret=S3_SECRET_KEY,
    client_kwargs={'endpoint_url': S3_ENDPOINT}
)

top_30_tickers = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "GOOG",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

# Connection to Alpaca
client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# Data Request
request_params = StockBarsRequest(
    symbol_or_symbols=top_30_tickers,
    timeframe=TimeFrame.Minute,
    start=datetime(2025, 4, 23),
    end=datetime.today()
)

# Download data
print("⬇️  Scarico i dati storici da Alpaca...")
bars = client.get_stock_bars(request_params)
df = bars.df.reset_index()

# Writing data by day
for date, group in df.groupby(df['timestamp'].dt.date):
    date_str = date.strftime("%Y-%m-%d")
    s3_path = f"{S3_BUCKET}/stock_data/{date_str}.parquet"
    full_path = f"s3://{s3_path}"

    if fs.exists(full_path):
        print(f"⏩ File già esistente: {s3_path}, salto.")
    else:
        group.to_parquet(full_path, engine="pyarrow", filesystem=fs, index=False)
        print(f"✅ Salvato: {s3_path}")
