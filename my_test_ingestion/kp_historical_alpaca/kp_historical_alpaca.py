from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, date
import json
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import logging
import time
import pytz
from threading import Timer

# === CONFIGURAZIONE ===
API_KEY_ALPACA = os.getenv("API_KEY_ALPACA", "PKSWWUCWEHB7XUXCFQWO")
API_SECRET_ALPACA = os.getenv("API_SECRET_ALPACA", "wG7Qaan4bQqbmYQN7PaaraPzAhYtU29JLXqlnoCo")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "h_alpaca")

START_DATE = datetime(2021, 1, 1)
END_DATE = datetime(2025, 5, 14)

CHECKPOINT_FILE = "checkpoint.json"

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "LLY",
    "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO", "IBM"
 ]

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)

# === Kafka ===
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            logger.info("✅ Connessione a Kafka riuscita.")
            return producer
        except Exception as e:
            logger.warning(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

# === Checkpoint ===
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ Impossibile leggere checkpoint: {e}")
    return {}

def save_checkpoint(cp):
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(cp, f, indent=2)
    except Exception as e:
        logger.error(f"❌ Errore scrittura checkpoint: {e}")

# === Scarica dati per un ticker ===
def download_ticker_data(ticker):
    logger.info(f"⬇️  Scarico dati per {ticker}...")

    client = StockHistoricalDataClient(API_KEY_ALPACA, API_SECRET_ALPACA)
    req = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=TimeFrame.Minute,
        start=START_DATE,
        end=END_DATE
    )

    try:
        bars = client.get_stock_bars(req).df
        if bars.empty:
            logger.warning(f"⚠️  Nessun dato per {ticker}")
            return None

        df = bars.reset_index()
        df = df.set_index('timestamp')
        df = df.tz_convert("America/New_York")
        df = df.between_time("09:30", "16:00")
        df = df.tz_convert("UTC").reset_index()
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        logger.info(f"📊 Scaricati {len(df)} record filtrati per {ticker}")
        return df

    except Exception as e:
        logger.error(f"❌ Errore scaricamento {ticker}: {e}")
        return None

# === Invio dati a Kafka per ogni giorno ===
def send_data_to_kafka(producer, ticker, df, checkpoint):
    if df is None or df.empty:
        return

    df['date'] = df['timestamp'].dt.date
    grouped = df.groupby('date')

    for date_val, group_df in grouped:
        date_str = str(date_val)
        if date_str in checkpoint.get(ticker, []):
            logger.info(f"⏭ Giorno già processato {ticker} {date_str}, salto.")
            continue

        message = {
            'ticker': ticker,
            'date': date_str,
            'year': date_val.year,
            'month': date_val.month,
            'data': group_df.drop('date', axis=1).to_dict('records')
        }

        try:
            future = producer.send(
                topic=KAFKA_TOPIC,
                key=f"{ticker}_{date_str}",
                value=message
            )
            future.get(timeout=10)
            logger.info(f"📤 Inviato {ticker} per {date_str} ({len(group_df)} record)")
            checkpoint.setdefault(ticker, []).append(date_str)
            save_checkpoint(checkpoint)
            reset_timer()

        except KafkaError as e:
            logger.error(f"❌ KafkaError {ticker} {date_str}: {e}")
        except Exception as e:
            logger.error(f"❌ Errore generico {ticker} {date_str}: {e}")


inactivity_timer = None

def timeout_exit():
    logger.warning("⏰ Nessun dato processato negli ultimi 300 secondi. Chiudo il producer.")
    save_checkpoint(checkpoint)  # opzionale: salvataggio prima dell'uscita
    os._exit(0)

def reset_timer():
    global inactivity_timer
    if inactivity_timer:
        inactivity_timer.cancel()
    inactivity_timer = Timer(300, timeout_exit)  # 5 minuti di inattività
    inactivity_timer.start()

# === Main ===
def main():
    global checkpoint
    logger.info("🚀 Avvio Producer Alpaca -> Kafka")
    producer = connect_kafka()
    checkpoint = load_checkpoint()

    try:
        processed_count = 0
        error_count = 0

        for ticker in TICKERS:
            try:
                # Verifica se tutte le date tra START_DATE ed END_DATE sono già nel checkpoint
                expected_dates = pd.date_range(START_DATE.date(), END_DATE.date(), freq='B')  # Giorni di borsa
                done_dates = set(checkpoint.get(ticker, []))
                remaining_dates = [d for d in expected_dates if d.strftime("%Y-%m-%d") not in done_dates]

                if not remaining_dates:
                    logger.info(f"✅ Ticker già completato: {ticker}, salto.")
                    continue

                df = download_ticker_data(ticker)
                if df is not None:
                    send_data_to_kafka(producer, ticker, df, checkpoint)
                    processed_count += 1
                    reset_timer() 
                else:
                    error_count += 1

            except Exception as e:
                logger.error(f"❌ Errore elaborazione {ticker}: {e}")
                error_count += 1

        producer.flush()
        logger.info(f"✅ Completato! Processati: {processed_count}, Errori: {error_count}")

    except KeyboardInterrupt:
        logger.info("⏹️  Interruzione da utente")
    except Exception as e:
        logger.error(f"❌ Errore fatale: {e}")
    finally:
        save_checkpoint(checkpoint)
        producer.close()
        logger.info("🔚 Producer chiuso")

if __name__ == "__main__":
    main()
