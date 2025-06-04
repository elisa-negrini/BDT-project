import os
import sys
import json
import numpy as np
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
import pytz
import pandas as pd
import io

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from minio import Minio
from minio.error import S3Error

TOP_30_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

fundamentals_data = {} # Popolato all'avvio tramite load_fundamental_data()

NY_TZ = pytz.timezone('America/New_York')

MINIO_URL = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_SECURE = False

def load_fundamental_data():
    """Carica i dati fondamentali delle aziende da MinIO."""
    print(" [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        
        bucket_name = "company-fundamentals"
        
        if not minio_client.bucket_exists(bucket_name):
            print(f"[ERROR] MinIO bucket '{bucket_name}' does not exist. No fundamental data loaded.", file=sys.stderr)
            return

        for ticker in TOP_30_TICKERS:
            object_name = f"{ticker}/2024.parquet"
            response = None
            try:
                response = minio_client.get_object(bucket_name, object_name)
                
                parquet_bytes = io.BytesIO(response.read())
                parquet_bytes.seek(0)
                df = pd.read_parquet(parquet_bytes)
                
                if not df.empty:
                    row = df.iloc[0]
                    # Ensure all values are converted to standard Python types (float, int)
                    # Added more robust handling for potential non-numeric values
                    eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) and pd.api.types.is_numeric_dtype(type(row.get("eps"))) else None
                    fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) and pd.api.types.is_numeric_dtype(type(row.get("cashflow_freeCashFlow"))) else None
                    revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) and pd.api.types.is_numeric_dtype(type(row.get("revenue"))) else None
                    net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) and pd.api.types.is_numeric_dtype(type(row.get("netIncome"))) else None
                    debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalDebt"))) else None
                    equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalStockholdersEquity"))) else None

                    profit_margin = net_income / revenue if revenue is not None and revenue != 0 else None
                    debt_to_equity = debt / equity if equity is not None and equity != 0 else None

                    fundamentals_data[ticker] = {
                        "eps": eps,
                        "freeCashFlow": fcf,
                        "profit_margin": profit_margin,
                        "debt_to_equity": debt_to_equity
                    }
                    print(f"[FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)
                else:
                    print(f"[WARN] Parquet file for {ticker}/{object_name} is empty.", file=sys.stderr)

            except S3Error as e:
                print(f"[ERROR] MinIO S3 Error for {ticker} ({object_name}): {e}", file=sys.stderr)
            except Exception as e:
                print(f"[ERROR] Could not load fundamental data for {ticker} from MinIO ({object_name}): {e}", file=sys.stderr)
            finally:
                if response:
                    response.close()
                    response.release_conn()
        print(" [INIT] Fundamental data loading complete.", file=sys.stderr)

    except Exception as e:
        print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)


class SlidingAggregator(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        def descriptor(name):
            return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

        # States for REAL trade data
        self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
        self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
        self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

        self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
        self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
        self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

        # States for FAKE (simulated) trade data
        self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
        self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
        self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

        self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
        self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
        self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

        # States for sentiment (specific per ticker)
        self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
        self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
        self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
        self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

        # Lo stato per il sentiment generale e i macrodata NON saranno qui,
        # ma verranno gestiti tramite Broadcast State (implementazione futura)

        self.last_timer_state = runtime_context.get_state(
            ValueStateDescriptor("last_timer", Types.LONG()))


    def _cleanup_old_entries(self, state, window_minutes):
        """Rimuove le entry dallo stato più vecchie della finestra specificata."""
        threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        keys_to_remove = []
        # It's crucial to convert the iterator to a list before iterating
        # to avoid ConcurrentModificationException if state is modified during iteration,
        # and to allow multiple passes if needed (though not directly here).
        for k in list(state.keys()): 
            try:
                dt_obj = isoparse(k)
                if dt_obj.tzinfo is None:
                    dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                
                if dt_obj < threshold:
                    keys_to_remove.append(k)
            except ValueError:
                print(f"[WARN] Invalid timestamp format '{k}' in state for cleanup. Removing.", file=sys.stderr)
                keys_to_remove.append(k)
            except Exception as e:
                print(f"[ERROR] Unexpected error during cleanup for key '{k}': {e}. Removing.", file=sys.stderr)
                keys_to_remove.append(k)
        
        for k_remove in keys_to_remove:
            state.remove(k_remove)


    def process_element(self, value, ctx):
        """Elabora ogni elemento in ingresso (stringa JSON) da Kafka."""
        try:
            data = json.loads(value)
            current_key = ctx.get_current_key()

            # --- Gestione dei dati di sentiment (specifici per ticker) ---
            if "social" in data and "sentiment_score" in data:
                social_source = data.get("social")
                sentiment_score = float(data.get("sentiment_score"))
                ts_str = data.get("timestamp")
                
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in sentiment data: {data}", file=sys.stderr)
                    return []

                # Assumiamo che qui arrivino solo sentiment per ticker specifici
                # o che "GENERAL" sia già stato filtrato/gestito prima di questo operatore keyed
                if current_key in TOP_30_TICKERS:
                    if social_source == "bluesky":
                        self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
                        self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
                    elif social_source == "news":
                        self.sentiment_news_1d.put(ts_str, sentiment_score)
                        self.sentiment_news_3d.put(ts_str, sentiment_score)
                    else:
                        print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
                        return []
                    # print(f"[SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr) # Rimosso per ridurre la verbosità
                return []

            # --- Gestione dei dati di Stock Trade ---
            elif "price" in data and "size" in data and "exchange" in data:
                ticker = data.get("ticker")
                if ticker not in TOP_30_TICKERS:
                    # Questo dovrebbe essere già filtrato dalla key_by, ma è un fallback
                    return []
                
                ts_str = data.get("timestamp")
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
                    return []

                price = float(data.get("price"))
                size = float(data.get("size"))
                exchange = data.get("exchange")

                if exchange != "RANDOM": # Dati reali
                    self.real_price_1m.put(ts_str, price)
                    self.real_price_5m.put(ts_str, price)
                    self.real_price_30m.put(ts_str, price)
                    self.real_size_1m.put(ts_str, size)
                    self.real_size_5m.put(ts_str, size)
                    self.real_size_30m.put(ts_str, size)
                else: # Dati simulati
                    self.fake_price_1m.put(ts_str, price)
                    self.fake_price_5m.put(ts_str, price)
                    self.fake_price_30m.put(ts_str, price)
                    self.fake_size_1m.put(ts_str, size)
                    self.fake_size_5m.put(ts_str, size)
                    self.fake_size_30m.put(ts_str, size)
                return []

            else:
                print(f"[WARN] Unrecognized data format in main job process_element: {value}", file=sys.stderr)
                return []

        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode JSON in main job process_element: {value}", file=sys.stderr)
            return []
        except Exception as e:
            print(f"[ERROR] process_element in main job: {e} for value: {value}", file=sys.stderr)
            return []
        finally:
            # Registra il timer per le predizioni ogni 10 secondi esatti
            last_timer = self.last_timer_state.value()
            current_processing_time = ctx.timer_service().current_processing_time()
            # Calculate the next exact 10-second mark
            next_ts = ((current_processing_time // 10000) + 1) * 10000 
            
            if last_timer is None or current_processing_time >= last_timer:
                ctx.timer_service().register_processing_time_timer(next_ts)
                self.last_timer_state.update(next_ts)


    def on_timer(self, timestamp, ctx):
        """Chiamata quando un timer registrato scatta."""
        try:
            # Converte il timestamp del timer (millisecondi) in un oggetto datetime UTC
            # e poi lo formatta come stringa ISO per la stampa e l'output
            ts_prediction = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            ts_str = ts_prediction.isoformat()
            
            ticker = ctx.get_current_key()

            # Solo i ticker nella lista TOP_30_TICKERS dovrebbero attivare questo timer
            if ticker not in TOP_30_TICKERS:
                return [] # Non processare chiavi non pertinenti

            # Funzioni helper
            # Convert RemovableConcatIterator to list explicitly here for robustness
            def mean(vals_iterator):
                vals_list = list(vals_iterator)
                return float(np.mean(vals_list)) if vals_list else 0.0

            def std(vals_iterator):
                vals_list = list(vals_iterator)
                return float(np.std(vals_list)) if vals_list and len(vals_list) > 1 else 0.0

            def total(vals_iterator):
                vals_list = list(vals_iterator)
                return float(np.sum(vals_list)) if vals_list else 0.0

            # Pulizia per TUTTI gli stati (reali e simulati)
            self._cleanup_old_entries(self.real_price_1m, 1)
            self._cleanup_old_entries(self.real_price_5m, 5)
            self._cleanup_old_entries(self.real_price_30m, 30)
            self._cleanup_old_entries(self.real_size_1m, 1)
            self._cleanup_old_entries(self.real_size_5m, 5)
            self._cleanup_old_entries(self.real_size_30m, 30)

            self._cleanup_old_entries(self.fake_price_1m, 1)
            self._cleanup_old_entries(self.fake_price_5m, 5)
            self._cleanup_old_entries(self.fake_price_30m, 30)
            self._cleanup_old_entries(self.fake_size_1m, 1)
            self._cleanup_old_entries(self.fake_size_5m, 5)
            self._cleanup_old_entries(self.fake_size_30m, 30)

            # Pulizia del sentiment specifico (invariata)
            self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
            self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
            self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

            # Usa il timestamp del timer per i calcoli orari, convertito in NY_TZ
            now_ny = ts_prediction.astimezone(NY_TZ) # Usa ts_prediction per ora NY
            market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
            
            is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Lun-Ven

            is_simulated_prediction = False
            if is_market_hours:
                # Always convert to list when fetching values from MapState to avoid iterator exhaustion
                price_1m_values = list(self.real_price_1m.values())
                price_5m_values = list(self.real_price_5m.values())
                price_30m_values = list(self.real_price_30m.values())
                size_1m_values = list(self.real_size_1m.values())
                size_5m_values = list(self.real_size_5m.values())
                size_30m_values = list(self.real_size_30m.values())
                is_simulated_prediction = False
            else:
                # Always convert to list when fetching values from MapState
                price_1m_values = list(self.fake_price_1m.values())
                price_5m_values = list(self.fake_price_5m.values())
                price_30m_values = list(self.fake_price_30m.values())
                size_1m_values = list(self.fake_size_1m.values())
                size_5m_values = list(self.fake_size_5m.values())
                size_30m_values = list(self.fake_size_30m.values())
                is_simulated_prediction = True


            # Calcola i flag di spike all'apertura/chiusura del mercato
            market_open_spike_flag = 0
            market_close_spike_flag = 0

            if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
                market_open_spike_flag = 1
            
            if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
                market_close_spike_flag = 1

            ticker_fundamentals = fundamentals_data.get(ticker, {})

            # Gestione di "minutes_since_open"
            minutes_since_open = -1 # Valore di default
            if now_ny >= market_open_time and now_ny < market_close_time:
                minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
            else:
                if now_ny < market_open_time: # Dalla mezzanotte all'apertura
                    minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
                    minutes_since_open = -(minutes_until_open) # Valore negativo per "prima dell'apertura"
                else: # Dopo la chiusura del mercato
                    minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30) # Minuti dalla chiusura + durata del mercato

            features = {
                "ticker": ticker,
                "timestamp": ts_str,
                "price_mean_1min": mean(price_1m_values),
                "price_mean_5min": mean(price_5m_values),
                "price_std_5min": std(price_5m_values),
                "price_mean_30min": mean(price_30m_values),
                "price_std_30min": std(price_30m_values),
                "size_tot_1min": total(size_1m_values),
                "size_tot_5min": total(size_5m_values),
                "size_tot_30min": total(size_30m_values),
                # SENTIMENT SPECIFICO PER TICKER
                "sentiment_bluesky_mean_2h": mean(list(self.sentiment_bluesky_2h.values())), # Ensure list conversion
                "sentiment_bluesky_mean_1d": mean(list(self.sentiment_bluesky_1d.values())), # Ensure list conversion
                "sentiment_news_mean_1d": mean(list(self.sentiment_news_1d.values())),       # Ensure list conversion
                "sentiment_news_mean_3d": mean(list(self.sentiment_news_3d.values())),       # Ensure list conversion
                # NEW TIME-BASED FEATURES
                "minutes_since_open": int(minutes_since_open),
                "day_of_week": int(now_ny.weekday()),
                "day_of_month": int(now_ny.day),
                "week_of_year": int(now_ny.isocalendar()[1]),
                "month_of_year": int(now_ny.month),
                "market_open_spike_flag": int(market_open_spike_flag),
                "market_close_spike_flag": int(market_close_spike_flag),
                # Dati fondamentali
                "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
                "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
                "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
                "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
                # Flag per indicare se la predizione è basata su dati simulati
                "is_simulated_prediction": is_simulated_prediction
            }

            result = json.dumps(features)
            print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

            return [result]
        except Exception as e:
            print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
            return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]

# --- Helper per splittare i dati di sentiment ---
def expand_sentiment_data(json_str):
    """
    Espande una singola stringa JSON di sentiment in più, se contiene una lista di ticker.
    Filtra specificamente il ticker "GENERAL" per assicurarsi che non venga processato qui.
    """
    try:
        data = json.loads(json_str)
        
        if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
            expanded_records = []
            original_ticker_list = data["ticker"]
            
            for ticker_item in original_ticker_list:
                # In questo job, processiamo SOLO i ticker specifici e non "GENERAL"
                if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
                    new_record = data.copy()
                    new_record["ticker"] = ticker_item
                    expanded_records.append(json.dumps(new_record))
            
            if not expanded_records:
                # Se dopo il filtro non rimangono ticker validi, non emettere nulla
                # print(f"[DEBUG] No specific tickers found or all filtered out for: {json_str}", file=sys.stderr)
                return []
            return expanded_records
        
        # Passa attraverso altri tipi di dati (es. trades) o sentiment già con un singolo ticker
        if "ticker" in data and data["ticker"] == "GENERAL":
            # Filtra esplicitamente i messaggi con ticker "GENERAL" qui,
            # dato che verranno gestiti dal job secondario.
            return []

        return [json_str] # Messaggi come trade o sentiment con singolo ticker
    except json.JSONDecodeError:
        print(f"[ERROR] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[ERROR] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
        return []

def route_by_ticker(json_str):
    """Determina la chiave per i dati JSON in ingresso."""
    try:
        data = json.loads(json_str)
        # Questo job si occupa solo di dati con un campo 'ticker' specifico
        if "ticker" in data:
            if data["ticker"] in TOP_30_TICKERS:
                return data["ticker"]
            else:
                # Messaggi con ticker non tracciati o "GENERAL" (già filtrato da expand_sentiment_data)
                # non dovrebbero arrivare qui se le trasformazioni precedenti funzionano
                return "discard_key"
        else:
            # Messaggi senza 'ticker' (es. macrodata) non dovrebbero arrivare qui
            # se i flussi sono separati correttamente.
            print(f"[WARN] Data with no 'ticker' field, discarding in main job: {json_str}", file=sys.stderr)
            return "discard_key"
    except json.JSONDecodeError:
        print(f"[WARN] Failed to decode JSON for key_by in main job: {json_str}", file=sys.stderr)
        return "invalid_json_key"
    except Exception as e:
        print(f"[ERROR] route_by_ticker in main job: {e} for {json_str}", file=sys.stderr)
        return "error_key"


def main():
    load_fundamental_data() # Carica i dati fondamentali una volta all'avvio del job

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Impostiamo la parallelizzazione a 1 per il testing iniziale,
                            # ma l'obiettivo è aumentarla per lo scaling per ticker.

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_main_job_group',
        'auto.offset.reset': 'earliest'
    }

    # Il consumer per il Job principale legge dai topic con dati specifici per ticker
    consumer = FlinkKafkaConsumer(
        topics=["stock_trades", "bluesky_sentiment"], # bluesky_sentiment verrà filtrato per i generali
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    producer = FlinkKafkaProducer(
        topic='main_data', # Topic di output per le predizioni
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    # Espandiamo e filtriamo i sentiment (escludendo 'GENERAL')
    expanded_and_filtered_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

    # Key by ticker
    keyed_stream = expanded_and_filtered_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
    # Processa con SlidingAggregator
    # Nota: la chiave 'discard_key' verrà gestita dal process function o semplicemente non genererà output significativo
    processed = keyed_stream.process(SlidingAggregator(), output_type=Types.STRING())
    
    # Filtra eventuali output indesiderati da "discard_key" se necessario,
    # anche se il process_element dovrebbe già restituire [] per quei casi.
    # processed.filter(lambda x: "discard_key" not in x).add_sink(producer)
    processed.add_sink(producer)

    env.execute("Main Job: Ticker-Specific Aggregation and Prediction")

if __name__ == "__main__":
    main()










































































# import os
# import sys
# import json
# import numpy as np
# from datetime import datetime, timezone, timedelta
# from dateutil.parser import isoparse
# import pytz
# import pandas as pd
# import io

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, BroadcastProcessFunction
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from minio import Minio
# from minio.error import S3Error

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# fundamentals_data = {} # Popolato all'avvio tramite load_fundamental_data()

# NY_TZ = pytz.timezone('America/New_York')

# MINIO_URL = "minio:9000"
# MINIO_ACCESS_KEY = "admin"
# MINIO_SECRET_KEY = "admin123"
# MINIO_SECURE = False

# # Alias per i dati macro, identici a quelli del job principale per coerenza
# macro_alias = {
#     "GDPC1": "gdp_real",
#     "CPIAUCSL": "cpi",
#     "FEDFUNDS": "ffr",
#     "DGS10": "t10y",
#     "DGS2": "t2y",
#     "T10Y2Y": "spread_10y_2y",
#     "UNRATE": "unemployment"
# }

# def load_fundamental_data():
#     """Carica i dati fondamentali delle aziende da MinIO."""
#     print(" [INIT] Loading fundamental data from MinIO...", file=sys.stderr)
#     try:
#         minio_client = Minio(
#             MINIO_URL,
#             access_key=MINIO_ACCESS_KEY,
#             secret_key=MINIO_SECRET_KEY,
#             secure=MINIO_SECURE
#         )
        
#         bucket_name = "company-fundamentals"
        
#         if not minio_client.bucket_exists(bucket_name):
#             print(f"[ERROR] MinIO bucket '{bucket_name}' does not exist. No fundamental data loaded.", file=sys.stderr)
#             return

#         for ticker in TOP_30_TICKERS:
#             object_name = f"{ticker}/2024.parquet"
#             response = None
#             try:
#                 response = minio_client.get_object(bucket_name, object_name)
                
#                 parquet_bytes = io.BytesIO(response.read())
#                 parquet_bytes.seek(0)
#                 df = pd.read_parquet(parquet_bytes)
                
#                 if not df.empty:
#                     row = df.iloc[0]
#                     # Ensure all values are converted to standard Python types (float, int)
#                     # Added more robust handling for potential non-numeric values
#                     eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) and pd.api.types.is_numeric_dtype(type(row.get("eps"))) else None
#                     fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) and pd.api.types.is_numeric_dtype(type(row.get("cashflow_freeCashFlow"))) else None
#                     revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) and pd.api.types.is_numeric_dtype(type(row.get("revenue"))) else None
#                     net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) and pd.api.types.is_numeric_dtype(type(row.get("netIncome"))) else None
#                     debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalDebt"))) else None
#                     equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) and pd.api.types.is_numeric_dtype(type(row.get("balance_totalStockholdersEquity"))) else None

#                     profit_margin = net_income / revenue if revenue is not None and revenue != 0 else None
#                     debt_to_equity = debt / equity if equity is not None and equity != 0 else None

#                     fundamentals_data[ticker] = {
#                         "eps": eps,
#                         "freeCashFlow": fcf,
#                         "profit_margin": profit_margin,
#                         "debt_to_equity": debt_to_equity
#                     }
#                     print(f"[FUNDAMENTALS] Loaded data for {ticker}: {fundamentals_data[ticker]}", file=sys.stderr)
#                 else:
#                     print(f"[WARN] Parquet file for {ticker}/{object_name} is empty.", file=sys.stderr)

#             except S3Error as e:
#                 print(f"[ERROR] MinIO S3 Error for {ticker} ({object_name}): {e}", file=sys.stderr)
#             except Exception as e:
#                 print(f"[ERROR] Could not load fundamental data for {ticker} from MinIO ({object_name}): {e}", file=sys.stderr)
#             finally:
#                 if response:
#                     response.close()
#                     response.release_conn()
#         print(" [INIT] Fundamental data loading complete.", file=sys.stderr)

#     except Exception as e:
#         print(f"[CRITICAL] Failed to initialize Minio client or load any fundamental data: {e}", file=sys.stderr)


# class StockDataAggregator(BroadcastProcessFunction):
#     # Define the broadcast state descriptor for global data
#     # This state will hold the latest values for macro data and general sentiment
#     GLOBAL_DATA_DESCRIPTOR = MapStateDescriptor(
#         "global_data_state",
#         Types.STRING(), # Key: e.g., "gdp_real", "cpi", "GENERAL_SENTIMENT_BLUESKY_2H_MEAN"
#         Types.FLOAT()   # Value: the actual data point
#     )
    
#     # Define state descriptors for general sentiment data (time-windowed)
#     # These will be updated by the broadcast stream and then aggregated for the global state
#     GENERAL_BLUESKY_SENTIMENT_2H_DESCRIPTOR = MapStateDescriptor(
#         "general_bluesky_sentiment_2h", Types.STRING(), Types.FLOAT())
#     GENERAL_BLUESKY_SENTIMENT_1D_DESCRIPTOR = MapStateDescriptor(
#         "general_bluesky_sentiment_1d", Types.STRING(), Types.FLOAT())
#     GENERAL_NEWS_SENTIMENT_1D_DESCRIPTOR = MapStateDescriptor(
#         "general_news_sentiment_1d", Types.STRING(), Types.FLOAT())
#     GENERAL_NEWS_SENTIMENT_3D_DESCRIPTOR = MapStateDescriptor(
#         "general_news_sentiment_3d", Types.STRING(), Types.FLOAT())


#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         # States for REAL trade data (Keyed State)
#         self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
#         self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
#         self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

#         self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
#         self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
#         self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

#         # States for FAKE (simulated) trade data (Keyed State)
#         self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
#         self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
#         self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

#         self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
#         self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
#         self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

#         # States for sentiment (specific per ticker) (Keyed State)
#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         # ValueState to keep track of the last registered timer timestamp (Keyed State)
#         self.last_timer_state = runtime_context.get_state(
#             ValueStateDescriptor("last_timer", Types.LONG()))

#         # Access to broadcast states for general sentiment (Managed by BroadcastProcessFunction)
#         # These are *not* keyed states, but are accessed via the BroadcastContext
#         # We need to explicitly get them within the open method for the BroadcastProcessFunction
#         self.general_bluesky_sentiment_2h_state = runtime_context.get_map_state(self.GENERAL_BLUESKY_SENTIMENT_2H_DESCRIPTOR)
#         self.general_bluesky_sentiment_1d_state = runtime_context.get_map_state(self.GENERAL_BLUESKY_SENTIMENT_1D_DESCRIPTOR)
#         self.general_news_sentiment_1d_state = runtime_context.get_map_state(self.GENERAL_NEWS_SENTIMENT_1D_DESCRIPTOR)
#         self.general_news_sentiment_3d_state = runtime_context.get_map_state(self.GENERAL_NEWS_SENTIMENT_3D_DESCRIPTOR)


#     def _cleanup_old_entries(self, state, window_minutes):
#         """Rimuove le entry dallo stato più vecchie della finestra specificata."""
#         threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#         keys_to_remove = []
#         for k in list(state.keys()): 
#             try:
#                 dt_obj = isoparse(k)
#                 if dt_obj.tzinfo is None:
#                     dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                
#                 if dt_obj < threshold:
#                     keys_to_remove.append(k)
#             except ValueError:
#                 print(f"[WARN] Invalid timestamp format '{k}' in state for cleanup. Removing.", file=sys.stderr)
#                 keys_to_remove.append(k)
#             except Exception as e:
#                 print(f"[ERROR] Unexpected error during cleanup for key '{k}': {e}. Removing.", file=sys.stderr)
#                 keys_to_remove.append(k)
        
#         for k_remove in keys_to_remove:
#             state.remove(k_remove)

#     def process_element(self, value, ctx: 'BroadcastProcessFunction.Context'):
#         """
#         Processes elements from the keyed stream (ticker-specific trades and sentiment).
#         This is equivalent to the old process_element from SlidingAggregator.
#         """
#         try:
#             data = json.loads(value)
#             current_key = ctx.get_current_key()

#             # --- Gestione dei dati di sentiment (specifici per ticker) ---
#             if "social" in data and "sentiment_score" in data:
#                 social_source = data.get("social")
#                 sentiment_score = float(data.get("sentiment_score"))
#                 ts_str = data.get("timestamp")
                
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in sentiment data: {data}", file=sys.stderr)
#                     return

#                 if current_key in TOP_30_TICKERS:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
#                     elif social_source == "news":
#                         self.sentiment_news_1d.put(ts_str, sentiment_score)
#                         self.sentiment_news_3d.put(ts_str, sentiment_score)
#                     else:
#                         print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
#                         return
#                 return

#             # --- Gestione dei dati di Stock Trade ---
#             elif "price" in data and "size" in data and "exchange" in data:
#                 ticker = data.get("ticker")
#                 if ticker not in TOP_30_TICKERS:
#                     return
                
#                 ts_str = data.get("timestamp")
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
#                     return

#                 price = float(data.get("price"))
#                 size = float(data.get("size"))
#                 exchange = data.get("exchange")

#                 if exchange != "RANDOM": # Dati reali
#                     self.real_price_1m.put(ts_str, price)
#                     self.real_price_5m.put(ts_str, price)
#                     self.real_price_30m.put(ts_str, price)
#                     self.real_size_1m.put(ts_str, size)
#                     self.real_size_5m.put(ts_str, size)
#                     self.real_size_30m.put(ts_str, size)
#                 else: # Dati simulati
#                     self.fake_price_1m.put(ts_str, price)
#                     self.fake_price_5m.put(ts_str, price)
#                     self.fake_price_30m.put(ts_str, price)
#                     self.fake_size_1m.put(ts_str, size)
#                     self.fake_size_5m.put(ts_str, size)
#                     self.fake_size_30m.put(ts_str, size)
#                 return

#             else:
#                 print(f"[WARN] Unrecognized data format in main job process_element: {value}", file=sys.stderr)
#                 return

#         except json.JSONDecodeError:
#             print(f"[ERROR] Failed to decode JSON in main job process_element: {value}", file=sys.stderr)
#             return
#         except Exception as e:
#             print(f"[ERROR] process_element in main job: {e} for value: {value}", file=sys.stderr)
#             return
#         finally:
#             # Registra il timer per le predizioni ogni 10 secondi esatti
#             last_timer = self.last_timer_state.value()
#             current_processing_time = ctx.timer_service().current_processing_time()
#             next_ts = ((current_processing_time // 10000) + 1) * 10000 
            
#             if last_timer is None or current_processing_time >= last_timer:
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)

#     def process_broadcast_element(self, value, ctx: 'BroadcastProcessFunction.BroadcastContext'):
#         """
#         Processes elements from the broadcast stream (general sentiment, macro data).
#         This method will update the broadcast state, which is then accessible by all
#         keyed instances in their on_timer method.
#         """
#         try:
#             data = json.loads(value)
#             broadcast_state = ctx.get_broadcast_state(self.GLOBAL_DATA_DESCRIPTOR)

#             # --- Gestione Dati Macro ---
#             if "alias" in data:
#                 alias_key = data.get("alias")
#                 new_value = float(data.get("value"))
                
#                 # Store macro data directly in the broadcast state
#                 broadcast_state.put(alias_key, new_value)
#                 print(f"[BROADCAST] Updated macro data '{alias_key}': {new_value}", file=sys.stderr)

#             # --- Gestione Sentiment Generale ---
#             elif "social" in data and "sentiment_score" in data and data.get("ticker") == "GENERAL":
#                 social_source = data.get("social")
#                 sentiment_score = float(data.get("sentiment_score"))
#                 ts_str = data.get("timestamp")

#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in general sentiment broadcast data: {data}", file=sys.stderr)
#                     return
                
#                 # Update the general sentiment states (which are also broadcast states)
#                 if social_source == "bluesky":
#                     self.general_bluesky_sentiment_2h_state.put(ts_str, sentiment_score)
#                     self.general_bluesky_sentiment_1d_state.put(ts_str, sentiment_score)
#                 elif social_source == "news":
#                     self.general_news_sentiment_1d_state.put(ts_str, sentiment_score)
#                     self.general_news_sentiment_3d_state.put(ts_str, sentiment_score)
                
#                 # Calculate means and store them in the main GLOBAL_DATA_DESCRIPTOR
#                 # This ensures the latest aggregated general sentiment is available
#                 # to all keyed operators.
                
#                 # Cleanup and calculate means for general sentiment
#                 self._cleanup_old_entries(self.general_bluesky_sentiment_2h_state, 2 * 60)
#                 self._cleanup_old_entries(self.general_bluesky_sentiment_1d_state, 24 * 60)
#                 self._cleanup_old_entries(self.general_news_sentiment_1d_state, 24 * 60)
#                 self._cleanup_old_entries(self.general_news_sentiment_3d_state, 3 * 24 * 60)

#                 def calculate_mean(vals_iterator):
#                     vals_list = list(vals_iterator)
#                     return float(np.mean(vals_list)) if vals_list else 0.0

#                 broadcast_state.put("general_sentiment_bluesky_mean_2h", calculate_mean(self.general_bluesky_sentiment_2h_state.values()))
#                 broadcast_state.put("general_sentiment_bluesky_mean_1d", calculate_mean(self.general_bluesky_sentiment_1d_state.values()))
#                 broadcast_state.put("general_sentiment_news_mean_1d", calculate_mean(self.general_news_sentiment_1d_state.values()))
#                 broadcast_state.put("general_sentiment_news_mean_3d", calculate_mean(self.general_news_sentiment_3d_state.values()))

#                 print(f"[BROADCAST] Updated general sentiment from {social_source} at {ts_str}", file=sys.stderr)
            
#             else:
#                 print(f"[WARN] Unrecognized broadcast data format: {value}", file=sys.stderr)

#         except json.JSONDecodeError:
#             print(f"[ERROR] Failed to decode JSON in process_broadcast_element: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"[ERROR] process_broadcast_element: {e} for value: {value}", file=sys.stderr)

#     def on_timer(self, timestamp: int, ctx: 'BroadcastProcessFunction.Context'):
#         """
#         Called when a registered timer fires. This now has access to both keyed and broadcast states.
#         """
#         try:
#             # Convert timer timestamp (milliseconds) to UTC datetime object
#             ts_prediction = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
#             ts_str = ts_prediction.isoformat()
            
#             ticker = ctx.get_current_key()

#             if ticker not in TOP_30_TICKERS:
#                 return [] 

#             def mean(vals_iterator):
#                 vals_list = list(vals_iterator)
#                 return float(np.mean(vals_list)) if vals_list else 0.0

#             def std(vals_iterator):
#                 vals_list = list(vals_iterator)
#                 return float(np.std(vals_list)) if vals_list and len(vals_list) > 1 else 0.0

#             def total(vals_iterator):
#                 vals_list = list(vals_iterator)
#                 return float(np.sum(vals_list)) if vals_list else 0.0

#             # Cleanup for ALL keyed states (real and simulated)
#             self._cleanup_old_entries(self.real_price_1m, 1)
#             self._cleanup_old_entries(self.real_price_5m, 5)
#             self._cleanup_old_entries(self.real_price_30m, 30)
#             self._cleanup_old_entries(self.real_size_1m, 1)
#             self._cleanup_old_entries(self.real_size_5m, 5)
#             self._cleanup_old_entries(self.real_size_30m, 30)

#             self._cleanup_old_entries(self.fake_price_1m, 1)
#             self._cleanup_old_entries(self.fake_price_5m, 5)
#             self._cleanup_old_entries(self.fake_price_30m, 30)
#             self._cleanup_old_entries(self.fake_size_1m, 1)
#             self._cleanup_old_entries(self.fake_size_5m, 5)
#             self._cleanup_old_entries(self.fake_size_30m, 30)

#             # Cleanup for ticker-specific sentiment
#             self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
#             self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

#             # Use timer timestamp for hourly calculations, converted to NY_TZ
#             now_ny = ts_prediction.astimezone(NY_TZ)
#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0, tzinfo=NY_TZ)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=NY_TZ)
            
#             is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5

#             is_simulated_prediction = False
#             if is_market_hours:
#                 price_1m_values = list(self.real_price_1m.values())
#                 price_5m_values = list(self.real_price_5m.values())
#                 price_30m_values = list(self.real_price_30m.values())
#                 size_1m_values = list(self.real_size_1m.values())
#                 size_5m_values = list(self.real_size_5m.values())
#                 size_30m_values = list(self.real_size_30m.values())
#                 is_simulated_prediction = False
#             else:
#                 price_1m_values = list(self.fake_price_1m.values())
#                 price_5m_values = list(self.fake_price_5m.values())
#                 price_30m_values = list(self.fake_price_30m.values())
#                 size_1m_values = list(self.fake_size_1m.values())
#                 size_5m_values = list(self.fake_size_5m.values())
#                 size_30m_values = list(self.fake_size_30m.values())
#                 is_simulated_prediction = True

#             # Calculate market spike flags
#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1
            
#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             ticker_fundamentals = fundamentals_data.get(ticker, {})

#             # Handle "minutes_since_open"
#             minutes_since_open = -1
#             if now_ny >= market_open_time and now_ny < market_close_time:
#                 minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
#             else:
#                 if now_ny < market_open_time:
#                     minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
#                     minutes_since_open = -(minutes_until_open)
#                 else:
#                     minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30)

#             # Access broadcast state here
#             # Make sure to get the read-only broadcast state
#             broadcast_state = ctx.get_broadcast_state(self.GLOBAL_DATA_DESCRIPTOR)
            
#             # Retrieve global sentiment and macro data from broadcast state
#             general_sentiment_bluesky_mean_2h = broadcast_state.get("general_sentiment_bluesky_mean_2h")
#             general_sentiment_bluesky_mean_1d = broadcast_state.get("general_sentiment_bluesky_mean_1d")
#             general_sentiment_news_mean_1d = broadcast_state.get("general_sentiment_news_mean_1d")
#             general_sentiment_news_mean_3d = broadcast_state.get("general_sentiment_news_mean_3d")

#             macro_data_for_features = {}
#             for alias_key in macro_alias.values():
#                 macro_data_for_features[alias_key] = broadcast_state.get(alias_key)

#             features = {
#                 "ticker": ticker,
#                 "timestamp": ts_str,
#                 "price_mean_1min": mean(price_1m_values),
#                 "price_mean_5min": mean(price_5m_values),
#                 "price_std_5min": std(price_5m_values),
#                 "price_mean_30min": mean(price_30m_values),
#                 "price_std_30min": std(price_30m_values),
#                 "size_tot_1min": total(size_1m_values),
#                 "size_tot_5min": total(size_5m_values),
#                 "size_tot_30min": total(size_30m_values),
#                 # SENTIMENT SPECIFIC TO TICKER
#                 "sentiment_bluesky_mean_2h": mean(list(self.sentiment_bluesky_2h.values())),
#                 "sentiment_bluesky_mean_1d": mean(list(self.sentiment_bluesky_1d.values())),
#                 "sentiment_news_mean_1d": mean(list(self.sentiment_news_1d.values())),
#                 "sentiment_news_mean_3d": mean(list(self.sentiment_news_3d.values())),
#                 # GLOBAL SENTIMENT (from broadcast state)
#                 "general_sentiment_bluesky_mean_2h": float(general_sentiment_bluesky_mean_2h) if general_sentiment_bluesky_mean_2h is not None else 0.0,
#                 "general_sentiment_bluesky_mean_1d": float(general_sentiment_bluesky_mean_1d) if general_sentiment_bluesky_mean_1d is not None else 0.0,
#                 "general_sentiment_news_mean_1d": float(general_sentiment_news_mean_1d) if general_sentiment_news_mean_1d is not None else 0.0,
#                 "general_sentiment_news_mean_3d": float(general_sentiment_news_mean_3d) if general_sentiment_news_mean_3d is not None else 0.0,
#                 # MACRO DATA (from broadcast state)
#                 **{k: (float(v) if v is not None else None) for k, v in macro_data_for_features.items()},
#                 # NEW TIME-BASED FEATURES
#                 "minutes_since_open": int(minutes_since_open),
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),
#                 # Fundamental data
#                 "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
#                 "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
#                 "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
#                 "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
#                 # Flag to indicate if the prediction is based on simulated data
#                 "is_simulated_prediction": is_simulated_prediction
#             }

#             result = json.dumps(features)
#             print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             return [result]
#         except Exception as e:
#             print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
#             return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]

# # --- Helper per splittare i dati di sentiment e macro ---
# def classify_and_prefix_data(json_str):
#     """
#     Classifica e prefissa i dati in ingresso per distinguerli tra keyed e broadcast.
#     - I dati macro sono prefissati con '__BROADCAST_MACRO__:'
#     - Il sentiment generale (ticker: "GENERAL") è prefissato con '__BROADCAST_GENERAL_SENTIMENT__:'
#     - Altri dati (trades, sentiment specifico per ticker) sono passati senza prefisso.
#     """
#     try:
#         data = json.loads(json_str)
        
#         # Classifica i dati macro
#         if "alias" in data:
#             return f"__BROADCAST_MACRO__:{json_str}"
        
#         # Classifica il sentiment generale
#         if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list) and "GENERAL" in data["ticker"]:
#             return f"__BROADCAST_GENERAL_SENTIMENT__:{json_str}"
        
#         # Espande il sentiment specifico per ticker (se è una lista)
#         if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#             expanded_records = []
#             original_ticker_list = data["ticker"]
#             for ticker_item in original_ticker_list:
#                 if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
#                     new_record = data.copy()
#                     new_record["ticker"] = ticker_item
#                     expanded_records.append(json.dumps(new_record))
#             return expanded_records if expanded_records else [] # Return empty list if no valid tickers
        
#         # Passa attraverso altri tipi di dati (es. trades) o sentiment già con un singolo ticker
#         if "ticker" in data and data["ticker"] in TOP_30_TICKERS:
#             return json_str
        
#         # Se non rientra in nessuna delle categorie sopra, scarta
#         print(f"[WARN] Unclassified data, discarding: {json_str}", file=sys.stderr)
#         return []

#     except json.JSONDecodeError:
#         print(f"[ERROR] Failed to decode JSON in classify_and_prefix_data: {json_str}", file=sys.stderr)
#         return []
#     except Exception as e:
#         print(f"[ERROR] classify_and_prefix_data: {e} for {json_str}", file=sys.stderr)
#         return []

# def route_by_ticker(json_str):
#     """Determina la chiave per i dati JSON in ingresso (solo per lo stream keyed)."""
#     try:
#         data = json.loads(json_str)
#         if "ticker" in data and data["ticker"] in TOP_30_TICKERS:
#             return data["ticker"]
#         # Questo caso non dovrebbe verificarsi se `classify_and_prefix_data` e i filtri sono corretti
#         print(f"[WARN] Data with no relevant 'ticker' field, discarding in key_by: {json_str}", file=sys.stderr)
#         return "discard_key"
#     except json.JSONDecodeError:
#         print(f"[WARN] Failed to decode JSON for key_by in main job: {json_str}", file=sys.stderr)
#         return "invalid_json_key"
#     except Exception as e:
#         print(f"[ERROR] route_by_ticker in main job: {e} for {json_str}", file=sys.stderr)
#         return "error_key"


# def main():
#     load_fundamental_data() # Carica i dati fondamentali una volta all'avvio del job

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1) # Impostiamo la parallelizzazione a 1 per il testing iniziale,
#                             # ma l'obiettivo è aumentarla per lo scaling per ticker.

#     consumer_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_main_job_group',
#         'auto.offset.reset': 'earliest'
#     }

#     # Il consumer legge da tutti i topic rilevanti
#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "bluesky_sentiment", "news_sentiment", "macrodata"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated_data', # Topic di output per le predizioni aggregate
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     raw_stream = env.add_source(consumer, type_info=Types.STRING())
    
#     # Applica la funzione per classificare e prefissare i dati
#     # Questo flat_map restituirà una lista di stringhe (potenzialmente vuota, o con prefissi)
#     classified_stream = raw_stream.flat_map(classify_and_prefix_data, output_type=Types.STRING())

#     # Separa lo stream in due: uno per i dati keyed e uno per i dati broadcast
#     keyed_stream_data = classified_stream.filter(
#         lambda x: not (x.startswith("__BROADCAST_MACRO__:") or x.startswith("__BROADCAST_GENERAL_SENTIMENT__:"))
#     )
    
#     broadcast_stream_data = classified_stream.filter(
#         lambda x: x.startswith("__BROADCAST_MACRO__:") or x.startswith("__BROADCAST_GENERAL_SENTIMENT__:")
#     ).map(
#         lambda x: x.split(":", 1)[1], # Rimuove il prefisso
#         output_type=Types.STRING()
#     )

#     # Key by ticker for ticker-specific data
#     keyed_stream = keyed_stream_data.key_by(route_by_ticker, key_type=Types.STRING())
    
#     # Broadcast the global data stream
#     broadcast_stream = broadcast_stream_data.broadcast(StockDataAggregator.GLOBAL_DATA_DESCRIPTOR)
    
#     # Connetti lo stream keyed con lo stream broadcast e applica la BroadcastProcessFunction
#     processed_stream = keyed_stream.connect(broadcast_stream) \
#                                    .process(StockDataAggregator(), output_type=Types.STRING())
    
#     # Aggiungi il sink al topic di output
#     processed_stream.add_sink(producer)

#     env.execute("Main Job: Ticker-Specific and Global Data Aggregation with Broadcast State")

# if __name__ == "__main__":
#     main()








# Traceback (most recent call last):


# File "/opt/main_job.py", line 1195, in <module>


# main()


# File "/opt/main_job.py", line 1186, in main


# processed_stream = keyed_stream.connect(broadcast_stream) \


# File "/usr/local/lib/python3.10/dist-packages/pyflink/datastream/data_stream.py", line 2677, in process


# raise TypeError("BroadcastProcessFunction should be applied to non-keyed DataStream")


# TypeError: BroadcastProcessFunction should be applied to non-keyed DataStream