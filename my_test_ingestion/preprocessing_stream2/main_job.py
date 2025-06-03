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
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from minio import Minio
# from minio.error import S3Error

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# # Queste due variabili diventeranno uno stato di broadcast nel Job principale,
# # alimentato da un altro topic Kafka gestito dal Job secondario.
# # Per ora, le inizializziamo vuote.
# macro_data_global = {} # Verrà popolato tramite broadcast state
# general_sentiment_global = { # Verrà popolato tramite broadcast state
#     "sentiment_bluesky_mean_general_2hours": 0.0,
#     "sentiment_bluesky_mean_general_1d": 0.0
# }


# fundamentals_data = {} # Popolato all'avvio tramite load_fundamental_data()

# NY_TZ = pytz.timezone('America/New_York')

# MINIO_URL = "minio:9000"
# MINIO_ACCESS_KEY = "admin"
# MINIO_SECRET_KEY = "admin123"
# MINIO_SECURE = False

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
#                     eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) else None
#                     fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) else None
#                     revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) else None
#                     net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) else None
#                     debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) else None
#                     equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) else None

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


# class SlidingAggregator(KeyedProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         # States for REAL trade data
#         self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
#         self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
#         self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

#         self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
#         self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
#         self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

#         # States for FAKE (simulated) trade data
#         self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
#         self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
#         self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

#         self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
#         self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
#         self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

#         # States for sentiment (specific per ticker)
#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         # Lo stato per il sentiment generale e i macrodata NON saranno qui,
#         # ma verranno gestiti tramite Broadcast State (implementazione futura)

#         self.last_timer_state = runtime_context.get_state(
#             ValueStateDescriptor("last_timer", Types.LONG()))


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


#     def process_element(self, value, ctx):
#         """Elabora ogni elemento in ingresso (stringa JSON) da Kafka."""
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
#                     return []

#                 # Assumiamo che qui arrivino solo sentiment per ticker specifici
#                 # o che "GENERAL" sia già stato filtrato/gestito prima di questo operatore keyed
#                 if current_key in TOP_30_TICKERS:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
#                     elif social_source == "news":
#                         self.sentiment_news_1d.put(ts_str, sentiment_score)
#                         self.sentiment_news_3d.put(ts_str, sentiment_score)
#                     else:
#                         print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
#                         return []
#                     # print(f"[SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr) # Rimosso per ridurre la verbosità
#                 return []

#             # --- Gestione dei dati di Stock Trade ---
#             elif "price" in data and "size" in data and "exchange" in data:
#                 ticker = data.get("ticker")
#                 if ticker not in TOP_30_TICKERS:
#                     # Questo dovrebbe essere già filtrato dalla key_by, ma è un fallback
#                     return []
                
#                 ts_str = data.get("timestamp")
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
#                     return []

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
#                 return []

#             else:
#                 print(f"[WARN] Unrecognized data format in main job process_element: {value}", file=sys.stderr)
#                 return []

#         except json.JSONDecodeError:
#             print(f"[ERROR] Failed to decode JSON in main job process_element: {value}", file=sys.stderr)
#             return []
#         except Exception as e:
#             print(f"[ERROR] process_element in main job: {e} for value: {value}", file=sys.stderr)
#             return []
#         finally:
#             # Registra il timer per le predizioni ogni 5 secondi
#             last_timer = self.last_timer_state.value()
#             if last_timer is None or ctx.timer_service().current_processing_time() >= last_timer:
#                 next_ts = ctx.timer_service().current_processing_time() + 5000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)


#     def on_timer(self, timestamp, ctx):
#         """Chiamata quando un timer registrato scatta."""
#         try:
#             now_utc = datetime.now(timezone.utc)
#             ts_str = now_utc.isoformat()
#             ticker = ctx.get_current_key()

#             # Solo i ticker nella lista TOP_30_TICKERS dovrebbero attivare questo timer
#             if ticker not in TOP_30_TICKERS:
#                 # print(f"[WARN] on_timer fired for unexpected key (not in TOP_30_TICKERS): {ticker}", file=sys.stderr)
#                 return [] # Non processare chiavi non pertinenti

#             # Funzioni helper (rimangono le stesse)
#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else 0.0

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals and len(vals) > 1 else 0.0

#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             # Pulizia per TUTTI gli stati (reali e simulati)
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

#             # Pulizia del sentiment specifico (invariata)
#             self._cleanup_old_entries(self.sentiment_bluesky_2h, 2 * 60)
#             self._cleanup_old_entries(self.sentiment_bluesky_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_1d, 24 * 60)
#             self._cleanup_old_entries(self.sentiment_news_3d, 3 * 24 * 60)

#             now_ny = now_utc.astimezone(NY_TZ)
#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0, tzinfo=NY_TZ)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=NY_TZ)
            
#             is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Lun-Ven

#             is_simulated_prediction = False
#             if is_market_hours:
#                 # Se il mercato è aperto, usa dati reali
#                 price_1m_values = self.real_price_1m.values()
#                 price_5m_values = self.real_price_5m.values()
#                 price_30m_values = self.real_price_30m.values()
#                 size_1m_values = self.real_size_1m.values()
#                 size_5m_values = self.real_size_5m.values()
#                 size_30m_values = self.real_size_30m.values()
#                 is_simulated_prediction = False
#             else:
#                 # Se il mercato è chiuso, usa dati simulati per la predizione
#                 price_1m_values = self.fake_price_1m.values()
#                 price_5m_values = self.fake_price_5m.values()
#                 price_30m_values = self.fake_price_30m.values()
#                 size_1m_values = self.fake_size_1m.values()
#                 size_5m_values = self.fake_size_5m.values()
#                 size_30m_values = self.fake_size_30m.values()
#                 is_simulated_prediction = True


#             # Calcola i flag di spike all'apertura/chiusura del mercato
#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1
            
#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             ticker_fundamentals = fundamentals_data.get(ticker, {})

#             # Gestione di "minutes_since_open"
#             minutes_since_open = -1 # Valore di default
#             if now_ny >= market_open_time and now_ny < market_close_time:
#                 minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
#             else:
#                 if now_ny < market_open_time: # Dalla mezzanotte all'apertura
#                     minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
#                     minutes_since_open = -(minutes_until_open) # Valore negativo per "prima dell'apertura"
#                 else: # Dopo la chiusura del mercato
#                     minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30) # Minuti dalla chiusura + durata del mercato

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
#                 # SENTIMENT SPECIFICO PER TICKER
#                 "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
#                 "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
#                 "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
#                 "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),
#                 # SENTIMENT GENERALE (verrà dal broadcast state)
#                 "sentiment_bluesky_mean_general_2hours": general_sentiment_global["sentiment_bluesky_mean_general_2hours"],
#                 "sentiment_bluesky_mean_general_1d": general_sentiment_global["sentiment_bluesky_mean_general_1d"],
#                 # NEW TIME-BASED FEATURES
#                 "minutes_since_open": int(minutes_since_open),
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),
#                 # Dati fondamentali
#                 "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
#                 "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
#                 "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
#                 "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
#                 # Flag per indicare se la predizione è basata su dati simulati
#                 "is_simulated_prediction": is_simulated_prediction
#             }
            
#             # MACRO DATA (verranno dal broadcast state)
#             for macro_key_alias, macro_value in macro_data_global.items():
#                 features[macro_key_alias] = float(macro_value)

#             result = json.dumps(features)
#             print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             return [result]
#         except Exception as e:
#             print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
#             return [json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)})]

# # --- Helper per splittare i dati di sentiment ---
# def expand_sentiment_data(json_str):
#     """
#     Espande una singola stringa JSON di sentiment in più, se contiene una lista di ticker.
#     Filtra specificamente il ticker "GENERAL" per assicurarsi che non venga processato qui.
#     """
#     try:
#         data = json.loads(json_str)
        
#         if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#             expanded_records = []
#             original_ticker_list = data["ticker"]
            
#             for ticker_item in original_ticker_list:
#                 # In questo job, processiamo SOLO i ticker specifici e non "GENERAL"
#                 if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
#                     new_record = data.copy()
#                     new_record["ticker"] = ticker_item
#                     expanded_records.append(json.dumps(new_record))
            
#             if not expanded_records:
#                 # Se dopo il filtro non rimangono ticker validi, non emettere nulla
#                 # print(f"[DEBUG] No specific tickers found or all filtered out for: {json_str}", file=sys.stderr)
#                 return []
#             return expanded_records
        
#         # Passa attraverso altri tipi di dati (es. trades) o sentiment già con un singolo ticker
#         if "ticker" in data and data["ticker"] == "GENERAL":
#             # Filtra esplicitamente i messaggi con ticker "GENERAL" qui,
#             # dato che verranno gestiti dal job secondario.
#             return []

#         return [json_str] # Messaggi come trade o sentiment con singolo ticker
#     except json.JSONDecodeError:
#         print(f"[ERROR] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
#         return []
#     except Exception as e:
#         print(f"[ERROR] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
#         return []

# def route_by_ticker(json_str):
#     """Determina la chiave per i dati JSON in ingresso."""
#     try:
#         data = json.loads(json_str)
#         # Questo job si occupa solo di dati con un campo 'ticker' specifico
#         if "ticker" in data:
#             if data["ticker"] in TOP_30_TICKERS:
#                 return data["ticker"]
#             else:
#                 # Messaggi con ticker non tracciati o "GENERAL" (già filtrato da expand_sentiment_data)
#                 # non dovrebbero arrivare qui se le trasformazioni precedenti funzionano
#                 return "discard_key"
#         else:
#             # Messaggi senza 'ticker' (es. macrodata) non dovrebbero arrivare qui
#             # se i flussi sono separati correttamente.
#             print(f"[WARN] Data with no 'ticker' field, discarding in main job: {json_str}", file=sys.stderr)
#             return "discard_key"
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

#     # Il consumer per il Job principale legge dai topic con dati specifici per ticker
#     consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "news_sentiment", "bluesky_sentiment"], # bluesky_sentiment verrà filtrato per i generali
#         deserialization_schema=SimpleStringSchema(),
#         properties=consumer_props
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated_data', # Topic di output per le predizioni
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )

#     stream = env.add_source(consumer, type_info=Types.STRING())
    
#     # Espandiamo e filtriamo i sentiment (escludendo 'GENERAL')
#     expanded_and_filtered_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

#     # Key by ticker
#     keyed_stream = expanded_and_filtered_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
#     # Processa con SlidingAggregator
#     # Nota: la chiave 'discard_key' verrà gestita dal process function o semplicemente non genererà output significativo
#     processed = keyed_stream.process(SlidingAggregator(), output_type=Types.STRING())
    
#     # Filtra eventuali output indesiderati da "discard_key" se necessario,
#     # anche se il process_element dovrebbe già restituire [] per quei casi.
#     # processed.filter(lambda x: "discard_key" not in x).add_sink(producer)
#     processed.add_sink(producer)

#     env.execute("Main Job: Ticker-Specific Aggregation and Prediction")

# if __name__ == "__main__":
#     main()


































































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
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, KeyedBroadcastProcessFunction
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from minio import Minio
# from minio.error import S3Error

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# # Queste due variabili diventeranno uno stato di broadcast nel Job principale,
# # alimentato da un altro topic Kafka gestito dal Job secondario.
# # Per ora, le inizializziamo vuote.
# # macro_data_global = {} # Verrà popolato tramite broadcast state
# # general_sentiment_global = { # Verrà popolato tramite broadcast state
# #     "sentiment_bluesky_mean_general_2hours": 0.0,
# #     "sentiment_bluesky_mean_general_1d": 0.0
# # }


# fundamentals_data = {} # Popolato all'avvio tramite load_fundamental_data()

# NY_TZ = pytz.timezone('America/New_York')

# # Broadcast State Descriptor
# GLOBAL_BROADCAST_DESCRIPTOR = MapStateDescriptor(
#     "global_data_state",
#     Types.STRING(),  # key: 'macro_data' or 'general_sentiment'
#     Types.MAP(Types.STRING(), Types.FLOAT())  # value: dict di valori float
# )


# MINIO_URL = "minio:9000"
# MINIO_ACCESS_KEY = "admin"
# MINIO_SECRET_KEY = "admin123"
# MINIO_SECURE = False

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
#                     eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) else None
#                     fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) else None
#                     revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) else None
#                     net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) else None
#                     debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) else None
#                     equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) else None

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


# class SlidingAggregatorBroadcast(KeyedBroadcastProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
#         self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
#         self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

#         self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
#         self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
#         self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

#         self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
#         self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
#         self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

#         self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
#         self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
#         self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
        
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         self.last_timer_state = runtime_context.get_state(ValueStateDescriptor("last_timer", Types.LONG()))


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


#     def process_element(self, value, ctx):
#         """Elabora ogni elemento in ingresso (stringa JSON) da Kafka."""
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
#                     return []

#                 # Assumiamo che qui arrivino solo sentiment per ticker specifici
#                 # o che "GENERAL" sia già stato filtrato/gestito prima di questo operatore keyed
#                 if current_key in TOP_30_TICKERS:
#                     if social_source == "bluesky":
#                         self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
#                         self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
#                     elif social_source == "news":
#                         self.sentiment_news_1d.put(ts_str, sentiment_score)
#                         self.sentiment_news_3d.put(ts_str, sentiment_score)
#                     else:
#                         print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
#                         return []
#                     # print(f"[SENTIMENT] {current_key} - {social_source} - {ts_str}: {sentiment_score}", file=sys.stderr) # Rimosso per ridurre la verbosità
#                 return []

#             # --- Gestione dei dati di Stock Trade ---
#             elif "price" in data and "size" in data and "exchange" in data:
#                 ticker = data.get("ticker")
#                 if ticker not in TOP_30_TICKERS:
#                     # Questo dovrebbe essere già filtrato dalla key_by, ma è un fallback
#                     return []
                
#                 ts_str = data.get("timestamp")
#                 if not ts_str:
#                     print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
#                     return []

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
#                 return []

#             else:
#                 print(f"[WARN] Unrecognized data format in main job process_element: {value}", file=sys.stderr)
#                 return []

#         except json.JSONDecodeError:
#             print(f"[ERROR] Failed to decode JSON in main job process_element: {value}", file=sys.stderr)
#             return []
#         except Exception as e:
#             print(f"[ERROR] process_element in main job: {e} for value: {value}", file=sys.stderr)
#             return []
#         finally:
#             # Registra il timer per le predizioni ogni 5 secondi
#             last_timer = self.last_timer_state.value()
#             if last_timer is None or ctx.timer_service().current_processing_time() >= last_timer:
#                 next_ts = ctx.timer_service().current_processing_time() + 5000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)

#     def process_broadcast_element(self, value, ctx, out):
#         print(f"[DEBUG] process_broadcast_element called with value: {value}, ctx: {ctx}, out: {out}", file=sys.stderr)
#         try:
#             data = json.loads(value)
#             macro = data.get("macro_data", {})
#             sentiment = data.get("general_sentiment", {})

#             # out qui è BroadcastProcessFunction.Context, e .get_broadcast_state() restituisce il MapState
#             broadcast_map_state = ctx.get_broadcast_state(GLOBAL_BROADCAST_DESCRIPTOR)
#             if macro:
#                 cleaned_macro = {k: float(v) for k, v in macro.items()}
#                 broadcast_map_state.put("macro_data", cleaned_macro)
#                 print(f"[DEBUG] Updated macro_data in broadcast state: {cleaned_macro}", file=sys.stderr)
                
#             if sentiment:
#                 cleaned_sentiment = {k: float(v) for k, v in sentiment.items()}
#                 broadcast_map_state.put("general_sentiment", cleaned_sentiment)
#                 print(f"[DEBUG] Updated general_sentiment in broadcast state: {cleaned_sentiment}", file=sys.stderr)
#         except Exception as e:
#             print(f"[ERROR] process_broadcast_element: {e}", file=sys.stderr)


#     def on_timer(self, timestamp, ctx, out):
#         """
#         Chiamata quando un timer registrato scatta.
#         Questo metodo aggrega i dati raccolti nello stato e produce un output.
#         """
#         try:
#             # Ottiene il timestamp corrente in UTC e lo formatta
#             now_utc = datetime.now(timezone.utc)
#             ts_str = now_utc.isoformat()
#             ticker = ctx.get_current_key()

#             # Ignora i timer per ticker non tracciati
#             if ticker not in TOP_30_TICKERS:
#                 # print(f"[WARN] on_timer fired for unexpected key (not in TOP_30_TICKERS): {ticker}", file=sys.stderr)
#                 return # Non emettere nulla

#             # --- Funzioni helper per calcoli statistici ---
#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else 0.0

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals and len(vals) > 1 else 0.0

#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             # --- Pulizia degli stati specifici per ticker ---
#             # Una singola funzione cleanup per migliorare la leggibilità
#             def cleanup_state(state, window_minutes):
#                 threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#                 keys_to_remove = []
#                 for k in list(state.keys()):
#                     try:
#                         dt_obj = isoparse(k)
#                         if dt_obj.tzinfo is None:
#                             dt_obj = dt_obj.replace(tzinfo=timezone.utc)
#                         if dt_obj < threshold:
#                             keys_to_remove.append(k)
#                     except ValueError:
#                         # Print a warning for invalid timestamp and remove
#                         print(f"[WARN] Invalid timestamp format '{k}' in state. Removing.", file=sys.stderr)
#                         keys_to_remove.append(k)
#                     except Exception as e:
#                         print(f"[ERROR] Error during state cleanup for key '{k}': {e}. Removing.", file=sys.stderr)
#                         keys_to_remove.append(k)
#                 for k_remove in keys_to_remove:
#                     state.remove(k_remove)

#             # Applica la pulizia a tutti gli stati temporali
#             cleanup_state(self.real_price_1m, 1)
#             cleanup_state(self.real_price_5m, 5)
#             cleanup_state(self.real_price_30m, 30)
#             cleanup_state(self.real_size_1m, 1)
#             cleanup_state(self.real_size_5m, 5)
#             cleanup_state(self.real_size_30m, 30)

#             cleanup_state(self.fake_price_1m, 1)
#             cleanup_state(self.fake_price_5m, 5)
#             cleanup_state(self.fake_price_30m, 30)
#             cleanup_state(self.fake_size_1m, 1)
#             cleanup_state(self.fake_size_5m, 5)
#             cleanup_state(self.fake_size_30m, 30)

#             cleanup_state(self.sentiment_bluesky_2h, 2 * 60) # 2 ore
#             cleanup_state(self.sentiment_bluesky_1d, 24 * 60) # 1 giorno
#             cleanup_state(self.sentiment_news_1d, 24 * 60) # 1 giorno
#             cleanup_state(self.sentiment_news_3d, 3 * 24 * 60) # 3 giorni

#             # --- Determinazione dell'orario di mercato e scelta dei dati (reali/simulati) ---
#             now_ny = now_utc.astimezone(NY_TZ)
#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
#             is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Lun-Ven

#             is_simulated_prediction = False
#             if is_market_hours:
#                 # Se il mercato è aperto, usa dati reali
#                 price_1m_values = self.real_price_1m.values()
#                 price_5m_values = self.real_price_5m.values()
#                 price_30m_values = self.real_price_30m.values()
#                 size_1m_values = self.real_size_1m.values()
#                 size_5m_values = self.real_size_5m.values()
#                 size_30m_values = self.real_size_30m.values()
#             else:
#                 # Se il mercato è chiuso, usa dati simulati per la predizione
#                 price_1m_values = self.fake_price_1m.values()
#                 price_5m_values = self.fake_price_5m.values()
#                 price_30m_values = self.fake_price_30m.values()
#                 size_1m_values = self.fake_size_1m.values()
#                 size_5m_values = self.fake_size_5m.values()
#                 size_30m_values = self.fake_size_30m.values()
#                 is_simulated_prediction = True

#             # Calcola i flag di spike all'apertura/chiusura del mercato
#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1

#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             # Gestione di "minutes_since_open"
#             minutes_since_open = -1 # Valore di default
#             if now_ny >= market_open_time and now_ny < market_close_time:
#                 minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
#             else:
#                 if now_ny < market_open_time: # Dalla mezzanotte all'apertura
#                     minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
#                     minutes_since_open = -minutes_until_open # Valore negativo per "prima dell'apertura"
#                 else: # Dopo la chiusura del mercato
#                     # Calcola i minuti passati dall'apertura teorica fino all'ora attuale (inclusa la durata del mercato)
#                     minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + (16*60 - 9*60 - 30) # Minuti dalla chiusura + durata del mercato (390 minuti)

#             # --- Accesso al Broadcast State ---
#             # Ottieni l'accesso al MapState che contiene i dati broadcast
#             broadcast_map_state = ctx.get_broadcast_state(GLOBAL_BROADCAST_DESCRIPTOR)

#             # Recupera i macrodata e il sentiment generale dal broadcast state
#             # Usa .get() con un default {} per evitare errori se la chiave non è ancora presente
#             macro_data_global = broadcast_map_state.get("macro_data") or {}
#             general_sentiment_global = broadcast_map_state.get("general_sentiment") or {
#                 "sentiment_bluesky_mean_general_2hours": 0.0,
#                 "sentiment_bluesky_mean_general_1d": 0.0
#             }

#             # --- Recupero dei dati fondamentali per il ticker corrente ---
#             # fundamentals_data è una variabile globale popolata all'avvio del job
#             ticker_fundamentals = fundamentals_data.get(ticker, {})

#             # --- Costruzione delle feature per la predizione ---
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

#                 # SENTIMENT SPECIFICO PER TICKER (dai keyed state)
#                 "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
#                 "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
#                 "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
#                 "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),

#                 # SENTIMENT GENERALE (dal broadcast state)
#                 "sentiment_bluesky_mean_general_2hours": general_sentiment_global.get("sentiment_bluesky_mean_general_2hours", 0.0),
#                 "sentiment_bluesky_mean_general_1d": general_sentiment_global.get("sentiment_bluesky_mean_general_1d", 0.0),

#                 # NUOVE FEATURE BASATE SUL TEMPO
#                 "minutes_since_open": int(minutes_since_open),
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),

#                 # DATI FONDAMENTALI (caricati all'avvio del job)
#                 "eps": float(ticker_fundamentals.get("eps", 0.0)) if ticker_fundamentals.get("eps") is not None else None,
#                 "freeCashFlow": float(ticker_fundamentals.get("freeCashFlow", 0.0)) if ticker_fundamentals.get("freeCashFlow") is not None else None,
#                 "profit_margin": float(ticker_fundamentals.get("profit_margin", 0.0)) if ticker_fundamentals.get("profit_margin") is not None else None,
#                 "debt_to_equity": float(ticker_fundamentals.get("debt_to_equity", 0.0)) if ticker_fundamentals.get("debt_to_equity") is not None else None,

#                 # Flag per indicare se la predizione è basata su dati simulati
#                 "is_simulated_prediction": is_simulated_prediction
#             }

#             # MACRO DATA (dal broadcast state)
#             # Aggiungi dinamicamente tutti i macrodata presenti nel broadcast state
#             for macro_key_alias, macro_value in macro_data_global.items():
#                 features[macro_key_alias] = float(macro_value)

#             # Converte le features in una stringa JSON
#             result = json.dumps(features)

#             print(f"[PREDICTION] {ts_str} - {ticker} => {result}", file=sys.stderr)

#             # Emette il risultato
#             out.collect(result) # Usa out.collect(result) per emettere, non ctx.output(result)

#         except Exception as e:
#             print(f"[ERROR] Errore nel on_timer per ticker {ticker}: {e}", file=sys.stderr)
#             # Emette un messaggio di errore per visibilità
#             out.collect(json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)}))
            
# # --- Helper per splittare i dati di sentiment ---
# def expand_sentiment_data(json_str):
#     """
#     Espande una singola stringa JSON di sentiment in più, se contiene una lista di ticker.
#     Filtra specificamente il ticker "GENERAL" per assicurarsi che non venga processato qui.
#     """
#     try:
#         data = json.loads(json_str)
        
#         if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#             expanded_records = []
#             original_ticker_list = data["ticker"]
            
#             for ticker_item in original_ticker_list:
#                 # In questo job, processiamo SOLO i ticker specifici e non "GENERAL"
#                 if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
#                     new_record = data.copy()
#                     new_record["ticker"] = ticker_item
#                     expanded_records.append(json.dumps(new_record))
            
#             if not expanded_records:
#                 # Se dopo il filtro non rimangono ticker validi, non emettere nulla
#                 # print(f"[DEBUG] No specific tickers found or all filtered out for: {json_str}", file=sys.stderr)
#                 return []
#             return expanded_records
        
#         # Passa attraverso altri tipi di dati (es. trades) o sentiment già con un singolo ticker
#         if "ticker" in data and data["ticker"] == "GENERAL":
#             # Filtra esplicitamente i messaggi con ticker "GENERAL" qui,
#             # dato che verranno gestiti dal job secondario.
#             return []

#         return [json_str] # Messaggi come trade o sentiment con singolo ticker
#     except json.JSONDecodeError:
#         print(f"[ERROR] Failed to decode JSON in expand_sentiment_data: {json_str}", file=sys.stderr)
#         return []
#     except Exception as e:
#         print(f"[ERROR] expand_sentiment_data: {e} for {json_str}", file=sys.stderr)
#         return []

# def route_by_ticker(json_str):
#     """Determina la chiave per i dati JSON in ingresso."""
#     try:
#         data = json.loads(json_str)
#         # Questo job si occupa solo di dati con un campo 'ticker' specifico
#         if "ticker" in data:
#             if data["ticker"] in TOP_30_TICKERS:
#                 return data["ticker"]
#             else:
#                 # Messaggi con ticker non tracciati o "GENERAL" (già filtrato da expand_sentiment_data)
#                 # non dovrebbero arrivare qui se le trasformazioni precedenti funzionano
#                 return "discard_key"
#         else:
#             # Messaggi senza 'ticker' (es. macrodata) non dovrebbero arrivare qui
#             # se i flussi sono separati correttamente.
#             print(f"[WARN] Data with no 'ticker' field, discarding in main job: {json_str}", file=sys.stderr)
#             return "discard_key"
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

#     kafka_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_main_job_group',
#         'auto.offset.reset': 'earliest'
#     }

#     ticker_consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "bluesky_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )
#     ticker_stream = env.add_source(ticker_consumer, type_info=Types.STRING())
#     keyed_stream = ticker_stream.key_by(lambda v: json.loads(v).get("ticker", "UNKNOWN"), key_type=Types.STRING())

#     global_consumer = FlinkKafkaConsumer(
#         topics=["global_data"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )
#     global_stream = env.add_source(global_consumer, type_info=Types.STRING())
#     broadcast_stream = global_stream.broadcast(GLOBAL_BROADCAST_DESCRIPTOR)

#     processed = keyed_stream.connect(broadcast_stream).process(
#         SlidingAggregatorBroadcast(), output_type=Types.STRING()
#     )

#     producer = FlinkKafkaProducer(
#         topic='aggregated_data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )
#     processed.add_sink(producer)

#     env.execute("Main Job with Broadcast State")

# if __name__ == "__main__":
#     main()

















































































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
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, KeyedBroadcastProcessFunction, MapFunction
# from pyflink.datastream.functions import FlatMapFunction
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from minio import Minio
# from minio.error import S3Error

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# fundamentals_data = {}

# NY_TZ = pytz.timezone('America/New_York')

# # Broadcast State Descriptor
# GLOBAL_BROADCAST_DESCRIPTOR = MapStateDescriptor(
#     "global_data_state",
#     Types.STRING(),  # key: 'macro_data' or 'general_sentiment'
#     Types.MAP(Types.STRING(), Types.FLOAT())  # value: dict di valori float
# )

# MINIO_URL = "minio:9000"
# MINIO_ACCESS_KEY = "admin"
# MINIO_SECRET_KEY = "admin123"
# MINIO_SECURE = False

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
#                     eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) else None
#                     fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) else None
#                     revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) else None
#                     net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) else None
#                     debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) else None
#                     equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) else None

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


# class ExpandSentimentFunction(FlatMapFunction):
#     """Funzione per espandere i dati di sentiment con liste di ticker."""
    
#     def flat_map(self, value):
#         try:
#             data = json.loads(value)

#             if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#                 expanded_records = []
#                 original_ticker_list = data["ticker"]

#                 for ticker_item in original_ticker_list:
#                     if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
#                         new_record = data.copy()
#                         new_record["ticker"] = ticker_item
#                         expanded_records.append(json.dumps(new_record))

#                 return expanded_records if expanded_records else []

#             if "ticker" in data and data["ticker"] == "GENERAL":
#                 return []

#             return [value] # Messaggi come trade o sentiment con singolo ticker

#         except Exception as e:
#             print(f"[ERROR] ExpandSentimentFunction: {e} per {value}", file=sys.stderr)
#             return []


# class TickerKeySelector(MapFunction):
#     """Funzione per estrarre la chiave ticker dai dati JSON."""
    
#     def map(self, value):
#         try:
#             data = json.loads(value)
#             if "ticker" in data and data["ticker"] in TOP_30_TICKERS:
#                 return (data["ticker"], value)
#             else:
#                 return ("DISCARD", value)
#         except Exception as e:
#             print(f"[ERROR] TickerKeySelector: {e} for {value}", file=sys.stderr)
#             return ("ERROR", value)


# class SlidingAggregatorBroadcast(KeyedBroadcastProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
#         self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
#         self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

#         self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
#         self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
#         self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

#         self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
#         self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
#         self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

#         self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
#         self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
#         self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
        
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         self.last_timer_state = runtime_context.get_state(ValueStateDescriptor("last_timer", Types.LONG()))

#     def process_element(self, value, ctx, out):
#         """Elabora ogni elemento in ingresso (stringa JSON) da Kafka."""
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
#             # Registra il timer per le predizioni ogni 5 secondi
#             current_time = ctx.timer_service().current_processing_time()
#             last_timer = self.last_timer_state.value()
#             if last_timer is None or current_time >= last_timer:
#                 next_ts = current_time + 5000
#                 ctx.timer_service().register_processing_time_timer(next_ts)
#                 self.last_timer_state.update(next_ts)

#     def process_broadcast_element(self, value, ctx, out):
#         """Processa gli elementi broadcast (dati globali)."""
#         try:
#             data = json.loads(value)
#             macro = data.get("macro_data", {})
#             sentiment = data.get("general_sentiment", {})

#             broadcast_state = ctx.get_broadcast_state(GLOBAL_BROADCAST_DESCRIPTOR)
            
#             if macro:
#                 cleaned_macro = {k: float(v) for k, v in macro.items()}
#                 broadcast_state.put("macro_data", cleaned_macro)
#                 print(f"[DEBUG] Updated macro_data in broadcast state: {cleaned_macro}", file=sys.stderr)
                
#             if sentiment:
#                 cleaned_sentiment = {k: float(v) for k, v in sentiment.items()}
#                 broadcast_state.put("general_sentiment", cleaned_sentiment)
#                 print(f"[DEBUG] Updated general_sentiment in broadcast state: {cleaned_sentiment}", file=sys.stderr)
                
#         except Exception as e:
#             print(f"[ERROR] process_broadcast_element: {e}", file=sys.stderr)

#     def on_timer(self, timestamp, ctx, out):
#         """Chiamata quando un timer registrato scatta."""
#         try:
#             now_utc = datetime.now(timezone.utc)
#             ts_str = now_utc.isoformat()
#             ticker = ctx.get_current_key()

#             if ticker not in TOP_30_TICKERS:
#                 return

#             # Funzioni helper
#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else 0.0

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals and len(vals) > 1 else 0.0

#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             # Pulizia stati
#             def cleanup_state(state, window_minutes):
#                 threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#                 keys_to_remove = []
#                 for k in list(state.keys()):
#                     try:
#                         dt_obj = isoparse(k)
#                         if dt_obj.tzinfo is None:
#                             dt_obj = dt_obj.replace(tzinfo=timezone.utc)
#                         if dt_obj < threshold:
#                             keys_to_remove.append(k)
#                     except Exception:
#                         keys_to_remove.append(k)
#                 for k_remove in keys_to_remove:
#                     state.remove(k_remove)

#             # Applica la pulizia
#             cleanup_state(self.real_price_1m, 1)
#             cleanup_state(self.real_price_5m, 5)
#             cleanup_state(self.real_price_30m, 30)
#             cleanup_state(self.real_size_1m, 1)
#             cleanup_state(self.real_size_5m, 5)
#             cleanup_state(self.real_size_30m, 30)
#             cleanup_state(self.fake_price_1m, 1)
#             cleanup_state(self.fake_price_5m, 5)
#             cleanup_state(self.fake_price_30m, 30)
#             cleanup_state(self.fake_size_1m, 1)
#             cleanup_state(self.fake_size_5m, 5)
#             cleanup_state(self.fake_size_30m, 30)
#             cleanup_state(self.sentiment_bluesky_2h, 2 * 60)
#             cleanup_state(self.sentiment_bluesky_1d, 24 * 60)
#             cleanup_state(self.sentiment_news_1d, 24 * 60)
#             cleanup_state(self.sentiment_news_3d, 3 * 24 * 60)

#             # Determina orario di mercato
#             now_ny = now_utc.astimezone(NY_TZ)
#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
#             is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5

#             is_simulated_prediction = False
#             if is_market_hours:
#                 price_1m_values = self.real_price_1m.values()
#                 price_5m_values = self.real_price_5m.values()
#                 price_30m_values = self.real_price_30m.values()
#                 size_1m_values = self.real_size_1m.values()
#                 size_5m_values = self.real_size_5m.values()
#                 size_30m_values = self.real_size_30m.values()
#             else:
#                 price_1m_values = self.fake_price_1m.values()
#                 price_5m_values = self.fake_price_5m.values()
#                 price_30m_values = self.fake_price_30m.values()
#                 size_1m_values = self.fake_size_1m.values()
#                 size_5m_values = self.fake_size_5m.values()
#                 size_30m_values = self.fake_size_30m.values()
#                 is_simulated_prediction = True

#             # Calcola flag di spike
#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1

#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             # Calcola minutes_since_open
#             minutes_since_open = -1
#             if now_ny >= market_open_time and now_ny < market_close_time:
#                 minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
#             else:
#                 if now_ny < market_open_time:
#                     minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
#                     minutes_since_open = -minutes_until_open
#                 else:
#                     minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + 390

#             # Accesso al broadcast state
#             broadcast_state = ctx.get_broadcast_state(GLOBAL_BROADCAST_DESCRIPTOR)
#             macro_data_global = broadcast_state.get("macro_data") or {}
#             general_sentiment_global = broadcast_state.get("general_sentiment") or {
#                 "sentiment_bluesky_mean_general_2hours": 0.0,
#                 "sentiment_bluesky_mean_general_1d": 0.0
#             }

#             # Dati fondamentali
#             ticker_fundamentals = fundamentals_data.get(ticker, {})

#             # Costruzione features
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
#                 "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
#                 "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
#                 "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
#                 "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),
#                 "sentiment_bluesky_mean_general_2hours": general_sentiment_global.get("sentiment_bluesky_mean_general_2hours", 0.0),
#                 "sentiment_bluesky_mean_general_1d": general_sentiment_global.get("sentiment_bluesky_mean_general_1d", 0.0),
#                 "minutes_since_open": int(minutes_since_open),
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),
#                 "eps": float(ticker_fundamentals.get("eps", 0.0)) if ticker_fundamentals.get("eps") is not None else 0.0,
#                 "freeCashFlow": float(ticker_fundamentals.get("freeCashFlow", 0.0)) if ticker_fundamentals.get("freeCashFlow") is not None else 0.0,
#                 "profit_margin": float(ticker_fundamentals.get("profit_margin", 0.0)) if ticker_fundamentals.get("profit_margin") is not None else 0.0,
#                 "debt_to_equity": float(ticker_fundamentals.get("debt_to_equity", 0.0)) if ticker_fundamentals.get("debt_to_equity") is not None else 0.0,
#                 "is_simulated_prediction": is_simulated_prediction
#             }

#             # Aggiungi macro data
#             for macro_key_alias, macro_value in macro_data_global.items():
#                 features[macro_key_alias] = float(macro_value)

#             result = json.dumps(features)
#             print(f"[PREDICTION] {ts_str} - {ticker} => prediction generated", file=sys.stderr)
#             out.collect(result)

#         except Exception as e:
#             print(f"[ERROR] on_timer per ticker {ctx.get_current_key()}: {e}", file=sys.stderr)
#             error_result = json.dumps({
#                 "ticker": ctx.get_current_key(), 
#                 "timestamp": datetime.now(timezone.utc).isoformat(), 
#                 "error": str(e)
#             })
#             out.collect(error_result)


# def main():
#     load_fundamental_data()

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     kafka_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_main_job_group',
#         'auto.offset.reset': 'earliest'
#     }

#     # Stream per ticker data (trades + sentiment)
#     ticker_consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "bluesky_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )
#     ticker_stream = env.add_source(ticker_consumer, type_info=Types.STRING())
    
#     # Espandi sentiment data e filtra
#     expanded_stream = ticker_stream.flat_map(ExpandSentimentFunction(), output_type=Types.STRING())
    
#     # Filtra solo i ticker validi
#     filtered_stream = expanded_stream.filter(
#         lambda x: json.loads(x).get("ticker") in TOP_30_TICKERS if x else False
#     )
    
#     # Key by ticker
#     keyed_stream = filtered_stream.key_by(
#         lambda x: json.loads(x).get("ticker", "UNKNOWN"), 
#         key_type=Types.STRING()
#     )

#     # Stream per dati globali (broadcast)
#     global_consumer = FlinkKafkaConsumer(
#         topics=["global_data"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )
#     global_stream = env.add_source(global_consumer, type_info=Types.STRING())
#     broadcast_stream = global_stream.broadcast(GLOBAL_BROADCAST_DESCRIPTOR)

#     # Connetti keyed stream con broadcast stream
#     connected_stream = keyed_stream.connect(broadcast_stream)
#     processed = connected_stream.process(
#         SlidingAggregatorBroadcast(), 
#         output_type=Types.STRING()
#     )

#     # Output sink
#     producer = FlinkKafkaProducer(
#         topic='aggregated_data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )
#     processed.add_sink(producer)

#     env.execute("Main Job with Broadcast State")

# if __name__ == "__main__":
#     main()

































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

# Le variabili globali per macro_data e general_sentiment_global non sono più necessarie qui
# poiché il job non si occuperà più del broadcast state direttamente.
# Verranno arricchite da un job successivo.

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
                    eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) else None
                    fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) else None
                    revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) else None
                    net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) else None
                    debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) else None
                    equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) else None

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

        # Lo stato per il sentiment generale e i macrodata NON saranno qui.

        self.last_timer_state = runtime_context.get_state(
            ValueStateDescriptor("last_timer", Types.LONG()))

    def _cleanup_old_entries(self, state, window_minutes):
        """Rimuove le entry dallo stato più vecchie della finestra specificata."""
        threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        keys_to_remove = []
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

    def process_element(self, value, ctx, out): # 'out' è necessario anche qui
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
                    return # Non emettere nulla

                # Assumiamo che qui arrivino solo sentiment per ticker specifici
                if current_key in TOP_30_TICKERS:
                    if social_source == "bluesky":
                        self.sentiment_bluesky_2h.put(ts_str, sentiment_score)
                        self.sentiment_bluesky_1d.put(ts_str, sentiment_score)
                    elif social_source == "news":
                        self.sentiment_news_1d.put(ts_str, sentiment_score)
                        self.sentiment_news_3d.put(ts_str, sentiment_score)
                    else:
                        print(f"[WARN] Unknown social source for ticker {current_key}: {social_source}", file=sys.stderr)
                        return # Non emettere nulla
                return # Non emettere nulla

            # --- Gestione dei dati di Stock Trade ---
            elif "price" in data and "size" in data and "exchange" in data:
                ticker = data.get("ticker")
                if ticker not in TOP_30_TICKERS:
                    # Questo dovrebbe essere già filtrato dalla key_by, ma è un fallback
                    return # Non emettere nulla
                
                ts_str = data.get("timestamp")
                if not ts_str:
                    print(f"[ERROR] Missing timestamp in trade data: {data}", file=sys.stderr)
                    return # Non emettere nulla

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
                return # Non emettere nulla

            else:
                print(f"[WARN] Unrecognized data format in main job process_element: {value}", file=sys.stderr)
                return # Non emettere nulla

        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode JSON in main job process_element: {value}", file=sys.stderr)
            return # Non emettere nulla
        except Exception as e:
            print(f"[ERROR] process_element in main job: {e} for value: {value}", file=sys.stderr)
            return # Non emettere nulla
        finally:
            # Registra il timer per le predizioni ogni 10 secondi esatti (allineato al tempo di elaborazione)
            current_processing_time = ctx.timer_service().current_processing_time()
            # Calcola il prossimo timestamp che sia un multiplo di 10 secondi
            next_aligned_ts = (current_processing_time // 10000 + 1) * 10000
            
            last_timer = self.last_timer_state.value()
            
            # Se il timer non è stato registrato o se il prossimo timer allineato è nel futuro
            if last_timer is None or next_aligned_ts > last_timer:
                ctx.timer_service().register_processing_time_timer(next_aligned_ts)
                self.last_timer_state.update(next_aligned_ts)


    def on_timer(self, timestamp, ctx, out): # 'out' è necessario anche qui
        """Chiamata quando un timer registrato scatta."""
        try:
            now_utc = datetime.now(timezone.utc)
            ts_str = now_utc.isoformat()
            ticker = ctx.get_current_key()

            # Solo i ticker nella lista TOP_30_TICKERS dovrebbero attivare questo timer
            if ticker not in TOP_30_TICKERS:
                # print(f"[WARN] on_timer fired for unexpected key (not in TOP_30_TICKERS): {ticker}", file=sys.stderr)
                return [] # Non processare chiavi non pertinenti

            # Funzioni helper (rimangono le stesse)
            def mean(vals):
                vals = list(vals)
                return float(np.mean(vals)) if vals else 0.0

            def std(vals):
                vals = list(vals)
                return float(np.std(vals)) if vals and len(vals) > 1 else 0.0

            def total(vals):
                vals = list(vals)
                return float(np.sum(vals)) if vals else 0.0

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

            now_ny = now_utc.astimezone(NY_TZ)
            market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0, tzinfo=NY_TZ)
            market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=NY_TZ)
            
            is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5 # Lun-Ven

            is_simulated_prediction = False
            if is_market_hours:
                # Se il mercato è aperto, usa dati reali
                price_1m_values = self.real_price_1m.values()
                price_5m_values = self.real_price_5m.values()
                price_30m_values = self.real_price_30m.values()
                size_1m_values = self.real_size_1m.values()
                size_5m_values = self.real_size_5m.values()
                size_30m_values = self.real_size_30m.values()
                is_simulated_prediction = False
            else:
                # Se il mercato è chiuso, usa dati simulati per la predizione
                price_1m_values = self.fake_price_1m.values()
                price_5m_values = self.fake_price_5m.values()
                price_30m_values = self.fake_price_30m.values()
                size_1m_values = self.fake_size_1m.values()
                size_5m_values = self.fake_size_5m.values()
                size_30m_values = self.fake_size_30m.values()
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
                "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
                "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
                "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
                "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),
                # SENTIMENT GENERALE E MACRO DATA NON SONO PIÙ QUI
                # Questi campi saranno aggiunti dal Job "PredictionEnricher"
                # Non è necessario inserirli con valori placeholder qui.
                
                # NEW TIME-BASED FEATURES
                "minutes_since_open": int(minutes_since_open),
                "day_of_week": int(now_ny.weekday()),
                "day_of_month": int(now_ny.day),
                "week_of_year": int(now_ny.isocalendar()[1]),
                "month_of_year": int(now_ny.month),
                "market_open_spike_flag": int(market_open_spike_flag),
                "market_close_spike_flag": int(market_close_spike_flag),
                # Dati fondamentali
                # Anche questi dati fondamentali potrebbero essere passati al prossimo job
                # tramite un broadcast state o ricaricati lì, per ora li lasciamo qui
                # ma è una cosa da considerare per il secondo job.
                "eps": float(ticker_fundamentals["eps"]) if ticker_fundamentals.get("eps") is not None else None,
                "freeCashFlow": float(ticker_fundamentals["freeCashFlow"]) if ticker_fundamentals.get("freeCashFlow") is not None else None,
                "profit_margin": float(ticker_fundamentals["profit_margin"]) if ticker_fundamentals.get("profit_margin") is not None else None,
                "debt_to_equity": float(ticker_fundamentals["debt_to_equity"]) if ticker_fundamentals.get("debt_to_equity") is not None else None,
                # Flag per indicare se la predizione è basata su dati simulati
                "is_simulated_prediction": is_simulated_prediction
            }
            
            result = json.dumps(features)
            print(f"[FEATURES_READY] {ts_str} - {ticker} => {result}", file=sys.stderr)

            out.collect(result) # Invia le features al nuovo topic 'ticker_features_ready'

        except Exception as e:
            print(f"[ERROR] on_timer for ticker {ticker}: {e}", file=sys.stderr)
            # In caso di errore, possiamo comunque emettere un messaggio di errore per tracciabilità
            out.collect(json.dumps({"ticker": ctx.get_current_key(), "timestamp": datetime.now(timezone.utc).isoformat(), "error": str(e)}))
        finally:
            # Registra il prossimo timer per il 10 secondi successivo, allineato
            current_processing_time = ctx.timer_service().current_processing_time()
            next_aligned_ts = (current_processing_time // 10000 + 1) * 10000
            ctx.timer_service().register_processing_time_timer(next_aligned_ts)
            self.last_timer_state.update(next_aligned_ts) # Aggiorna lo stato dell'ultimo timer registrato


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
                return "discard_key" # Una chiave che verrà ignorata
        else:
            # Messaggi senza 'ticker' (es. macrodata) non dovrebbero arrivare qui
            # se i flussi sono separati correttamente.
            print(f"[WARN] Data with no 'ticker' field, discarding in main job: {json_str}", file=sys.stderr)
            return "discard_key" # Una chiave che verrà ignorata
    except json.JSONDecodeError:
        print(f"[WARN] Failed to decode JSON for key_by in main job: {json_str}", file=sys.stderr)
        return "invalid_json_key" # Una chiave che verrà ignorata
    except Exception as e:
        print(f"[ERROR] route_by_ticker in main job: {e} for {json_str}", file=sys.stderr)
        return "error_key" # Una chiave che verrà ignorata


def main():
    load_fundamental_data() # Carica i dati fondamentali una volta all'avvio del job

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Mantieni a 1 per il debug iniziale

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_feature_aggregator_group', # Nuovo group ID
        'auto.offset.reset': 'earliest'
    }

    # Il consumer per il Job principale legge dai topic con dati specifici per ticker
    consumer = FlinkKafkaConsumer(
        topics=["stock_trades", "news_sentiment", "bluesky_sentiment"], 
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    # Questo producer ora invia le features grezze al nuovo topic intermedio
    producer_features = FlinkKafkaProducer(
        topic='ticker_features_ready', # Nuovo topic di output per le features
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    # Espandiamo e filtriamo i sentiment (escludendo 'GENERAL')
    expanded_and_filtered_stream = stream.flat_map(expand_sentiment_data, output_type=Types.STRING())

    # Key by ticker
    keyed_stream = expanded_and_filtered_stream.key_by(route_by_ticker, key_type=Types.STRING())
    
    # Processa con SlidingAggregator
    # Filtriamo le chiavi di "scarto" dopo il key_by per evitare che l'operatore riceva eventi non validi
    # e per assicurarsi che i timer vengano registrati solo per i TOP_30_TICKERS
    valid_keyed_stream = keyed_stream.filter(lambda x: x[0] in TOP_30_TICKERS)
    
    processed_features = valid_keyed_stream.process(SlidingAggregator(), output_type=Types.STRING())
    
    processed_features.add_sink(producer_features)

    env.execute("Stock Feature Aggregation Job")

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
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, KeyedBroadcastProcessFunction, MapFunction
# from pyflink.datastream.functions import FlatMapFunction
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
# from minio import Minio
# from minio.error import S3Error

# TOP_30_TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]

# fundamentals_data = {}

# NY_TZ = pytz.timezone('America/New_York')

# # Broadcast State Descriptor
# GLOBAL_BROADCAST_DESCRIPTOR = MapStateDescriptor(
#     "global_data_state",
#     Types.STRING(),  # key: 'macro_data' or 'general_sentiment'
#     Types.MAP(Types.STRING(), Types.FLOAT())  # value: dict di valori float
# )

# MINIO_URL = "minio:9000"
# MINIO_ACCESS_KEY = "admin"
# MINIO_SECRET_KEY = "admin123"
# MINIO_SECURE = False

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
#                     eps = float(row.get("eps")) if "eps" in row and pd.notna(row.get("eps")) else None
#                     fcf = float(row.get("cashflow_freeCashFlow")) if "cashflow_freeCashFlow" in row and pd.notna(row.get("cashflow_freeCashFlow")) else None
#                     revenue = float(row.get("revenue")) if "revenue" in row and pd.notna(row.get("revenue")) else None
#                     net_income = float(row.get("netIncome")) if "netIncome" in row and pd.notna(row.get("netIncome")) else None
#                     debt = float(row.get("balance_totalDebt")) if "balance_totalDebt" in row and pd.notna(row.get("balance_totalDebt")) else None
#                     equity = float(row.get("balance_totalStockholdersEquity")) if "balance_totalStockholdersEquity" in row and pd.notna(row.get("balance_totalStockholdersEquity")) else None

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


# class ExpandSentimentFunction(FlatMapFunction):
#     """Funzione per espandere i dati di sentiment con liste di ticker."""
    
#     def flat_map(self, value):
#         try:
#             data = json.loads(value)

#             if "social" in data and "sentiment_score" in data and isinstance(data.get("ticker"), list):
#                 expanded_records = []
#                 original_ticker_list = data["ticker"]

#                 for ticker_item in original_ticker_list:
#                     if ticker_item != "GENERAL" and ticker_item in TOP_30_TICKERS:
#                         new_record = data.copy()
#                         new_record["ticker"] = ticker_item
#                         expanded_records.append(json.dumps(new_record))

#                 return expanded_records if expanded_records else []

#             if "ticker" in data and data["ticker"] == "GENERAL":
#                 return []

#             return [value] # Messaggi come trade o sentiment con singolo ticker

#         except Exception as e:
#             print(f"[ERROR] ExpandSentimentFunction: {e} per {value}", file=sys.stderr)
#             return []


# class TickerKeySelector(MapFunction):
#     """Funzione per estrarre la chiave ticker dai dati JSON."""
    
#     def map(self, value):
#         try:
#             data = json.loads(value)
#             if "ticker" in data and data["ticker"] in TOP_30_TICKERS:
#                 return (data["ticker"], value)
#             else:
#                 return ("DISCARD", value)
#         except Exception as e:
#             print(f"[ERROR] TickerKeySelector: {e} for {value}", file=sys.stderr)
#             return ("ERROR", value)


# class SlidingAggregatorBroadcast(KeyedBroadcastProcessFunction):
#     def open(self, runtime_context: RuntimeContext):
#         def descriptor(name):
#             return MapStateDescriptor(name, Types.STRING(), Types.FLOAT())

#         self.real_price_1m = runtime_context.get_map_state(descriptor("real_price_1m"))
#         self.real_price_5m = runtime_context.get_map_state(descriptor("real_price_5m"))
#         self.real_price_30m = runtime_context.get_map_state(descriptor("real_price_30m"))

#         self.real_size_1m = runtime_context.get_map_state(descriptor("real_size_1m"))
#         self.real_size_5m = runtime_context.get_map_state(descriptor("real_size_5m"))
#         self.real_size_30m = runtime_context.get_map_state(descriptor("real_size_30m"))

#         self.fake_price_1m = runtime_context.get_map_state(descriptor("fake_price_1m"))
#         self.fake_price_5m = runtime_context.get_map_state(descriptor("fake_price_5m"))
#         self.fake_price_30m = runtime_context.get_map_state(descriptor("fake_price_30m"))

#         self.fake_size_1m = runtime_context.get_map_state(descriptor("fake_size_1m"))
#         self.fake_size_5m = runtime_context.get_map_state(descriptor("fake_size_5m"))
#         self.fake_size_30m = runtime_context.get_map_state(descriptor("fake_size_30m"))

#         self.sentiment_bluesky_2h = runtime_context.get_map_state(descriptor("sentiment_bluesky_2h"))
#         self.sentiment_bluesky_1d = runtime_context.get_map_state(descriptor("sentiment_bluesky_1d"))
        
#         self.sentiment_news_1d = runtime_context.get_map_state(descriptor("sentiment_news_1d"))
#         self.sentiment_news_3d = runtime_context.get_map_state(descriptor("sentiment_news_3d"))

#         self.last_timer_state = runtime_context.get_state(ValueStateDescriptor("last_timer", Types.LONG()))

#     def process_element(self, value, ctx, out):
#         """Elabora ogni elemento in ingresso (stringa JSON) da Kafka."""
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
#             # Register a timer for predictions every 10 seconds, aligned to the clock
#             current_processing_time = ctx.timer_service().current_processing_time()
#             last_timer = self.last_timer_state.value()

#             # Calculate the next aligned timestamp (e.g., 13:10:00, 13:10:10, ...)
#             # Convert milliseconds to seconds, floor to the nearest 10-second mark, then add 10 seconds
#             # and convert back to milliseconds
#             next_aligned_timestamp = (current_processing_time // 10000 + 1) * 10000 
            
#             if last_timer is None or next_aligned_timestamp > last_timer:
#                 ctx.timer_service().register_processing_time_timer(next_aligned_timestamp)
#                 self.last_timer_state.update(next_aligned_timestamp)


#     def process_broadcast_element(self, value, ctx, out): # Aggiunto 'out'
#         """Processa gli elementi broadcast (dati globali)."""
#         try:
#             data = json.loads(value)
#             macro = data.get("macro_data", {})
#             sentiment = data.get("general_sentiment", {})

#             broadcast_state = ctx.get_broadcast_state(GLOBAL_BROADCAST_DESCRIPTOR)
            
#             if macro:
#                 cleaned_macro = {k: float(v) for k, v in macro.items()}
#                 broadcast_state.put("macro_data", cleaned_macro)
#                 print(f"[DEBUG] Updated macro_data in broadcast state: {cleaned_macro}", file=sys.stderr)
                
#             if sentiment:
#                 cleaned_sentiment = {k: float(v) for k, v in sentiment.items()}
#                 broadcast_state.put("general_sentiment", cleaned_sentiment)
#                 print(f"[DEBUG] Updated general_sentiment in broadcast state: {cleaned_sentiment}", file=sys.stderr)
                
#         except Exception as e:
#             print(f"[ERROR] process_broadcast_element: {e}", file=sys.stderr)
#         # Non devi chiamare out.collect() qui a meno che non voglia emettere l'elemento broadcast
#         # a valle, cosa che di solito non si fa per i dati broadcast.

#     def on_timer(self, timestamp, ctx, out):
#         """Chiamata quando un timer registrato scatta."""
#         try:
#             # Get the current time based on the timer's timestamp
#             now_utc = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
#             ts_str = now_utc.isoformat()
#             ticker = ctx.get_current_key()

#             if ticker not in TOP_30_TICKERS:
#                 return

#             # Helper functions
#             def mean(vals):
#                 vals = list(vals)
#                 return float(np.mean(vals)) if vals else 0.0

#             def std(vals):
#                 vals = list(vals)
#                 return float(np.std(vals)) if vals and len(vals) > 1 else 0.0

#             def total(vals):
#                 vals = list(vals)
#                 return float(np.sum(vals)) if vals else 0.0

#             # State cleanup function
#             def cleanup_state(state, window_minutes):
#                 threshold = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
#                 keys_to_remove = []
#                 for k in list(state.keys()):
#                     try:
#                         dt_obj = isoparse(k)
#                         if dt_obj.tzinfo is None:
#                             dt_obj = dt_obj.replace(tzinfo=timezone.utc)
#                         if dt_obj < threshold:
#                             keys_to_remove.append(k)
#                     except Exception:
#                         keys_to_remove.append(k) # Remove malformed timestamps too
#                 for k_remove in keys_to_remove:
#                     state.remove(k_remove)

#             # Apply cleanup to all relevant states
#             cleanup_state(self.real_price_1m, 1)
#             cleanup_state(self.real_price_5m, 5)
#             cleanup_state(self.real_price_30m, 30)
#             cleanup_state(self.real_size_1m, 1)
#             cleanup_state(self.real_size_5m, 5)
#             cleanup_state(self.real_size_30m, 30)
#             cleanup_state(self.fake_price_1m, 1)
#             cleanup_state(self.fake_price_5m, 5)
#             cleanup_state(self.fake_price_30m, 30)
#             cleanup_state(self.fake_size_1m, 1)
#             cleanup_state(self.fake_size_5m, 5)
#             cleanup_state(self.fake_size_30m, 30)
#             cleanup_state(self.sentiment_bluesky_2h, 2 * 60)
#             cleanup_state(self.sentiment_bluesky_1d, 24 * 60)
#             cleanup_state(self.sentiment_news_1d, 24 * 60)
#             cleanup_state(self.sentiment_news_3d, 3 * 24 * 60)

#             # Determine market hours
#             now_ny = now_utc.astimezone(NY_TZ)
#             market_open_time = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
#             market_close_time = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
#             is_market_hours = market_open_time <= now_ny < market_close_time and now_ny.weekday() < 5

#             is_simulated_prediction = False
#             if is_market_hours:
#                 price_1m_values = self.real_price_1m.values()
#                 price_5m_values = self.real_price_5m.values()
#                 price_30m_values = self.real_price_30m.values()
#                 size_1m_values = self.real_size_1m.values()
#                 size_5m_values = self.real_size_5m.values()
#                 size_30m_values = self.real_size_30m.values()
#             else:
#                 price_1m_values = self.fake_price_1m.values()
#                 price_5m_values = self.fake_price_5m.values()
#                 price_30m_values = self.fake_price_30m.values()
#                 size_1m_values = self.fake_size_1m.values()
#                 size_5m_values = self.fake_size_5m.values()
#                 size_30m_values = self.fake_size_30m.values()
#                 is_simulated_prediction = True

#             # Calculate spike flags
#             market_open_spike_flag = 0
#             market_close_spike_flag = 0

#             if market_open_time <= now_ny < (market_open_time + timedelta(minutes=5)):
#                 market_open_spike_flag = 1

#             if (market_close_time - timedelta(minutes=5)) <= now_ny < market_close_time:
#                 market_close_spike_flag = 1

#             # Calculate minutes_since_open
#             minutes_since_open = -1
#             if now_ny >= market_open_time and now_ny < market_close_time:
#                 minutes_since_open = int((now_ny - market_open_time).total_seconds() // 60)
#             else:
#                 if now_ny < market_open_time:
#                     minutes_until_open = int((market_open_time - now_ny).total_seconds() // 60)
#                     minutes_since_open = -minutes_until_open
#                 else:
#                     minutes_since_open = int((now_ny - market_close_time).total_seconds() // 60) + 390 # 390 minutes in trading day

#             # Access broadcast state
#             broadcast_state = ctx.get_broadcast_state(GLOBAL_BROADCAST_DESCRIPTOR)
#             macro_data_global = broadcast_state.get("macro_data") or {}
#             general_sentiment_global = broadcast_state.get("general_sentiment") or {
#                 "sentiment_bluesky_mean_general_2hours": 0.0,
#                 "sentiment_bluesky_mean_general_1d": 0.0
#             }

#             # Fundamental data
#             ticker_fundamentals = fundamentals_data.get(ticker, {})

#             # Construct features
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
#                 "sentiment_bluesky_mean_2h": mean(self.sentiment_bluesky_2h.values()),
#                 "sentiment_bluesky_mean_1d": mean(self.sentiment_bluesky_1d.values()),
#                 "sentiment_news_mean_1d": mean(self.sentiment_news_1d.values()),
#                 "sentiment_news_mean_3d": mean(self.sentiment_news_3d.values()),
#                 "sentiment_bluesky_mean_general_2hours": general_sentiment_global.get("sentiment_bluesky_mean_general_2hours", 0.0),
#                 "sentiment_bluesky_mean_general_1d": general_sentiment_global.get("sentiment_bluesky_mean_general_1d", 0.0),
#                 "minutes_since_open": int(minutes_since_open),
#                 "day_of_week": int(now_ny.weekday()),
#                 "day_of_month": int(now_ny.day),
#                 "week_of_year": int(now_ny.isocalendar()[1]),
#                 "month_of_year": int(now_ny.month),
#                 "market_open_spike_flag": int(market_open_spike_flag),
#                 "market_close_spike_flag": int(market_close_spike_flag),
#                 "eps": float(ticker_fundamentals.get("eps", 0.0)) if ticker_fundamentals.get("eps") is not None else 0.0,
#                 "freeCashFlow": float(ticker_fundamentals.get("freeCashFlow", 0.0)) if ticker_fundamentals.get("freeCashFlow") is not None else 0.0,
#                 "profit_margin": float(ticker_fundamentals.get("profit_margin", 0.0)) if ticker_fundamentals.get("profit_margin") is not None else 0.0,
#                 "debt_to_equity": float(ticker_fundamentals.get("debt_to_equity", 0.0)) if ticker_fundamentals.get("debt_to_equity") is not None else 0.0,
#                 "is_simulated_prediction": is_simulated_prediction
#             }

#             # Add macro data
#             for macro_key_alias, macro_value in macro_data_global.items():
#                 features[macro_key_alias] = float(macro_value)

#             result = json.dumps(features)
#             print(f"[PREDICTION] {ts_str} - {ticker} => prediction generated", file=sys.stderr)
#             out.collect(result)

#         except Exception as e:
#             print(f"[ERROR] on_timer for ticker {ctx.get_current_key()}: {e}", file=sys.stderr)
#             error_result = json.dumps({
#                 "ticker": ctx.get_current_key(), 
#                 "timestamp": datetime.now(timezone.utc).isoformat(), 
#                 "error": str(e)
#             })
#             out.collect(error_result)
#         finally:
#             # Re-register the timer for the *next* 10-second interval
#             current_processing_time = ctx.timer_service().current_processing_time()
#             next_aligned_timestamp = (current_processing_time // 10000 + 1) * 10000 
#             ctx.timer_service().register_processing_time_timer(next_aligned_timestamp)
#             self.last_timer_state.update(next_aligned_timestamp) # Update last_timer_state here as well


# def main():
#     load_fundamental_data()

#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     kafka_props = {
#         'bootstrap.servers': 'kafka:9092',
#         'group.id': 'flink_stock_main_job_group',
#         'auto.offset.reset': 'earliest'
#     }

#     # Stream per ticker data (trades + sentiment)
#     ticker_consumer = FlinkKafkaConsumer(
#         topics=["stock_trades", "bluesky_sentiment"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )
#     ticker_stream = env.add_source(ticker_consumer, type_info=Types.STRING())
    
#     # Espandi sentiment data e filtra
#     expanded_stream = ticker_stream.flat_map(ExpandSentimentFunction(), output_type=Types.STRING())
    
#     # Filtra solo i ticker validi
#     filtered_stream = expanded_stream.filter(
#         lambda x: json.loads(x).get("ticker") in TOP_30_TICKERS if x else False
#     )
    
#     # Key by ticker
#     keyed_stream = filtered_stream.key_by(
#         lambda x: json.loads(x).get("ticker", "UNKNOWN"), 
#         key_type=Types.STRING()
#     )

#     # Stream per dati globali (broadcast)
#     global_consumer = FlinkKafkaConsumer(
#         topics=["global_data"],
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )
#     global_stream = env.add_source(global_consumer, type_info=Types.STRING())

#     # Specificare type_info anche per il broadcast (se non è già implicito)
#     broadcast_stream = global_stream.broadcast(GLOBAL_BROADCAST_DESCRIPTOR)

#     # Connetti keyed stream con broadcast stream
#     connected_stream = keyed_stream.connect(broadcast_stream)
#     processed = connected_stream.process(
#         SlidingAggregatorBroadcast(), 
#         output_type=Types.STRING()
#     )

#     # Output sink
#     producer = FlinkKafkaProducer(
#         topic='aggregated_data',
#         serialization_schema=SimpleStringSchema(),
#         producer_config={'bootstrap.servers': 'kafka:9092'}
#     )
#     processed.add_sink(producer)

#     env.execute("Main Job with Broadcast State")

# if __name__ == "__main__":
#     main()























