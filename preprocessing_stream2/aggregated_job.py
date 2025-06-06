# import json
# import sys
# import time
# from kafka.admin import KafkaAdminClient
# from kafka.errors import NoBrokersAvailable, KafkaError

# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.common.typeinfo import Types
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
# from pyflink.datastream.functions import BroadcastProcessFunction
# from pyflink.datastream.state import MapStateDescriptor


# # === 🔄 Aspetta che Kafka sia disponibile e abbia i topic richiesti ===

# def wait_for_kafka_topics(required_topics=("main_data", "global_data"), timeout=5):
#     """
#     Attende che il broker Kafka sia disponibile e che tutti i topic necessari esistano.
#     """
#     while True:
#         try:
#             admin = KafkaAdminClient(bootstrap_servers="kafka:9092")
#             topics = admin.list_topics()
#             if all(topic in topics for topic in required_topics):
#                 print("✅ Kafka è pronto con tutti i topic richiesti.", file=sys.stderr)
#                 return
#             else:
#                 print(f"⏳ Aspettando i topic: {required_topics}... (presenti: {topics})", file=sys.stderr)
#         except (NoBrokersAvailable, KafkaError) as e:
#             print(f"⏳ Kafka non disponibile, ritento tra {timeout}s... ({e})", file=sys.stderr)
#         time.sleep(timeout)

# wait_for_kafka_topics()

# # === ⚙️ Definizione del MapStateDescriptor (ORA GLOBALE) ===
# # Spostato fuori dalla classe per evitare l'istanza anticipata
# broadcast_state_desc = MapStateDescriptor(
#     "global_broadcast_state", Types.STRING(), Types.STRING()
# )

# # === ⚙️ Funzione di merge con stato broadcast ===

# class MergeWithGlobalBroadcast(BroadcastProcessFunction):
#     """
#     Questa funzione Processa il flusso principale ('main_data') e lo unisce con i dati
#     più recenti ricevuti dal flusso broadcast ('global_data').
#     """

#     def process_element(self, value: str, ctx: 'BroadcastProcessFunction.ReadOnlyContext', out):
#         """
#         Processa gli elementi dal flusso principale (main_data).
#         """
#         print(f"[DEBUG_INVOKE] Calling process_element for main stream with value: {value[:50]}...", file=sys.stderr)
#         try:
#             main_data = json.loads(value)
#             print(f"[DEBUG] Parsed main_data: {main_data}", file=sys.stderr)
#             ticker = main_data.get("ticker", "N/A")
#             print(f"[DEBUG] Main data ticker: {ticker}", file=sys.stderr)

#             # Accede al descrittore di stato globale
#             broadcast_state = ctx.get_broadcast_state(broadcast_state_desc)
#             global_str = broadcast_state.get("latest")
#             print(f"[DEBUG] Retrieved global_str from state: {global_str[:50] if global_str else 'None'}", file=sys.stderr)
            
#             if global_str:
#                 global_data = json.loads(global_str)
#                 print(f"[DEBUG] Parsed global_data: {global_data}", file=sys.stderr)
#                 merged = {**main_data, **global_data}
#                 merged_json = json.dumps(merged)
#                 out.collect(merged_json)
#                 print(f"[INFO] ✅ Merged data for ticker {ticker} sent to final_predictions", file=sys.stderr)
#             else:
#                 print(f"[WARN] ❌ No global data available for ticker {ticker} - emitting main_data only", file=sys.stderr)
#                 out.collect(value)
                
#         except json.JSONDecodeError as e:
#             print(f"[ERROR] 💥 Invalid JSON in main_data: {e} - Data: {value}", file=sys.stderr)
#             out.collect(value)

#         except Exception as e:
#             print(f"[ERROR] 💥 process_element unexpected error: {e}", file=sys.stderr)
#             try:
#                 out.collect(value)
#             except Exception as inner_e:
#                 print(f"[ERROR] Failed to emit original value after error: {inner_e}", file=sys.stderr)


#     def process_broadcast_element(self, value: str, ctx: 'BroadcastProcessFunction.Context'):
#         """
#         Processa gli elementi dal flusso broadcast (global_data).
#         """
#         print(f"[DEBUG_INVOKE] Calling process_broadcast_element for broadcast stream with value: {value[:50]}...", file=sys.stderr)
#         try:
#             print(f"[DEBUG] 🌍 Processing broadcast element: {value[:100]}...", file=sys.stderr)
            
#             json.loads(value)
            
#             # Accede al descrittore di stato globale
#             broadcast_state = ctx.get_broadcast_state(broadcast_state_desc)
#             broadcast_state.put("latest", value)
#             print(f"[INFO] 🌍 Updated global broadcast state successfully", file=sys.stderr)
#         except json.JSONDecodeError as e:
#             print(f"[ERROR] 💥 Invalid JSON in global_data received for broadcast: {e} - Data: {value}", file=sys.stderr)
#         except Exception as e:
#             print(f"[ERROR] 💥 process_broadcast_element unexpected error: {e}", file=sys.stderr)


# # === 🚀 Flink Streaming Job ===

# def main():
#     """
#     Funzione principale che definisce e esegue il job Flink.
#     """
#     env = StreamExecutionEnvironment.get_execution_environment()
#     env.set_parallelism(1)

#     kafka_props = {
#         "bootstrap.servers": "kafka:9092",
#         "auto.offset.reset": "earliest",
#         "group.id": "merge_job_group",
#         "fetch.min.bytes": "1024",
#         "fetch.max.wait.ms": "500"
#     }

#     print(f"[INFO] 🚀 Starting Broadcast Merge Job", file=sys.stderr)
#     print(f"[CONFIG] Kafka bootstrap.servers = {kafka_props['bootstrap.servers']}", file=sys.stderr)
#     print(f"[CONFIG] Parallelism = {env.get_parallelism()}", file=sys.stderr)

#     # --- Kafka sources ---
#     main_consumer = FlinkKafkaConsumer(
#         topics="main_data",
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )

#     global_consumer = FlinkKafkaConsumer(
#         topics="global_data",
#         deserialization_schema=SimpleStringSchema(),
#         properties=kafka_props
#     )

#     # --- Kafka sink ---
#     producer = FlinkKafkaProducer(
#         topic="final_predictions",
#         serialization_schema=SimpleStringSchema(),
#         producer_config={
#             "bootstrap.servers": "kafka:9092",
#             "batch.size": "16384",
#             "linger.ms": "5",
#             "compression.type": "snappy"
#         }
#     )

#     # --- Stream definitions ---
#     main_stream = env.add_source(main_consumer, type_info=Types.STRING())
#     global_stream = env.add_source(global_consumer, type_info=Types.STRING())

#     broadcast_global_stream = global_stream.broadcast(broadcast_state_desc)

#     connected_stream = main_stream.connect(broadcast_global_stream)

#     merged_stream = connected_stream.process(
#         MergeWithGlobalBroadcast(), 
#         output_type=Types.STRING()
#     )

#     merged_stream.add_sink(producer)

#     env.execute("Broadcast Merge Job: Join Predictions with Global Broadcast Data")

# if __name__ == "__main__":
#     main()







































import sys
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types

# --- VARIABILI GLOBALI ---
global_data_dict = {}


# Ordine dei campi nell'output JSON
OUTPUT_FIELD_ORDER = [
    "ticker",
    "timestamp",
    "price_mean_1min",
    "price_mean_5min",
    "price_std_5min",
    "price_mean_30min",
    "price_std_30min",
    "size_tot_1min",
    "size_tot_5min",
    "size_tot_30min",
    "sentiment_bluesky_mean_2hours",
    "sentiment_bluesky_mean_1day",
    "sentiment_news_mean_1day",
    "sentiment_news_mean_3days",
    "sentiment_general_bluesky_mean_2hours",
    "sentiment_general_bluesky_mean_1day",
    "minutes_since_open",
    "day_of_week",
    "day_of_month",
    "week_of_year",
    "month_of_year",
    "market_open_spike_flag",
    "market_close_spike_flag",
    "eps",
    "free_cash_flow",
    "profit_margin",
    "debt_to_equity",
    "gdp_real",
    "cpi",
    "ffr",
    "t10y",
    "t2y",
    "spread_10y_2y",
    "unemployment"
]

# Chiavi per la route
GLOBAL_DATA_KEY = "global_data_key_for_join"
MAIN_DATA_KEY = "main_data_key_for_join"

class SlidingAggregator(KeyedProcessFunction):
    def process_element(self, value, ctx):
        global global_data_dict

        try:
            data = json.loads(value)
            current_key = ctx.get_current_key()
            
            # --- Gestione dei Dati Globali (GLOBAL_DATA_KEY) ---
            if current_key == GLOBAL_DATA_KEY:
                if isinstance(data, dict):
                    # Inizializza i campi di sentiment generale a 0.0 prima di aggiornarli
                    # Questo assicura che se un campo non è presente nel payload, sia comunque 0.0
                    general_sentiment_fields = [
                        "sentiment_general_bluesky_mean_2hours",
                        "sentiment_general_bluesky_mean_1day"
                    ]
                    for field in general_sentiment_fields:
                        global_data_dict[field] = 0.0

                    for k, v in data.items():
                        try:
                            if k == "timestamp":
                                continue
                            global_data_dict[k] = float(v) if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '', 1).isdigit()) else v
                        except ValueError:
                            global_data_dict[k] = v
                    print(f"[GLOBAL-DATA-HANDLER] Aggiornato global_data_dict: {global_data_dict}", file=sys.stderr)
                else:
                    print(f"[WARN] Messaggio global_data non è un dict valido: {value}", file=sys.stderr)
                return [] 

            # --- Gestione dei Dati Principali (MAIN_DATA_KEY) ---
            elif current_key == MAIN_DATA_KEY:
                ticker = data.get("ticker")
                # Non dovrebbe essere necessario il controllo TOP_30_TICKERS qui
                # se il primo job e la route_by_ticker sono configurati correttamente.
                # Tuttavia, lo manteniamo per sicurezza.
                # if ticker not in TOP_30_TICKERS:
                #     print(f"[WARN] Ticker '{ticker}' non nei TOP_30_TICKERS. Ignoro.", file=sys.stderr)
                #     return []
                
                # Pre-popola merged_data con i valori di default per tutti i campi dall'inizio
                merged_data = {}
                for field in OUTPUT_FIELD_ORDER:
                    if field in ["ticker", "timestamp"]:
                        merged_data[field] = None # Valori che possono essere stringhe o None
                    else:
                        merged_data[field] = 0.0 # Tutti gli altri campi numerici defaultano a 0.0

                # Sovrascrivi con i dati globali (macro e sentiment generale)
                for k, v in global_data_dict.items():
                    if isinstance(v, (int, float)):
                        merged_data[k] = float(v)
                    elif isinstance(v, str) and v.replace('.', '', 1).isdigit():
                        merged_data[k] = float(v)
                    else:
                        merged_data[k] = v # Mantiene stringhe o altri tipi

                # Sovrascrivi con i dati specifici del ticker (trade e sentiment specifico)
                for k, v in data.items():
                    # ATTENZIONE: i nomi dei campi sentiment specifici nel main_data sono "2h", "1d", "3d"
                    # Dobbiamo mapparli ai nomi lunghi di OUTPUT_FIELD_ORDER qui, prima dell'assegnazione
                    mapped_k = k
                    if k == "sentiment_bluesky_mean_2hours":
                        mapped_k = "sentiment_bluesky_mean_2hours"
                    elif k == "sentiment_bluesky_mean_1day":
                        mapped_k = "sentiment_bluesky_mean_1day"
                    elif k == "sentiment_news_mean_1day":
                        mapped_k = "sentiment_news_mean_1day"
                    elif k == "sentiment_news_mean_3days":
                        mapped_k = "sentiment_news_mean_3days"

                    if mapped_k in merged_data: # Assicurati che il campo sia nell'OUTPUT_FIELD_ORDER
                        if isinstance(v, (int, float)):
                            merged_data[mapped_k] = float(v)
                        elif isinstance(v, str) and v.replace('.', '', 1).isdigit():
                            merged_data[mapped_k] = float(v)
                        else:
                            merged_data[mapped_k] = v # Mantiene stringhe o altri tipi (es. None dal primo job)
                
                # Costruisci il dizionario finale nell'ordine desiderato
                result_data = {}
                for field in OUTPUT_FIELD_ORDER:
                    # Usa .get() per recuperare il valore; se il campo non è in merged_data (improbabile ora)
                    # o se il suo valore è None, verrà recuperato correttamente.
                    result_data[field] = merged_data.get(field)
                
                result_json = json.dumps(result_data)
                print(f"[MAIN-DATA-PROCESS] {ticker} - Dati combinati e ordinati: {result_json}", file=sys.stderr)
                yield result_json

        except json.JSONDecodeError:
            print(f"[ERROR] Impossibile decodificare JSON: {value}", file=sys.stderr)
            return []
        except Exception as e:
            print(f"[ERROR] Errore in process_element: {e} per valore: {value}", file=sys.stderr)
            return [json.dumps({"error": str(e), "original_message": value})]

def route_by_ticker(json_str):
    """Determina la chiave per i dati JSON in ingresso."""
    try:
        data = json.loads(json_str)

        if "gdp_real" in data or ("sentiment_score" in data and data.get("ticker") == "GENERAL"):
            # Se è un macro dato O un sentiment generale
            return GLOBAL_DATA_KEY     
        elif "ticker" in data: # Se ha un campo ticker, è un dato ticker-specifico
            # Questa condizione include sia i trade che i sentiment specifici per ticker
            # Non è più necessario filtrare TOP_30_TICKERS qui, lo fa la process_element se necessario.
            return MAIN_DATA_KEY
        else:
            print(f"[WARN] Dati non riconosciuti o senza ticker: {json_str}", file=sys.stderr)
            return "discard_key"

    except json.JSONDecodeError:
        print(f"[WARN] Impossibile decodificare JSON per key_by: {json_str}", file=sys.stderr)
        return "invalid_json_key"
    except Exception as e:
        print(f"[ERROR] Errore in route_by_ticker: {e} per {json_str}", file=sys.stderr)
        return "error_key"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics=["main_data", "global_data"], # Consumerà da entrambi i topic
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    producer = FlinkKafkaProducer(
        topic='aggregated_data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(consumer, type_info=Types.STRING())
    
    keyed = stream.key_by(route_by_ticker, key_type=Types.STRING())
    
    processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())
    
    processed.add_sink(producer)

    env.execute("Global Data Join (Parallelism 1)")

if __name__ == "__main__":
    main()