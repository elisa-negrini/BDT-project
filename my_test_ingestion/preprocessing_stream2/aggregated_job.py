import os
import sys
import json
import numpy as np
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
import pytz
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, MapFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor

# Costanti
TOP_30_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "ORCL", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]

GLOBAL_DATA_KEY = "global_context_key"
NY_TZ = pytz.timezone('America/New_York')


class PredictionEnricher(KeyedProcessFunction):
    """
    Arricchisce le predizioni con i dati globali più recenti.
    Ogni istanza mantiene l'ultimo stato globale ricevuto e lo applica
    a tutte le predizioni che arrivano per il ticker corrispondente.
    """
    
    def open(self, runtime_context: RuntimeContext):
        # Stato per memorizzare l'ultimo snapshot dei dati globali
        self.global_data_state = runtime_context.get_state(
            ValueStateDescriptor("latest_global_data", Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))
        )
        
    def process_element(self, value, ctx, out):
        """
        Processa elementi in ingresso che possono essere:
        1. Dati globali (macro + sentiment generale) - aggiorna lo stato
        2. Features per ticker - arricchisce con dati globali e emette
        """
        try:
            data = json.loads(value)
            current_key = ctx.get_current_key()
            
            # --- Gestione aggiornamento dati globali ---
            if "data_type" in data and data["data_type"] == "global":
                # Messaggio di aggiornamento dei dati globali
                # Questi messaggi arrivano a tutte le partizioni
                
                macro_data = data.get("macro_data", {})
                general_sentiment = data.get("general_sentiment", {})
                timestamp = data.get("timestamp")
                
                # Aggiorna lo stato globale locale
                global_snapshot = {
                    "macro_data": macro_data,
                    "general_sentiment": general_sentiment,
                    "last_updated": timestamp
                }
                
                self.global_data_state.update(global_snapshot)
                print(f"[ENRICHER-{current_key}] Updated global data at {timestamp}", file=sys.stderr)
                
                # Non emettere nulla per gli aggiornamenti globali
                return
                
            # --- Gestione arricchimento features ticker ---
            elif "ticker" in data and data["ticker"] in TOP_30_TICKERS:
                # Messaggio con features da arricchire
                ticker = data["ticker"]
                
                # Recupera l'ultimo stato globale
                global_snapshot = self.global_data_state.value()
                
                if global_snapshot is None:
                    # Se non abbiamo ancora dati globali, usa valori di default
                    print(f"[WARN-ENRICHER] No global data available yet for {ticker}, using defaults", file=sys.stderr)
                    global_snapshot = {
                        "macro_data": {},
                        "general_sentiment": {
                            "sentiment_bluesky_mean_general_2hours": 0.0,
                            "sentiment_bluesky_mean_general_1d": 0.0
                        },
                        "last_updated": datetime.now(timezone.utc).isoformat()
                    }
                
                # Crea il messaggio arricchito
                enriched_data = data.copy()
                
                # Aggiungi i dati macro
                macro_data = global_snapshot.get("macro_data", {})
                enriched_data.update({
                    "gdp_real": float(macro_data.get("gdp_real", 0.0)),
                    "cpi": float(macro_data.get("cpi", 0.0)),
                    "ffr": float(macro_data.get("ffr", 0.0)),
                    "t10y": float(macro_data.get("t10y", 0.0)),
                    "t2y": float(macro_data.get("t2y", 0.0)),
                    "spread_10y_2y": float(macro_data.get("spread_10y_2y", 0.0)),
                    "unemployment": float(macro_data.get("unemployment", 0.0))
                })
                
                # Aggiungi il sentiment generale
                general_sentiment = global_snapshot.get("general_sentiment", {})
                enriched_data.update({
                    "sentiment_bluesky_mean_general_2hours": float(general_sentiment.get("sentiment_bluesky_mean_general_2hours", 0.0)),
                    "sentiment_bluesky_mean_general_1d": float(general_sentiment.get("sentiment_bluesky_mean_general_1d", 0.0))
                })
                
                # Aggiungi metadati sull'arricchimento
                enriched_data["global_data_timestamp"] = global_snapshot.get("last_updated")
                enriched_data["enrichment_timestamp"] = datetime.now(timezone.utc).isoformat()
                
                result = json.dumps(enriched_data)
                print(f"[ENRICHED] {ticker} => Features enriched with global data from {global_snapshot.get('last_updated')}", file=sys.stderr)
                
                out.collect(result)
                
            else:
                print(f"[WARN-ENRICHER] Unexpected data format or ticker not in TOP_30: {value}", file=sys.stderr)
                
        except json.JSONDecodeError:
            print(f"[ERROR-ENRICHER] Failed to decode JSON: {value}", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR-ENRICHER] process_element: {e} for value: {value}", file=sys.stderr)


class GlobalDataForwarder(MapFunction):
    """
    Trasforma i dati globali aggiungendo un marker per identificarli
    e li duplica per essere inviati a tutte le partizioni dei ticker.
    """
    
    def map(self, value):
        try:
            data = json.loads(value)
            
            # Aggiungi un marker per identificare i dati globali
            forwarded_data = {
                "data_type": "global",
                "macro_data": data.get("macro_data", {}),
                "general_sentiment": data.get("general_sentiment", {}),
                "timestamp": data.get("timestamp", datetime.now(timezone.utc).isoformat())
            }
            
            return json.dumps(forwarded_data)
            
        except json.JSONDecodeError:
            print(f"[ERROR-FORWARDER] Failed to decode JSON: {value}", file=sys.stderr)
            return value
        except Exception as e:
            print(f"[ERROR-FORWARDER] map: {e} for value: {value}", file=sys.stderr)
            return value


def route_enricher_input(json_str):
    """
    Determina la chiave di routing per i messaggi in ingresso:
    - Dati globali: vengono distribuiti a tutte le chiavi dei ticker
    - Features ticker: vengono instradate al ticker specifico
    """
    try:
        data = json.loads(json_str)
        
        if "data_type" in data and data["data_type"] == "global":
            # Per i dati globali, dobbiamo scegliere una strategia di distribuzione
            # Opzione 1: Inviare a una chiave speciale che poi redistribuisce
            # Opzione 2: Inviare a tutti i ticker (ma questo richiederebbe broadcast)
            # 
            # Useremo l'approccio di inviarli a tutti i ticker tramite una funzione che
            # duplica il messaggio per ogni ticker
            return "BROADCAST_GLOBAL"  # Chiave speciale per i dati globali
            
        elif "ticker" in data and data["ticker"] in TOP_30_TICKERS:
            # Features specifiche per ticker
            return data["ticker"]
            
        else:
            print(f"[WARN-ROUTER] Unexpected data or ticker not in TOP_30: {json_str}", file=sys.stderr)
            return "discard_key"
            
    except json.JSONDecodeError:
        print(f"[WARN-ROUTER] Failed to decode JSON: {json_str}", file=sys.stderr)
        return "invalid_json_key"
    except Exception as e:
        print(f"[ERROR-ROUTER] route_enricher_input: {e} for {json_str}", file=sys.stderr)
        return "error_key"


class GlobalDataBroadcaster(KeyedProcessFunction):
    """
    Operatore speciale che riceve i dati globali e li redistribuisce
    a tutte le partizioni dei ticker per sincronizzare lo stato.
    """
    
    def process_element(self, value, ctx, out):
        try:
            data = json.loads(value)
            
            if "data_type" in data and data["data_type"] == "global":
                # Emetti il messaggio globale per ogni ticker
                # In questo modo ogni partizione di ticker riceverà l'aggiornamento
                for ticker in TOP_30_TICKERS:
                    keyed_global_data = data.copy()
                    keyed_global_data["target_ticker"] = ticker
                    out.collect(json.dumps(keyed_global_data))
                    
                print(f"[BROADCASTER] Distributed global data to all {len(TOP_30_TICKERS)} tickers", file=sys.stderr)
            
        except Exception as e:
            print(f"[ERROR-BROADCASTER] process_element: {e} for value: {value}", file=sys.stderr)


def route_after_broadcast(json_str):
    """Route i dati dopo il broadcast ai ticker appropriati."""
    try:
        data = json.loads(json_str)
        
        if "target_ticker" in data:
            return data["target_ticker"]
        elif "ticker" in data and data["ticker"] in TOP_30_TICKERS:
            return data["ticker"]
        else:
            return "discard_key"
            
    except Exception as e:
        print(f"[ERROR-ROUTE-AFTER] route_after_broadcast: {e} for {json_str}", file=sys.stderr)
        return "error_key"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)  # Parallelizzabile per gestire più ticker contemporaneamente

    consumer_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_prediction_enricher_group',
        'auto.offset.reset': 'earliest'
    }

    # Consumer per le features dei ticker dal job principale
    ticker_features_consumer = FlinkKafkaConsumer(
        topics=["ticker_features_ready"],
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    # Consumer per i dati globali dal job secondario
    global_data_consumer = FlinkKafkaConsumer(
        topics=["global_data"],
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    # Producer per i dati aggregati finali
    aggregated_producer = FlinkKafkaProducer(
        topic='aggregated_data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    # Stream delle features dei ticker
    ticker_features_stream = env.add_source(ticker_features_consumer, type_info=Types.STRING())
    
    # Stream dei dati globali
    global_data_stream = env.add_source(global_data_consumer, type_info=Types.STRING())
    
    # Trasforma i dati globali aggiungendo il marker
    marked_global_stream = global_data_stream.map(GlobalDataForwarder(), output_type=Types.STRING())
    
    # Prima fase: broadcast dei dati globali
    global_keyed = marked_global_stream.key_by(route_enricher_input, key_type=Types.STRING())
    broadcasted_global = global_keyed.process(GlobalDataBroadcaster(), output_type=Types.STRING())
    
    # Unisce i due stream: features ticker + dati globali broadcastati
    combined_stream = ticker_features_stream.union(broadcasted_global)
    
    # Seconda fase: routing finale e arricchimento
    final_keyed = combined_stream.key_by(route_after_broadcast, key_type=Types.STRING())
    
    # Filtra solo i ticker validi prima dell'arricchimento
    valid_stream = final_keyed.filter(lambda x: route_after_broadcast(x) in TOP_30_TICKERS)
    
    # Arricchisce le features con i dati globali
    enriched_stream = valid_stream.process(PredictionEnricher(), output_type=Types.STRING())
    
    # Invia i dati arricchiti al topic finale
    enriched_stream.add_sink(aggregated_producer)

    env.execute("Prediction Enricher Job")


if __name__ == "__main__":
    main()