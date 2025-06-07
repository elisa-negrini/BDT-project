import sys
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
import time

# --- GLOBAL VARIABLES ---
global_data_dict = {}
global_data_received = False  # Flag to track if global data has been received

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
AGGREGATED_TOPIC = 'aggregated_data'
NUM_PARTITIONS = 2  # Desired number of partitions for the aggregated topic

# Order of fields in the output JSON
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
    "unemployment",
    "is_simulated_prediction"
]

# Keys for routing data streams
GLOBAL_DATA_KEY = "global_data_key_for_join"
MAIN_DATA_KEY = "main_data_key_for_join"

def wait_for_kafka():
    """
    Waits until Kafka is available and responsive before proceeding.
    """
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id='kafka_health_check',
                request_timeout_ms=5000 # Set a reasonable timeout for connection attempts
            )

            # Attempt to list existing topics to confirm connectivity
            topics = admin_client.list_topics()
            admin_client.close()

            print(f"[INFO] Kafka is available! Found {len(topics)} topics.", file=sys.stderr)
            return True

        except Exception as e:
            print(f"[INFO] Kafka not yet available: {e}. Retrying in 5 seconds...", file=sys.stderr)
            time.sleep(5)

def create_kafka_topic_with_partitions():
    """
    Creates the Kafka topic with the specified number of partitions.
    Checks if the topic already exists to avoid errors.
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='flink_admin_client'
        )

        # Check if the topic already exists
        existing_topics = admin_client.list_topics()
        if AGGREGATED_TOPIC in existing_topics:
            print(f"[INFO] Topic '{AGGREGATED_TOPIC}' already exists.", file=sys.stderr)
        else:
            # Create the new topic with desired partitions
            topic = NewTopic(
                name=AGGREGATED_TOPIC,
                num_partitions=NUM_PARTITIONS,
                replication_factor=1  # Adjust according to your cluster configuration
            )

            admin_client.create_topics([topic])
            print(f"[INFO] Topic '{AGGREGATED_TOPIC}' created with {NUM_PARTITIONS} partitions.", file=sys.stderr)

        admin_client.close()

    except Exception as e:
        print(f"[ERROR] Error creating topic: {e}", file=sys.stderr)

def get_partition_for_ticker(ticker):
    """
    Determines the Kafka partition based on the ticker using hashing.
    """
    if not ticker:
        return 0
    return hash(ticker) % NUM_PARTITIONS

class SlidingAggregator(KeyedProcessFunction):
    """
    A Flink KeyedProcessFunction that processes incoming data streams.
    It merges global macroeconomic and general sentiment data with
    ticker-specific data based on predefined keys.
    """
    def process_element(self, value, ctx):
        global global_data_dict, global_data_received

        try:
            data = json.loads(value)
            current_key = ctx.get_current_key()

            # --- Handle Global Data (GLOBAL_DATA_KEY) ---
            if current_key == GLOBAL_DATA_KEY:
                if isinstance(data, dict):
                    # Initialize general sentiment fields in global data
                    general_sentiment_fields = [
                        "sentiment_general_bluesky_mean_2hours",
                        "sentiment_general_bluesky_mean_1day"
                    ]
                    for field in general_sentiment_fields:
                        global_data_dict[field] = 0.0

                    # Update global_data_dict with received global values
                    for k, v in data.items():
                        if k == "timestamp":
                            continue

                        # Handle boolean, numeric, and string values for global data
                        if isinstance(v, bool):
                            global_data_dict[k] = v
                        elif isinstance(v, (int, float)):
                            global_data_dict[k] = float(v)
                        elif isinstance(v, str) and v.replace('.', '', 1).isdigit(): # Check if string is a valid number
                            global_data_dict[k] = float(v)
                        else:
                            global_data_dict[k] = v

                    # Mark that global data has been received
                    global_data_received = True
                    print(f"[GLOBAL-DATA-HANDLER] Updated global_data_dict: {global_data_dict}", file=sys.stderr)
                else:
                    print(f"[WARN] Global data message is not a valid dictionary: {value}", file=sys.stderr)
                return [] # Global data messages do not produce direct output for aggregation

            # --- Handle Main Data (MAIN_DATA_KEY) ---
            elif current_key == MAIN_DATA_KEY:
                # Ensure global data is received before processing main data
                if not global_data_received:
                    print(f"[WAIT] Waiting for global data before processing main data. Message ignored: {data.get('ticker', 'unknown')}", file=sys.stderr)
                    return [] # Discard message if global data is not ready

                ticker = data.get("ticker")

                # Initialize merged_data with default values based on OUTPUT_FIELD_ORDER
                merged_data = {}
                for field in OUTPUT_FIELD_ORDER:
                    if field in ["ticker", "timestamp"]:
                        merged_data[field] = None # Will be filled by ticker-specific data
                    elif field == "is_simulated_prediction":
                        merged_data[field] = False # Default boolean value
                    else:
                        merged_data[field] = 0.0 # Default numeric value

                # Overwrite with global data (macro and general sentiment)
                for k, v in global_data_dict.items():
                    if isinstance(v, bool):
                        merged_data[k] = v
                    elif isinstance(v, (int, float)):
                        merged_data[k] = float(v)
                    elif isinstance(v, str) and v.replace('.', '', 1).isdigit(): # Check if string is a valid number
                        merged_data[k] = float(v)
                    else:
                        merged_data[k] = v

                # Overwrite with ticker-specific data (trade metrics and specific sentiment)
                for k, v in data.items():
                    # Direct mapping for sentiment fields (as they might have different names in source)
                    mapped_k = k
                    if k == "sentiment_bluesky_mean_2hours":
                        mapped_k = "sentiment_bluesky_mean_2hours"
                    elif k == "sentiment_bluesky_mean_1day":
                        mapped_k = "sentiment_bluesky_mean_1day"
                    elif k == "sentiment_news_mean_1day":
                        mapped_k = "sentiment_news_mean_1day"
                    elif k == "sentiment_news_mean_3days":
                        mapped_k = "sentiment_news_mean_3days"

                    if mapped_k in merged_data:
                        # Handle boolean, numeric, and string values for main data
                        if isinstance(v, bool):
                            merged_data[mapped_k] = v
                        elif isinstance(v, (int, float)):
                            merged_data[mapped_k] = float(v)
                        elif isinstance(v, str) and v.replace('.', '', 1).isdigit(): # Check if string is a valid number
                            merged_data[mapped_k] = float(v)
                        else:
                            merged_data[mapped_k] = v

                # Construct the final dictionary in the desired order
                result_data = {}
                for field in OUTPUT_FIELD_ORDER:
                    result_data[field] = merged_data.get(field)

                partition = get_partition_for_ticker(ticker)

                # Convert the result to JSON string
                result_json = json.dumps(result_data)

                print(f"[MAIN-DATA-PROCESS] {ticker} - Partition {partition} - Combined data: {result_json}", file=sys.stderr)
                yield result_json # Emit the combined data

        except json.JSONDecodeError:
            print(f"[ERROR] Failed to decode JSON: {value}", file=sys.stderr)
            return []
        except Exception as e:
            print(f"[ERROR] Error in process_element: {e} for value: {value}", file=sys.stderr)
            # Emit an error message for debugging purposes
            return [json.dumps({"error": str(e), "original_message": value})]

def route_by_ticker(json_str):
    """
    Determines the key for incoming JSON data, routing it to either
    the global data handler or the main data handler.
    """
    try:
        data = json.loads(json_str)

        # Route to global data if it contains macroeconomic or general sentiment indicators
        if "gdp_real" in data or ("sentiment_score" in data and data.get("ticker") == "GENERAL"):
            return GLOBAL_DATA_KEY
        # Route to main data if it contains a ticker
        elif "ticker" in data:
            return MAIN_DATA_KEY
        else:
            print(f"[WARN] Unrecognized data or missing ticker: {json_str}", file=sys.stderr)
            return "discard_key" # Discard messages that don't fit expected patterns

    except json.JSONDecodeError:
        print(f"[WARN] Failed to decode JSON for key_by: {json_str}", file=sys.stderr)
        return "invalid_json_key"
    except Exception as e:
        print(f"[ERROR] Error in route_by_ticker: {e} for {json_str}", file=sys.stderr)
        return "error_key"

def extract_ticker_for_partitioning(json_str):
    """
    Extracts the ticker from the JSON string for Kafka partitioning.
    """
    try:
        data = json.loads(json_str)
        ticker = data.get("ticker", "unknown")
        return ticker
    except:
        return "unknown" # Return "unknown" if parsing fails

def main():
    """
    Main function to set up and execute the Flink streaming job.
    Initializes Kafka connections, creates topics, defines data sources,
    processing logic, and sinks.
    """
    # First, wait for Kafka to be available
    print("[INFO] Checking Kafka availability...", file=sys.stderr)
    if not wait_for_kafka():
        print("[ERROR] Failed to connect to Kafka. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Create the Kafka topic with partitions after Kafka is available
    print("[INFO] Creating Kafka topic with partitions...", file=sys.stderr)
    create_kafka_topic_with_partitions()

    # Wait a moment to ensure the topic is created before starting consumer
    time.sleep(2)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Set parallelism for the job

    consumer_props = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'flink_stock_group',
        'auto.offset.reset': 'earliest' # Start consuming from the earliest offset
    }

    # Define Kafka consumer for 'main_data' and 'global_data' topics
    consumer = FlinkKafkaConsumer(
        topics=["main_data", "global_data"],
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props
    )

    # Configure the Kafka producer
    # Flink handles serialization via SimpleStringSchema
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        # 'key.serializer' and 'value.serializer' are managed by SimpleStringSchema
    }

    # Add Kafka consumer as a source to the Flink environment
    stream = env.add_source(consumer, type_info=Types.STRING())

    # Key the stream by the routing function to direct messages to appropriate handlers
    keyed = stream.key_by(route_by_ticker, key_type=Types.STRING())

    # Apply the KeyedProcessFunction for data aggregation and merging
    processed = keyed.process(SlidingAggregator(), output_type=Types.STRING())

    # Define the final Kafka producer for aggregated data
    final_producer = FlinkKafkaProducer(
        topic=AGGREGATED_TOPIC,
        serialization_schema=SimpleStringSchema(), # Serialize output as simple strings
        producer_config=producer_config
    )

    # Partition the processed stream by ticker before sending to the sink
    # This ensures messages for the same ticker go to the same Kafka partition
    partitioned_stream = processed.key_by(extract_ticker_for_partitioning, key_type=Types.STRING())
    partitioned_stream.add_sink(final_producer) # Add Kafka producer as a sink

    print(f"[INFO] Starting Flink job with partitioning on {NUM_PARTITIONS} partitions...", file=sys.stderr)
    env.execute("Global Data Join with Kafka Partitioning")

if __name__ == "__main__":
    main()