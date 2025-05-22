from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
import json


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka consumer configuration
    consumer_properties = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_stock_group'
    }

    consumer = FlinkKafkaConsumer(
        topics='stock_trades',
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_properties
    )

    # Kafka producer configuration
    producer = FlinkKafkaProducer(
        topic='aggregation',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    # Stream pipeline
    stream = env.add_source(consumer, type_info=Types.STRING())

    # Dummy transformation with print
    def transform_and_print(x):
        print(f"⬇️ Received message: {x}")
        transformed = x + " - processed"
        print(f"⬆️ Sending message: {transformed}")
        return transformed

    transformed = stream.map(transform_and_print, output_type=Types.STRING())

    transformed.add_sink(producer)

    env.execute("Stock Trade Processing Pipeline")


if __name__ == "__main__":
    main()
