from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crea la sessione Spark
spark = SparkSession.builder \
    .appName("KafkaSimpleReader") \
    .getOrCreate()

# Leggi dallo stream Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Converti il valore da bytes a stringa
messages = df.selectExpr("CAST(value AS STRING) as message")

# Stampa a console
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("✅ Spark Kafka consumer avviato. In attesa di messaggi su 'test_topic'...")
query.awaitTermination()
