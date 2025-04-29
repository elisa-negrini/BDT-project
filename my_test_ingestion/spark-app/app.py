from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lag
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder \
    .appName("StockAnomalyDetection") \
    .getOrCreate()

# Define schema for parsing the JSON
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read stream from Kafka
kafka_streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka messages
parsed_df = kafka_streaming_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Cast timestamp correctly
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Define a window spec to calculate previous price
windowSpec = Window.partitionBy("ticker").orderBy("timestamp")

# Add previous price
with_prev = parsed_df.withColumn("prev_price", lag("price").over(windowSpec))

# Calculate price change percentage
price_change = with_prev.withColumn(
    "price_change_pct",
    ((col("price") - col("prev_price")) / col("prev_price")) * 100
)

# Detect anomalies: price jump > +10% or drop < -10%
anomalies = price_change.filter(
    (col("price_change_pct") > 10) | (col("price_change_pct") < -10)
)

# Output anomalies to the console
query = anomalies.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()