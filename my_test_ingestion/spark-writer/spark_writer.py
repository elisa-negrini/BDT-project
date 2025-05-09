from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("StockParquetWriter") \
    .getOrCreate()

# Schema dei messaggi Kafka
schema = StructType() \
    .add("Ticker", StringType()) \
    .add("Timestamp", StringType()) \
    .add("Price", DoubleType()) \
    .add("Size", IntegerType()) \
    .add("Exchange", StringType())

# Lettura da Kafka
raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock_trades") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing JSON
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Aggiunta colonna 'date'
parsed_df = parsed_df.withColumn("date", to_date(col("Timestamp")))

# Scrittura Parquet partizionata
parsed_df.write \
    .partitionBy("Ticker", "date") \
    .mode("append") \
    .parquet("s3a://stock-data/stock/")

print("✓ Scrittura completata")
