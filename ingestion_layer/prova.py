from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .config("spark.pyspark.python", sys.executable) \
    .config("spark.pyspark.driver.python", sys.executable) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Dataframe111")
df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])

print("Dataframe")
df.show(truncate =False)
print("fine")
spark.stop()