from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, max, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

# Set SPARK_LOCAL_IP to avoid loopback warning
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TwitchStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for Twitch stream data
schema = StructType([
    StructField("user_name", StringType()),
    StructField("game_name", StringType()),
    StructField("viewer_count", IntegerType()),
    StructField("started_at", StringType())
])

def get_kafka_stream():
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "twitch_streams") \
            .load()

        parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        return parsed
    except Exception as e:
        print("⚠️ Kafka stream could not be initialized:", e)
        return None

# Try to get Kafka stream
stream_df = get_kafka_stream()

if stream_df is None:
    print("❌ Kafka stream is not available. Exiting.")
    exit(1)

# Convert 'started_at' to timestamp
stream_df = stream_df.withColumn("started_at", to_timestamp("started_at"))

# Perform aggregation every 5 entries in the topic (using trigger interval)
# We'll collect the metrics and process in batches
metrics_df = stream_df.groupBy(
    col("game_name")
).agg(
    avg("viewer_count").alias("avg_viewers"),
    count("*").alias("stream_count"),
    max("viewer_count").alias("max_viewers")
)

# Output metrics to console
query = metrics_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 100) \
    .trigger(processingTime='5 seconds') \
    .start()  # Fixed indentation here

spark.streams.awaitAnyTermination()
