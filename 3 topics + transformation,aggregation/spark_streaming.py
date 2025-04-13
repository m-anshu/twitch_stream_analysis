from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, expr, count, sum as spark_sum, when
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import time

# Avoid loopback warning
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TwitchStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming stream data
schema = StructType([
    StructField("user_name", StringType()),
    StructField("game_name", StringType()),
    StructField("viewer_count", IntegerType()),
    StructField("started_at", StringType())  # Keep as StringType initially
])

# Function to read from Kafka
def get_kafka_stream(topic):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("maxOffsetsPerTrigger", 20) \
            .load()

        parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        return parsed
    except Exception as e:
        print("⚠️ Kafka stream could not be initialized:", e)
        return None

# Prompt the user for the Kafka topic to subscribe to
topic_input = "twitch_streams_high"

# Get the stream
stream_df = get_kafka_stream(topic_input)

if stream_df is None:
    print("❌ Kafka stream is not available. Exiting.")
    exit(1)

# ────────────────────────────────
# ✨ TRANSFORMATION
# ────────────────────────────────

# Convert 'started_at' to TimestampType
stream_df = stream_df.withColumn("started_at", to_timestamp(col("started_at")))

# Filter streams that have more than 1000 viewers
filtered_streams = stream_df.filter(col("viewer_count") > 1000)

# Add a new column 'is_popular' to label streams with more than 5000 viewers as "popular"
filtered_streams = filtered_streams.withColumn(
    "is_popular", when(col("viewer_count") > 5000, True).otherwise(False)
)


# ────────────────────────────────
# ✨ AGGREGATION
# ────────────────────────────────
# Function to process each batch of 20 records
def process_batch(df, epoch_id):
    # Aggregation per batch
    viewers_per_game = df.groupBy("game_name").agg(
        spark_sum("viewer_count").alias("total_viewers"),
        count("*").alias("num_streams")
    ).orderBy(spark_sum("viewer_count").desc())

    # Display the aggregated data to the console
    viewers_per_game.show()

# ────────────────────────────────
# READ 20 RECORDS AND SLEEP FOR 20 SECONDS
# ────────────────────────────────

# Use `foreachBatch` to control how we process batches of data (in chunks of 20)
stream_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="20 seconds") \
    .start()

# Wait for the streams to process and ensure the stream is running continuously
spark.streams.awaitAnyTermination()

# Simulating a wait after reading the 20 records and processing them
# This sleep will allow the Spark job to process 20 records, then wait for the next batch
time.sleep(20)
