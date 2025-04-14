from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, expr, count, sum as spark_sum, when, current_timestamp
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
            .option("maxOffsetsPerTrigger", 50) \
            .load()

        parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        return parsed
    except Exception as e:
        print("⚠️ Kafka stream could not be initialized:", e)
        return None

# Kafka topic input
topic_input = "twitch_streams_high"
stream_df = get_kafka_stream(topic_input)

if stream_df is None:
    print("❌ Kafka stream is not available. Exiting.")
    exit(1)

# ────────────────────────────────
# ✨ TRANSFORMATION
# ────────────────────────────────

# Convert 'started_at' to timestamp
stream_df = stream_df.withColumn("started_at", to_timestamp(col("started_at")))

# Filter streams with more than 1000 viewers
filtered_streams = stream_df.filter(col("viewer_count") > 1000)

# Add 'is_popular' flag
filtered_streams = filtered_streams.withColumn(
    "is_popular", when(col("viewer_count") > 5000, True).otherwise(False)
)

# Add 'record_time' for MySQL
filtered_streams = filtered_streams.withColumn("record_time", current_timestamp())

# ────────────────────────────────
# ✨ FOREACH BATCH
# ────────────────────────────────

def process_batch(df, epoch_id):
    # Aggregation
    viewers_per_game = df.groupBy("game_name").agg(
        spark_sum("viewer_count").alias("total_viewers"),
        count("*").alias("num_streams")
    ).orderBy(spark_sum("viewer_count").desc())

    # Show in console
    viewers_per_game.show()

    # Save raw records to MySQL
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:mysql://localhost:3306/twitchdb") \
      .option("driver", "com.mysql.cj.jdbc.Driver") \
      .option("dbtable", "twitch_streams_raw") \
      .option("user", "root") \
      .option("password", "root") \
      .mode("append") \
      .save()

# ────────────────────────────────
# ⏱️ STREAMING START
# ────────────────────────────────

filtered_streams.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="20 seconds") \
    .start()

spark.streams.awaitAnyTermination()
