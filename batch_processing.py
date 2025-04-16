from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, expr, count, sum as spark_sum,
    when, current_timestamp, avg
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import time
import mysql.connector

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
    StructField("started_at", StringType())  # Kept as StringType, converted later
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

# Kafka topic to consume
topic_input = "twitch_streams_high"
stream_df = get_kafka_stream(topic_input)

if stream_df is None:
    print("❌ Kafka stream is not available. Exiting.")
    exit(1)

# ────────────────────────────────
# ✨ TRANSFORMATION
# ────────────────────────────────

# Convert started_at to timestamp
stream_df = stream_df.withColumn("started_at", to_timestamp(col("started_at")))

# Add record time
stream_df = stream_df.withColumn("record_time", current_timestamp())

# Filter out low-viewer streams
filtered_streams = stream_df.filter(col("viewer_count") > 1000)

# Add popularity flag
filtered_streams = filtered_streams.withColumn(
    "is_popular", when(col("viewer_count") > 5000, True).otherwise(False)
)

# Add popularity bucket
filtered_streams = filtered_streams.withColumn(
    "viewer_bucket",
    when(col("viewer_count") > 25000, "High")
    .when(col("viewer_count") > 15000, "Medium")
    .otherwise("Low")
)

# ────────────────────────────────
# ✨ FOREACH BATCH
# ────────────────────────────────

def process_batch(df, epoch_id):
    start_time = time.time()  # Start the timer

    print(f"\n📦 Processing batch {epoch_id}")

    # Save raw + enriched data to MySQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/twitchdb") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "twitch_streams_enriched") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()

    # Connect to MySQL and execute queries to get metrics
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="twitchdb"
    )
    cursor = mysql_connection.cursor()

    # 1. Top games by total viewers
    cursor.execute("""
        SELECT game_name, SUM(viewer_count) AS total_viewers, COUNT(*) AS num_streams, AVG(viewer_count) AS avg_viewers
        FROM twitch_streams_enriched
        GROUP BY game_name
        ORDER BY total_viewers DESC
        LIMIT 5
    """)
    top_games = cursor.fetchall()
    print("🎮 Top games by viewers:")
    for game in top_games:
        print(game)

    # 2. Stream count by popularity
    cursor.execute("""
        SELECT viewer_bucket, COUNT(*) AS count
        FROM twitch_streams_enriched
        GROUP BY viewer_bucket
        ORDER BY viewer_bucket
    """)
    popularity_dist = cursor.fetchall()
    print("📊 Stream count by popularity:")
    for dist in popularity_dist:
        print(dist)

    # 3. Top streamers by total viewers
    cursor.execute("""
        SELECT user_name, SUM(viewer_count) AS total_viewers, COUNT(*) AS num_sessions
        FROM twitch_streams_enriched
        GROUP BY user_name
        ORDER BY total_viewers DESC
        LIMIT 5
    """)
    top_streamers = cursor.fetchall()
    print("👤 Top streamers:")
    for streamer in top_streamers:
        print(streamer)

    # Calculate the time taken for the batch processing
    duration = time.time() - start_time
    print(f"[PERF] Stream processing batch {epoch_id} took {duration:.2f} seconds")

    # Close MySQL connection
    cursor.close()
    mysql_connection.close()

# ────────────────────────────────
# ⏱️ START STREAMING
# ────────────────────────────────

filtered_streams.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="20 seconds") \
    .start()

spark.streams.awaitAnyTermination()
