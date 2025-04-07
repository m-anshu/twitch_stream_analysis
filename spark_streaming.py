from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
    """
    Attempts to connect to Kafka and read the stream.
    If it fails, returns None.
    """
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

# Aggregation 1: Average viewer count per game every minute
avg_df = stream_df.groupBy(
    window(col("started_at"), "1 minute"),
    col("game_name")
).agg({"viewer_count": "avg"}) \
.withColumnRenamed("avg(viewer_count)", "avg_viewers")

# Aggregation 2: Count of streams per game
count_df = stream_df.groupBy("game_name").agg(count("*").alias("stream_count"))

# Aggregation 3: Maximum viewer count per game
max_df = stream_df.groupBy("game_name").agg(max("viewer_count").alias("max_viewers"))

# Output all aggregations to console (only for real streaming data)
if stream_df.isStreaming:
    avg_query = avg_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()

    count_query = count_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    max_query = max_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # Keep streaming alive
    spark.streams.awaitAnyTermination()
else:
    print("💡 Using static data (no live Kafka stream). Showing example aggregations:")
    avg_df.show(truncate=False)
    count_df.show(truncate=False)
    max_df.show(truncate=False)
