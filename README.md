# Twitch Stream Processing with Kafka, Spark, and MySQL

This project processes Twitch stream data using a Kafka producer, Spark Structured Streaming, and MySQL. The data is fetched from the Twitch API, sent to Kafka, and consumed by Spark for processing. The processed data is stored in a MySQL database, where various analytics queries are executed.

## Project Structure

- **`twitch_kafka_producer.py`**: This script fetches stream data from the Twitch API and sends it to Kafka based on viewer count.
- **`spark_streaming.py`**: This script reads the data from Kafka using Spark Structured Streaming, processes the data, and prints the top games and streamers.
- **`batch_processing.py`**: This script extends the previous one by saving the processed data into MySQL and running analytics queries on it.

## Requirements

- **Apache Kafka**: Make sure Kafka is installed and running on your machine. You can follow the [Kafka documentation](https://kafka.apache.org/quickstart) for installation.
- **MySQL**: A MySQL instance must be running with the `twitchdb` database created, along with the `twitch_streams_enriched` table to store processed data.
- **Apache Spark**: You need to have Spark installed. Follow the [Spark documentation](https://spark.apache.org/docs/latest/) for installation.

### Python Packages:
- `requests`
- `kafka-python`
- `pyspark`
- `mysql-connector-python`

You can install the required Python packages by running:

```bash
pip install requests kafka-python pyspark mysql-connector-python
```
## Setting Up MYSQL

### Create DB
```bash
CREATE DATABASE twitchdb;
```


### Create Table
```bash
CREATE TABLE twitch_streams_enriched (
    user_name VARCHAR(255),
    game_name VARCHAR(255),
    viewer_count INT,
    started_at DATETIME,
    record_time DATETIME,
    is_popular BOOLEAN,
    viewer_bucket VARCHAR(50)
);
```


## Kafka Topics

The Kafka producer sends data to the following topics based on viewer count:

    twitch_streams_high: For streams with high viewer counts.

    twitch_streams_mid: For streams with medium viewer counts.

    twitch_streams_low: For streams with low viewer counts.

## How It Works
1. Kafka Producer (twitch_kafka_producer.py):

    Fetches data from the Twitch API.

    Based on the stream's viewer count and age, sends the data to one of the Kafka topics: twitch_streams_high, twitch_streams_mid, or twitch_streams_low.

2. Spark Consumer (spark_streaming.py):

    Consumes data from the twitch_streams_high topic (you can modify to consume from other topics if needed).

    Applies transformations to filter streams with more than 1000 viewers and categorizes them into popularity buckets.

    Prints metrics such as top games by viewers, stream count by popularity, and top streamers.

3. Batch Processing (batch_processing.py):

    Consumes data from Kafka using Spark.

    Saves the processed data into MySQL.

    Runs analytical SQL queries on the data in MySQL to retrieve metrics like top games, stream count by popularity, and top streamers by total viewers.

## Running the Project
1. Start Kafka and MySQL

Ensure that Kafka and MySQL are running.

Start Kafka by running the following command in the terminal:

    bin/kafka-server-start.sh config/server.properties

Create the necessary database and tables in MySQL as mentioned above.

2. Start Kafka Producer

Run the twitch_kafka_producer.py script to start sending stream data to Kafka:

    python twitch_kafka_producer.py

3. Start Stream Processing

Run the batch_processing.py script to start consuming from Kafka, processing the data using Spark and displaying the results on terminal:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 spark_streaming.py


4. Start Batch Processing

Run the batch_processing.py script to start consuming from Kafka, processing the data, and storing the results in MySQL:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.33 spark_streaming.py

5. Monitor Output

As the data is processed, the output will be printed to the terminal with metrics such as:

    Top games by total viewers.

    Stream count by popularity.

    Top streamers by total viewers.

    The time taken for processing each batch in each type of processing method(stream/batch).

## Customizing the Project

    You can modify the Twitch API parameters in twitch_kafka_producer.py to fetch more or fewer streams.

    Adjust the stream processing logic in spark_streaming.py to include more metrics or modify the transformations.

    Modify the MySQL queries in batch_processing.py to suit your needs for analytics.

# License

This project is licensed under the MIT License - see the LICENSE file for details.

