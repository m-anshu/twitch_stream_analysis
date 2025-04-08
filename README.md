# Twitch Stream Analysis

## 1. First ensure kafka and spark are installed on your environment and then proceed:

Run twitch_kafka_producer.py using this command
python twitch_kafka_producer.py


## 2. Run spark_streaming.py on a seperate terminal using this command. (Make sure the version numbers match)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 spark_streaming.py
 
