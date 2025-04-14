# Twitch Stream Analysis

## 1. First ensure kafka and spark are installed on your environment and then proceed:

Run twitch_kafka_producer.py using this command
python twitch_kafka_producer.py


## 2. Run spark_streaming.py on a seperate terminal using this command. (Make sure the version numbers match)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 spark_streaming.py
 
# Steps in detail :


#  Kafka + MySQL + Spark Setup Guide

##  1. Start Zookeeper & Kafka Containers
If they’re already running, skip this step. Otherwise, run:

```bash
docker start zookeeper
docker start kafka
```

---

## 2. Spin Up a MySQL Container

```bash
docker run -d \
  --name mysql \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=twitchdb \
  -p 3306:3306 \
  mysql:8.0
```
start the container after using docker start mysql

---

## 3. Activate Your Spark Virtual Environment

```bash
source ~/spark-env/bin/activate
```

---

## 4. CLI into the MySQL Container

```bash
docker exec -it mysql mysql -uroot -proot
```

---

## 5. Run Kafka Producer Script  

```bash
python3 twitch_kafka_producer.py
```

---

## 6. Add MySQL Connector to Spark

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.33 \
  spark_streaming.py
```

---

## 7. Run Batch Mode SQL Queries

```bash
docker exec -it mysql mysql -uroot -proot -e "SELECT * FROM twitchdb.twitch_aggregates;"
```

---

## 8. Table Schema

```sql
USE twitchdb;

DROP TABLE IF EXISTS twitch_streams_raw;

CREATE TABLE twitch_streams_raw (
    user_name VARCHAR(100),
    game_name VARCHAR(100),
    viewer_count INT,
    started_at DATETIME,
    is_popular BOOLEAN,
    record_time TIMESTAMP
);
```
```

