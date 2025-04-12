import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, struct, avg, current_timestamp
)
from pyspark.sql.types import (
    StructType, StringType, IntegerType, FloatType
)

# Пакет, необхідний для читання Kafka зі Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
)

load_dotenv()

# Конфігурація MySQL
mysql_config = {
    "url": os.getenv("MYSQL_URL"),
    "bio_table": os.getenv("MYSQL_BIO_DB"),
    "results_table": os.getenv("MYSQL_RESULTS_DB"),
    "agg_table": os.getenv("MYSQL_AGG_DB"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Конфігурація Kafka
kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(","),
    "username": os.getenv("KAFKA_USERNAME"),
    "password": os.getenv("KAFKA_PASSWORD"),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
    "event_topic": os.getenv("KAFKA_EVENT_TOPIC"),
    "output_topic": os.getenv("KAFKA_OUTPUT_TOPIC")
}

# Створення Spark сесії
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("MyUnifiedApp") \
    .master("local[*]") \
    .getOrCreate()

# Етап 3. Читання результатів
results_df = spark.read.format('jdbc').options(
    url=mysql_config["url"],
    driver=mysql_config["driver"],
    dbtable=mysql_config["results_table"],
    user=mysql_config["user"],
    password=mysql_config["password"]
).load()

# Етап 3. Kafka-продюсер
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# Етап 3. Відправлення кожного рядка результатів в Kafka
for row in results_df.toJSON().collect():
    data = json.loads(row)
    key = str(data["athlete_id"])
    producer.send(kafka_config["event_topic"], key=key, value=data)

producer.flush()
producer.close()

# Етап 3. Схема для читання результатів з Kafka
results_schema = StructType() \
    .add("edition", StringType()) \
    .add("edition_id", IntegerType()) \
    .add("country_noc", StringType()) \
    .add("sport", StringType()) \
    .add("event", StringType()) \
    .add("result_id", StringType()) \
    .add("athlete", StringType()) \
    .add("athlete_id", IntegerType()) \
    .add("pos", StringType()) \
    .add("medal", StringType()) \
    .add("isTeamSport", StringType())

# Етап 3. Читання потоку даних результатів із Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
    .option("subscribe", kafka_config["event_topic"]) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .load()

# Етап 3. Парсимо JSON результатів
parsed_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .withColumn("json", from_json(col("value"), results_schema)) \
    .select("json.*")

# Етап 1. Читання біо-даних
bio_df = spark.read.format('jdbc').options(
    url=mysql_config["url"],
    driver=mysql_config["driver"],
    dbtable=mysql_config["bio_table"],
    user=mysql_config["user"],
    password=mysql_config["password"]
).load()

# Етап 2. Фільтрація біо-даних
bio_clean_df = bio_df \
    .withColumn("athlete_id", col("athlete_id").cast(IntegerType())) \
    .withColumn("height", col("height").cast(FloatType())) \
    .withColumn("weight", col("weight").cast(FloatType())) \
    .filter(col("height").isNotNull() & col("weight").isNotNull())

# Етап 4. Об'єднання Kafka-даних результатів зі статичними біо-даними
enriched_df = parsed_df.join(bio_clean_df, on="athlete_id", how="inner") \
    .select(
        parsed_df["sport"],
        parsed_df["medal"],
        bio_clean_df["sex"],
        bio_clean_df["country_noc"],
        bio_clean_df["height"],
        bio_clean_df["weight"]
    )

# Етап 5. Агрегація даних
df_aggregated = enriched_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
).withColumn("timestamp", current_timestamp())

# Етап 6. Функція для обробки та запису кожної партії даних
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df.selectExpr("CAST(sport AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
        .option("topic", kafka_config["output_topic"]) \
        .option("kafka.security.protocol", kafka_config["security_protocol"]) \
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
        .save()

    batch_df.write.format("jdbc").options(
        url=mysql_config["url"],
        driver=mysql_config["driver"],
        dbtable=mysql_config["agg_table"],
        user=mysql_config["user"],
        password=mysql_config["password"]
    ).mode("append").save()

# Етап 6. Запуск стріму
df_aggregated.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoint/output_stream") \
    .start() \
    .awaitTermination()
