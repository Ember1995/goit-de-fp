from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
from pyspark.sql.types import FloatType
import os

# Ініціалізуємо SparkSession
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

# Шляхи до директорій
silver_dir = "silver"
gold_dir = "gold"

# Шляхи до таблиць
silver_bio_path = f"{silver_dir}/athlete_bio"
silver_events_path = f"{silver_dir}/athlete_event_results"
gold_results_path = f"{gold_dir}/avg_stats"

# Створюємо gold-директорію, якщо не існує
os.makedirs(gold_dir, exist_ok=True)

# Зчитування silver-таблиць
df_bio = spark.read.parquet(silver_bio_path)
df_events = spark.read.parquet(silver_events_path)

# Приводимо вагу і зріст до типу float у df_bio
df_bio = df_bio.withColumn("weight", col("weight").cast(FloatType()))
df_bio = df_bio.withColumn("height", col("height").cast(FloatType()))

# Join по колонці athlete_id
df_joined = df_events.join(df_bio, on="athlete_id", how="inner").drop(df_events.country_noc)

# Обчислення середніх значень та додавання timestamp
df_avg = df_joined.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg(col("weight")).alias("avg_weight"),
        avg(col("height")).alias("avg_height")
    ).withColumn("timestamp", current_timestamp())

# Записуємо результат у форматі parquet в gold
df_avg.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(gold_results_path)

# Вивід фінального DataFrame у лог Airflow
df_avg.show()

print(f"Дані збережено в: {gold_results_path}")

# Завершення роботи Spark
spark.stop()
