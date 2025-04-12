from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import os
import re

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

# Шляхи до директорій
bronze_dir = "bronze"
silver_dir = "silver"

# Таблиці, які обробляємо
tables = ["athlete_bio", "athlete_event_results"]

# Створюємо silver-директорію, якщо не існує
os.makedirs(silver_dir, exist_ok=True)

# Python-функція для очищення тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text)) if text else None

# Spark UDF
clean_text_udf = udf(clean_text, StringType())

# Функція для очищення всіх текстових колонок
def clean_text_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, clean_text_udf(col(field.name)))
    return df

# Основний цикл обробки таблиць
for table in tables:
    bronze_path = os.path.join(bronze_dir, table)
    silver_path = os.path.join(silver_dir, table)

    # Зчитування parquet з bronze layer
    df = spark.read.format("parquet").load(bronze_path)

    # Очистка текстових колонок
    df_cleaned = clean_text_columns(df)

    # Дедублікація
    df_deduped = df_cleaned.distinct()

    # Репартиціювання для оптимізації
    df_repartitioned = df_deduped.repartition(4)

    # Запис до silver layer
    df_repartitioned.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(silver_path)
    
    # Вивід фінального DataFrame у лог (для перевірки в Airflow)
    df_repartitioned.show()

    print(f"Оброблені дані збережено для таблиці {table}: {silver_path}")

# Завершення Spark-сесії
spark.stop()
