from pyspark.sql import SparkSession
import os
import requests

# Функція для завантаження CSV-файлу з FTP
def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Завантаження з {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(os.path.join("landing", local_file_path + ".csv"), 'wb') as file:
            file.write(response.content)
        print(f"Файл збережено як landing/{local_file_path}.csv")
    else:
        exit(f"Не вдалося завантажити файл. Status code: {response.status_code}")

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("LandingToBronze") \
    .getOrCreate()

# Створення директорій, якщо не існують
landing_dir = "landing"
bronze_dir = "bronze"
os.makedirs(landing_dir, exist_ok=True)
os.makedirs(bronze_dir, exist_ok=True)

# Список таблиць, які потрібно завантажити
tables = ["athlete_bio", "athlete_event_results"]

# Завантаження та збереження у форматі Parquet
for table_name in tables:
    # Завантажити CSV
    download_data(table_name)

    # Шляхи
    csv_path = os.path.join(landing_dir, f"{table_name}.csv")
    bronze_path = os.path.join(bronze_dir, table_name)

    # Зчитування у Spark DataFrame
    df = spark.read \
        .option("header", "true") \
        .csv(csv_path)

    # Збереження у форматі Parquet
    df.write \
        .mode("overwrite") \
        .parquet(bronze_path)
    
        # Вивід фінального DataFrame у лог (для перевірки в Airflow)
    df.show()

    print(f"Дані збережено в {bronze_path}")

# Завершення роботи Spark
spark.stop()
