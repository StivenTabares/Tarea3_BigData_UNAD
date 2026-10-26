from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min

def run_batch_processing():
    # 1. Crear la sesión de Spark
    spark = SparkSession.builder \
        .appName("BatchDataProcessing_IoT") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("--- 1. Cargando el Dataset ---")
    # df = spark.read.option("header", "true").option("inferSchema", "true").csv("sensor_data.csv")
    
    # Simulación de carga para demostración estructural:
    data = [
        ("sensor_1", "2026-10-26 10:00:00", 22.5, 45.0),
        ("sensor_2", "2026-10-26 10:01:00", 35.1, 60.0),
        ("sensor_1", "2026-10-26 10:02:00", 23.0, 44.5),
        ("sensor_3", "2026-10-26 10:03:00", 19.8, 70.2),
        ("sensor_2", "2026-10-26 10:04:00", 36.0, 58.1)
    ]
    columns = ["sensor_id", "timestamp", "temperature", "humidity"]
    df = spark.createDataFrame(data, columns)

    print("Datos originales:")
    df.show()

    print("--- 2. Limpieza y Transformación ---")
    # Filtrar valores nulos o anomalías físicas evidentes (ej. temperaturas irreales)
    df_clean = df.dropna().filter((col("temperature") >= -50) & (col("temperature") <= 60))

    print("--- 3. Análisis Exploratorio de Datos (EDA) ---")
    print("Resumen estadístico de las variables:")
    df_clean.describe().show()

    print("--- 4. Agregaciones (Promedios y Máximas por Sensor) ---")
    resumen_sensor = df_clean.groupBy("sensor_id").agg(
        avg("temperature").alias("temp_promedio"),
        avg("humidity").alias("humedad_promedio"),
        max("temperature").alias("temp_maxima")
    )
    resumen_sensor.show()

    print("--- 5. Guardado de Resultados ---")
    resumen_sensor.write.mode("overwrite").option("header", "true").csv("output_batch_results")
    print("Proceso Batch finalizado con éxito.")

    spark.stop()

if __name__ == "__main__":
    run_batch_processing()