from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def run_streaming_consumer():
    # Crear sesión de Spark integrando el conector oficial de Kafka (ajusta la versión de Spark/Scala si es necesario)
    spark = SparkSession.builder \
        .appName("SensorDataStreamingConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Definir el esquema JSON de los datos que envía el productor
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    print("Conectando al Stream de Kafka...")
    # Leer el flujo en tiempo real desde el topic de Kafka
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor_data") \
        .load()

    # Extraer y parsear la columna binaria 'value' a formato estructurado JSON
    json_df = df_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # Configurar la salida en tiempo real hacia la consola
    query = json_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_streaming_consumer()