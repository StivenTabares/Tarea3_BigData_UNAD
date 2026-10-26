Tarea 3: Procesamiento de Datos con Apache Spark y Kafka (Big Data - UNAD)
Repositorio oficial para el desarrollo de la Tarea 3 del curso de Big Data de la Universidad Nacional Abierta y a Distancia (UNAD). 
Este proyecto implementa soluciones de procesamiento de datos masivos bajo dos paradigmas fundamentales: Procesamiento por Lotes (Batch) 
y Procesamiento en Tiempo Real (Streaming) utilizando un entorno nativo en Linux Mint.

Tecnologías y Herramientas Utilizadas
- Sistema Operativo: Linux Mint (Nativo)
- Lenguaje: Python 3.12

Procesamiento Distribuido: Apache Spark (PySpark v3.5.0)

Infraestructura de Streaming: Apache Kafka (v3.9.1) & Apache Zookeeper

Entorno de Desarrollo: Visual Studio Code (VS Code)

Control de Versiones: Git y GitHub

📁 Estructura del Proyecto
El repositorio se encuentra organizado de la siguiente manera:

Plaintext
Tarea3_BigData/
│
├── batch_analysis.py              # Script de PySpark para procesamiento Batch y análisis exploratorio (EDA)
├── kafka_producer.py              # Simulador de dispositivo IoT que emite eventos JSON hacia Kafka
├── spark_streaming_consumer.py    # Consumidor de Spark Streaming para procesar micro-lotes en tiempo real
├── requirements.txt               # Dependencias y librerías de Python utilizadas
└── README.md                      # Documentación oficial del proyecto

Guía de Ejecución Local
Sigue estos pasos en tu terminal de Linux para replicar y ejecutar la práctica:

1. Clonar el repositorio
Bash:

git clone https://github.com/tu-usuario/tu-repositorio.git
cd Tarea3_BigData

3. Configurar el entorno virtual
Bash:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

5. Ejecutar el Procesamiento Batch
Bash:

python3 batch_analysis.py

7. Ejecutar la infraestructura de Streaming (Kafka)
Paso A (Terminal 1 - Zookeeper):

Bash
cd ~/kafka/kafka_2.13-3.9.1
bin/zookeeper-server-start.sh config/zookeeper.properties

Paso B (Terminal 2 - Servidor Kafka):

Bash
cd ~/kafka/kafka_2.13-3.9.1
bin/kafka-server-start.sh config/server.properties

Paso C (Terminal 3 - Productor de Sensores):

Bash
python3 kafka_producer.py

Paso D (Terminal 4 - Consumidor Spark Streaming):

Bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming_consumer.py

=========================================================================================================

Autor
Estudiante: Brahian Stiven Tabares Castano

Curso: Big Data

Institución: Universidad Nacional Abierta y a Distancia (UNAD)
