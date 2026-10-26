import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configurar el productor apuntando al broker local de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    return {
        "sensor_id": f"sensor_{random.randint(1, 3)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(20.0, 40.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2)
    }

if __name__ == "__main__":
    print("Iniciando productor de Kafka (Presiona Ctrl+C para detener)...")
    try:
        while True:
            sensor_data = generate_sensor_data()
            producer.send('sensor_data', value=sensor_data)
            print(f"Enviado a Kafka -> {sensor_data}")
            time.sleep(2) # Envía un registro cada 2 segundos
    except KeyboardInterrupt:
        print("\nProductor detenido.")