from kafka import KafkaProducer
import json
import random
from datetime import datetime, timedelta

def generate_random_data():
    """Genera un mensaje de datos de ventas con valores aleatorios y posibles errores."""
    product_id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=10))
    timestamp = int(datetime.now().timestamp() * 1000)
    quantity_sold = random.randint(1, 100)
    revenue = round(random.uniform(100, 5000), 2)
    # Introduce errores con cierta probabilidad
    if random.random() < 0.1:  # 10% de probabilidad de error
        revenue = "ERROR"  # Error de formato en revenue
    return {
        "timestamp": timestamp,
        "product_id": product_id,
        "quantity_sold": quantity_sold,
        "revenue": revenue
    }

def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    topic = 'sales_stream'
    
    try:
        while True:
            data = generate_random_data()
            producer.send(topic, value=data)
            print(f"Sent data: {data}")
            producer.flush()
            # Esperar algún tiempo antes de enviar el próximo mensaje
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
