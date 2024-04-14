from kafka import KafkaConsumer
import boto3
import json

# Configurar el consumidor de Kafka
consumer = KafkaConsumer(
    'sales_stream',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Configurar boto3 para usar LocalStack
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

# Nombre del bucket en LocalStack S3
bucket_name = 'kafka-data'

# Consumir mensajes de Kafka y subir a S3
for message in consumer:
    data = message.value
    key = f"{data['store_id']}_{data['product_id']}_{data['timestamp']}.json"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
    print(f"Uploaded data to S3: {key}")
