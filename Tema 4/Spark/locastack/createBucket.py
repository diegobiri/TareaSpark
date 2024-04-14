import boto3

# Configurar boto3 para usar LocalStack
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',  # Asegúrate de que esta URL sea correcta para tu LocalStack
    aws_access_key_id='test',               # Estas claves son para LocalStack, donde no necesitas claves reales
    aws_secret_access_key='test'
)

# Crear buckets en LocalStack S3
def create_buckets():
    buckets = ['csv-data', 'kafka-data', 'postgresql-data']
    for bucket_name in buckets:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} created')

if __name__ == '__main__':
    create_buckets()


