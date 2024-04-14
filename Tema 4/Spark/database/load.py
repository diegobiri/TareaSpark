import boto3
import os

# Configura boto3 para usar LocalStack
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',  # Asegúrate de que esta URL sea correcta para tu LocalStack
    aws_access_key_id='test',               # Estas claves son para LocalStack, donde no necesitas claves reales
    aws_secret_access_key='test'
)

# Nombre del archivo CSV y bucket
file_name = 'sales_data.csv'
bucket_name = 'csv-data'

def export_to_csv(conn, csv_file_name):
    with conn.cursor() as cursor:
        with open(csv_file_name, 'w', newline='') as csv_file:
            cursor.copy_expert("COPY stores TO STDOUT WITH CSV HEADER", csv_file)
            
# Subir el archivo CSV a LocalStack S3
def upload_file_to_s3():
    if os.path.isfile(file_name):
        s3_client.upload_file(file_name, bucket_name, file_name)
        print(f'File {file_name} uploaded to {bucket_name} in LocalStack S3')

if __name__ == "__main__":
    conn = connect_db()
    export_to_csv(conn, "stores_data.csv")
    upload_file_to_s3()
    conn.close()

