from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("DataCleaning") \
    .master("local[*]") \
    .config("fs.s3a.endpoint", "http://localhost:4566") \
    .config("fs.s3a.access.key", "test") \
    .config("fs.s3a.secret.key", "test") \
    .config("fs.s3a.path.style.access", True) \
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Leer datos desde S3 (LocalStack)
df_csv = spark.read.csv("s3a://csv-data/sales_data.csv", header=True, inferSchema=True)
df_kafka = spark.read.json("s3a://kafka-data/*")

# Limpiar y transformar los datos (ejemplo con CSV)
df_csv_clean = df_csv.na.drop(subset=["Store ID", "Revenue"])  # Eliminar filas con valores nulos en columnas importantes
df_csv_clean = df_csv_clean.dropDuplicates()  # Eliminar duplicados

# Añadir columnas adicionales
df_csv_clean = df_csv_clean.withColumn("Tratados", lit(True)) \
                           .withColumn("Fecha Inserción", current_timestamp())

# Guardar o procesar los datos limpios
df_csv_clean.write.mode("overwrite").parquet("s3a://processed-data/csv_data_clean/")

# Detener SparkSession
spark.stop()
