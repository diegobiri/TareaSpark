from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max

spark = SparkSession.builder \
    .appName("DataWarehouseCreation") \
    .master("local[*]") \
    .getOrCreate()

# Suponiendo que df es tu DataFrame principal cargado de S3
df = spark.read.parquet("s3a://processed-data/csv_data_clean/")

# 1. Análisis de Ventas
sales_analysis = df.groupBy("Store ID").agg(
    sum("Revenue").alias("Total Revenue"),
    max("Revenue").alias("Max Sale"),
    avg("Revenue").alias("Average Sale")
)
sales_analysis.write.mode("overwrite").parquet("s3a://data-warehouse/sales_analysis/")

# 2. Análisis Geográfico
geo_analysis = df.groupBy("Location").agg(
    sum("Revenue").alias("Total Revenue")
)
geo_analysis.write.mode("overwrite").parquet("s3a://data-warehouse/geo_analysis/")

# 3. Análisis Demográfico
# Suponiendo que tienes datos demográficos como columnas en df
demo_analysis = df.groupBy("Demographic Group").agg(
    sum("Revenue").alias("Total Revenue")
)
demo_analysis.write.mode("overwrite").parquet("s3a://data-warehouse/demo_analysis/")

# 4. Análisis Temporal
temporal_analysis = df.groupBy("Date").agg(
    sum("Revenue").alias("Total Revenue")
)
temporal_analysis.write.mode("overwrite").parquet("s3a://data-warehouse/temporal_analysis/")

spark.stop()
