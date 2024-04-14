from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, max, avg, month

def main():
    # Inicializar SparkSession
    spark = SparkSession.builder \
        .appName("CompleteSalesDataAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    # Cargar DataFrames desde S3 (suponiendo que los datos están en Parquet)
    sales_df = spark.read.parquet("s3a://data-warehouse/sales_analysis/")
    stores_df = spark.read.parquet("s3a://data-warehouse/geo_analysis/")
    products_df = spark.read.parquet("s3a://data-warehouse/product_analysis/")

    # a. Análisis de Ventas
    # ¿Qué tienda tiene los mayores ingresos totales?
    sales_analysis = sales_df.groupBy("Store ID").agg(
        sum("Revenue").alias("Total Revenue"),
        max("Revenue").alias("Max Sale"),
        avg("Revenue").alias("Average Sale")
    )
    sales_analysis.orderBy("Total Revenue", ascending=False).show(1)

    # ¿Cuáles son los ingresos totales generados en una fecha concreta?
    specific_date = "2023-01-01"  # Ajusta esta fecha según tus necesidades
    sales_df.filter(sales_df["Date"] == specific_date) \
            .groupBy("Date").agg(sum("Revenue").alias("Total Revenue")) \
            .show()

    # ¿Qué producto tiene la mayor cantidad vendida?
    sales_df.groupBy("Product ID").agg(sum("Quantity Sold").alias("Total Quantity Sold")) \
            .orderBy("Total Quantity Sold", ascending=False) \
            .show(1)

    # b. Análisis Geográfico
    # ¿Cuáles son las regiones con mejores resultados en función de los ingresos?
    stores_df.orderBy("Total Revenue", ascending=False).show()

    # ¿Existe alguna correlación entre la ubicación de la tienda y el rendimiento de las ventas?
    sales_df.join(stores_df, "Store ID").groupBy("Location").agg(sum("Revenue").alias("Total Revenue")) \
            .orderBy("Total Revenue", ascending=False) \
            .show()

    # c. Análisis Demográfico
    # ¿Cómo varía el rendimiento de las ventas entre los distintos grupos demográficos?
    sales_df.groupBy("Demographic Group").agg(sum("Revenue").alias("Revenue by Group")) \
            .orderBy("Revenue by Group", ascending=False) \
            .show()

    # d. Análisis Temporal
    # ¿Cómo varía el rendimiento de las ventas a lo largo del tiempo (diariamente)?
    sales_df.groupBy("Date").agg(sum("Revenue").alias("Daily Revenue")).orderBy("Date").show()

    # Finalizar sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
