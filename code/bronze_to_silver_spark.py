# code/bronze_to_silver_spark.py

from pyspark.sql import SparkSession
import os
import sys

# Detectar entorno
# Por defecto: local
ENV = os.getenv("ENV", "local")

if ENV == "local":
    BRONZE_PATH = "./data/bronze"
    SILVER_PATH = "./data/silver"
else:
    BUCKET = "wasi-qollqui-lake-53930c9b"
    BRONZE_PATH = f"gs://{BUCKET}/bronze"
    SILVER_PATH = f"gs://{BUCKET}/silver"

tables = [
    "clientes",
    "deuda",
    "pagos",
    "productos",
    "gestores",
    "gestiones_cobranza",
    "promesas_pago",
    "dim_calendario"
]


def main():

    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .master("local[*]") \
        .getOrCreate()

    for table in tables:
        print(f"Processing {table}")

        input_path = f"{BRONZE_PATH}/{table}"
        output_path = f"{SILVER_PATH}/{table}"

        df = spark.read.parquet(input_path)

        # limpiar nombres de columnas
        for column in df.columns:
            df = df.withColumnRenamed(column, column.lower().strip())

        # eliminar duplicados
        df = df.dropDuplicates()

        df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()