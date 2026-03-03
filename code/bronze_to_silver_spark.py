# code/bronze_to_silver_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Por defecto asumimos producción
ENV = os.getenv("ENV", "prod")

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

    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .getOrCreate()
    )

    for table in tables:
        print(f"Processing {table}")

        input_path = f"{BRONZE_PATH}/{table}"
        output_path = f"{SILVER_PATH}/{table}"

        df = spark.read.parquet(input_path)

        # Normalizar nombres de columnas
        for column_name in df.columns:
            df = df.withColumnRenamed(
                column_name,
                column_name.lower().strip()
            )

        # Eliminar duplicados
        df = df.dropDuplicates()

        df.write.mode("overwrite").parquet(output_path)

        print(f"{table} processed successfully")

    spark.stop()


if __name__ == "__main__":
    main()