# code/silver_to_gold_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

ENV = os.getenv("ENV", "prod")

if ENV == "local":
    SILVER_PATH = "./data/silver"
    GOLD_PATH = "./data/gold"
else:
    BUCKET = "wasi-qollqui-lake-53930c9b"
    SILVER_PATH = f"gs://{BUCKET}/silver"
    GOLD_PATH = f"gs://{BUCKET}/gold"


def main():

    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

    # ========================
    # DIMENSIONS
    # ========================

    clientes = spark.read.parquet(f"{SILVER_PATH}/clientes")
    dim_cliente = clientes.dropDuplicates(["customer_id"])
    dim_cliente.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_cliente")

    productos = spark.read.parquet(f"{SILVER_PATH}/productos")
    dim_producto = productos.dropDuplicates(["product_id"])
    dim_producto.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_producto")

    gestores = spark.read.parquet(f"{SILVER_PATH}/gestores")
    dim_gestor = gestores.dropDuplicates(["gestor_id"])
    dim_gestor.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_gestor")

    calendario = spark.read.parquet(f"{SILVER_PATH}/dim_calendario")
    dim_fecha = calendario.dropDuplicates(["fecha"])
    dim_fecha.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_fecha")

    # ========================
    # FACT TABLES
    # ========================

    deuda = spark.read.parquet(f"{SILVER_PATH}/deuda")

    fact_deuda = deuda.select(
        "debt_id",
        "customer_id",
        "product_id",
        "monto_original",
        "saldo_actual",
        "dias_mora",
        "bucket_mora",
        "fecha_vencimiento",
        "estado_deuda"
    )

    fact_deuda.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_deuda")


    pagos = spark.read.parquet(f"{SILVER_PATH}/pagos")

    fact_pagos = pagos.select(
        "payment_id",
        "debt_id",
        "customer_id",
        "monto_pago",
        "metodo_pago",
        "canal_pago",
        "fecha_pago"
    )

    fact_pagos.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_pagos")


    gestiones = spark.read.parquet(f"{SILVER_PATH}/gestiones_cobranza")

    fact_gestiones = gestiones.select(
        "gestion_id",
        "customer_id",
        "debt_id",
        "gestor_id",
        "canal",
        "resultado",
        "exito_gestion",
        "tiempo_respuesta_min",
        "fecha_gestion"
    )

    fact_gestiones.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_gestiones")

    spark.stop()


if __name__ == "__main__":
    main()