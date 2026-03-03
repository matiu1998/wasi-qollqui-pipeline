from google.cloud import storage
import pandas as pd
import io

from src.config.settings import BUCKET_NAME
from src.utils.logger import get_logger

logger = get_logger(__name__)

SILVER_PATH = "silver"
GOLD_PATH = "gold"

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


# =====================================================
# UTILIDADES
# =====================================================

def read_parquet_from_gcs(path):
    blob = bucket.blob(path)
    data = blob.download_as_bytes()
    df = pd.read_parquet(io.BytesIO(data))
    return df


def upload_parquet(df, path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    blob = bucket.blob(path)
    blob.upload_from_string(buffer.getvalue())
    logger.info(f"Gold dataset written: {path}")


# =====================================================
# BUILD DIMENSIONS
# =====================================================

def build_dimensions():

    logger.info("Building Dimensions")

    # DIM CLIENTE
    clientes = read_parquet_from_gcs("silver/clientes/clientes.parquet")
    dim_cliente = clientes.drop_duplicates(subset=["customer_id"])
    upload_parquet(dim_cliente, "gold/dim_cliente/dim_cliente.parquet")

    # DIM PRODUCTO
    productos = read_parquet_from_gcs("silver/productos/productos.parquet")
    dim_producto = productos.drop_duplicates(subset=["product_id"])
    upload_parquet(dim_producto, "gold/dim_producto/dim_producto.parquet")

    # DIM GESTOR
    gestores = read_parquet_from_gcs("silver/gestores/gestores.parquet")
    dim_gestor = gestores.drop_duplicates(subset=["gestor_id"])
    upload_parquet(dim_gestor, "gold/dim_gestor/dim_gestor.parquet")

    # DIM FECHA
    calendario = read_parquet_from_gcs("silver/dim_calendario/dim_calendario.parquet")
    dim_fecha = calendario.drop_duplicates(subset=["fecha"])
    upload_parquet(dim_fecha, "gold/dim_fecha/dim_fecha.parquet")


# =====================================================
# BUILD FACT TABLES
# =====================================================

def build_facts():

    logger.info("Building Fact Tables")

    # FACT DEUDA
    deuda = read_parquet_from_gcs("silver/deuda/deuda.parquet")

    fact_deuda = deuda[
        [
            "debt_id",
            "customer_id",
            "product_id",
            "monto_original",
            "saldo_actual",
            "dias_mora",
            "bucket_mora",
            "fecha_vencimiento",
            "estado_deuda",
        ]
    ]

    upload_parquet(fact_deuda, "gold/fact_deuda/fact_deuda.parquet")

    # FACT PAGOS
    pagos = read_parquet_from_gcs("silver/pagos/pagos.parquet")

    fact_pagos = pagos[
        [
            "payment_id",
            "debt_id",
            "customer_id",
            "monto_pago",
            "metodo_pago",
            "canal_pago",
            "fecha_pago",
        ]
    ]

    upload_parquet(fact_pagos, "gold/fact_pagos/fact_pagos.parquet")

    # FACT GESTIONES
    gestiones = read_parquet_from_gcs(
        "silver/gestiones_cobranza/gestiones_cobranza.parquet"
    )

    fact_gestiones = gestiones[
        [
            "gestion_id",
            "customer_id",
            "debt_id",
            "gestor_id",
            "canal",
            "resultado",
            "exito_gestion",
            "tiempo_respuesta_min",
            "fecha_gestion",
        ]
    ]

    upload_parquet(fact_gestiones, "gold/fact_gestiones/fact_gestiones.parquet")

    # FACT PROMESAS
    promesas = read_parquet_from_gcs(
        "silver/promesas_pago/promesas_pago.parquet"
    )

    fact_promesas = promesas[
        [
            "promesa_id",
            "customer_id",
            "debt_id",
            "monto_prometido",
            "fecha_promesa",
            "cumplida",
        ]
    ]

    upload_parquet(fact_promesas, "gold/fact_promesas/fact_promesas.parquet")


# =====================================================
# RUN PIPELINE
# =====================================================

def run():

    logger.info("Starting Silver → Gold (Dimensional Model)")

    build_dimensions()
    build_facts()

    logger.info("Gold Model Completed Successfully 🚀")


if __name__ == "__main__":
    run()