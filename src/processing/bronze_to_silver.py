from google.cloud import storage
import pandas as pd
import io

from src.config.settings import BUCKET_NAME
from src.utils.logger import get_logger

logger = get_logger(__name__)

# -----------------------------
# DATA LAKE PATHS
# -----------------------------

BRONZE_PATH = "bronze"
SILVER_PATH = "silver"

# -----------------------------
# GCS CLIENT
# -----------------------------

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# -----------------------------
# PROCESS TABLE
# -----------------------------

def process_table(table_name):

    logger.info(f"Processing table: {table_name}")

    # Ruta archivo Bronze
    blob_path = f"{BRONZE_PATH}/{table_name}/{table_name}.parquet"
    blob = bucket.blob(blob_path)

    # Descargar archivo desde GCS
    data = blob.download_as_bytes()

    # Leer parquet
    df = pd.read_parquet(io.BytesIO(data))

    # -----------------------------
    # LIMPIEZA (Silver Layer)
    # -----------------------------

    # estandarizar nombres columnas
    df.columns = df.columns.str.lower().str.strip()

    # eliminar duplicados
    df = df.drop_duplicates()

    # -----------------------------
    # GUARDAR EN SILVER
    # -----------------------------

    output_buffer = io.BytesIO()

    df.to_parquet(output_buffer, index=False)

    silver_blob = bucket.blob(
        f"{SILVER_PATH}/{table_name}/{table_name}.parquet"
    )

    silver_blob.upload_from_string(output_buffer.getvalue())

    logger.info(f"Silver created: {table_name}")


# -----------------------------
# PIPELINE RUNNER
# -----------------------------

def run():

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

    logger.info("Starting Bronze → Silver pipeline")

    for table in tables:
        process_table(table)

    logger.info("Silver pipeline completed")


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":
    run()