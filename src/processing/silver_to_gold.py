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


def generate_kpi_cliente():

    logger.info("Generating KPI Cliente")

    clientes = read_parquet_from_gcs("silver/clientes/clientes.parquet")
    deuda = read_parquet_from_gcs("silver/deuda/deuda.parquet")
    pagos = read_parquet_from_gcs("silver/pagos/pagos.parquet")

    deuda_cliente = deuda.groupby("customer_id").agg(
        deuda_total=("monto_original", "sum"),
        saldo_actual=("saldo_actual", "sum")
    ).reset_index()

    pagos_cliente = pagos.groupby("customer_id").agg(
        total_pagado=("monto_pago", "sum")
    ).reset_index()

    df = deuda_cliente.merge(pagos_cliente, on="customer_id", how="left")

    df["total_pagado"] = df["total_pagado"].fillna(0)

    df["ratio_recuperacion"] = df["total_pagado"] / df["deuda_total"]

    upload_parquet(df, "gold/kpi_cliente/kpi_cliente.parquet")


def generate_kpi_gestor():

    logger.info("Generating KPI Gestor")

    gestiones = read_parquet_from_gcs("silver/gestiones_cobranza/gestiones_cobranza.parquet")
    gestores = read_parquet_from_gcs("silver/gestores/gestores.parquet")

    gestiones_agg = gestiones.groupby("gestor_id").agg(
        total_gestiones=("gestion_id", "count"),
        gestiones_exitosas=("exito_gestion", "sum")
    ).reset_index()

    gestiones_agg["tasa_exito"] = (
        gestiones_agg["gestiones_exitosas"] /
        gestiones_agg["total_gestiones"]
    )

    df = gestiones_agg.merge(gestores, on="gestor_id", how="left")

    upload_parquet(df, "gold/kpi_gestor/kpi_gestor.parquet")


def generate_kpi_mora():

    logger.info("Generating KPI Mora")

    deuda = read_parquet_from_gcs("silver/deuda/deuda.parquet")

    df = deuda.groupby("bucket_mora").agg(
        clientes=("customer_id", "nunique"),
        deuda_total=("saldo_actual", "sum")
    ).reset_index()

    upload_parquet(df, "gold/kpi_mora_bucket/kpi_mora_bucket.parquet")


def generate_kpi_cobranza_resumen():

    logger.info("Generating KPI Cobranza Resumen")

    deuda = read_parquet_from_gcs("silver/deuda/deuda.parquet")
    pagos = read_parquet_from_gcs("silver/pagos/pagos.parquet")

    deuda_total = deuda["monto_original"].sum()
    saldo_actual = deuda["saldo_actual"].sum()
    total_pagado = pagos["monto_pago"].sum()

    recovery_rate = total_pagado / deuda_total

    df = pd.DataFrame([{
        "deuda_total": deuda_total,
        "saldo_actual": saldo_actual,
        "total_pagado": total_pagado,
        "recovery_rate": recovery_rate
    }])

    upload_parquet(df, "gold/kpi_cobranza_resumen/kpi_cobranza_resumen.parquet")


def generate_kpi_promesas():

    logger.info("Generating KPI Promesas")

    promesas = read_parquet_from_gcs("silver/promesas_pago/promesas_pago.parquet")

    total = len(promesas)
    cumplidas = promesas["cumplida"].sum()

    tasa = cumplidas / total

    df = pd.DataFrame([{
        "total_promesas": total,
        "promesas_cumplidas": cumplidas,
        "tasa_cumplimiento": tasa
    }])

    upload_parquet(df, "gold/kpi_promesas_pago/kpi_promesas_pago.parquet")


def run():

    logger.info("Starting Silver → Gold pipeline")

    generate_kpi_cliente()
    generate_kpi_gestor()
    generate_kpi_mora()
    generate_kpi_cobranza_resumen()
    generate_kpi_promesas()

    logger.info("Gold layer completed")


if __name__ == "__main__":
    run()