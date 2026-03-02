from pyspark.sql import SparkSession, functions as F
import os
import re

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("wasi-qollqui-pipeline")
    .getOrCreate()
)

# ============================================================
# Config
# - BUCKET llega por: --properties=spark.wasi.bucket=...
# - DATASET y PROJECT se obtienen por env / defaults
# ============================================================
BUCKET = spark.conf.get("spark.wasi.bucket", None)
if not BUCKET:
    raise ValueError("No se encontró spark.wasi.bucket. Pásalo con --properties=spark.wasi.bucket=TU_BUCKET")

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT") or ""
DATASET = os.environ.get("BQ_DATASET", "wasi_qollqui")  # si quieres, luego lo pasamos por env en GitHub Actions

bronze = f"gs://{BUCKET}/bronze/csv"
silver = f"gs://{BUCKET}/silver/parquet"
gold   = f"gs://{BUCKET}/gold/parquet"

# ============================================================
# Helpers
# ============================================================
def normalize_cols(df):
    """Normaliza nombres: lower, trim, espacios->_, quita símbolos raros."""
    for c in df.columns:
        new_c = c.strip().lower()
        new_c = re.sub(r"\s+", "_", new_c)
        new_c = re.sub(r"[^a-z0-9_]", "", new_c)
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df

def to_double(df, colname):
    """Convierte una columna a double (soporta separadores ,)."""
    if colname not in df.columns:
        return df
    return df.withColumn(
        colname,
        F.regexp_replace(F.col(colname).cast("string"), ",", ".").cast("double")
    )

def pick_col(df, candidates):
    """Devuelve el primer nombre de columna existente en df.columns."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def read_csv(name):
    """Lee CSV de bronze/<name>.csv con inferSchema."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{bronze}/{name}.csv")
    )
    return normalize_cols(df)

# ============================================================
# Read Bronze (CSV)
# ============================================================
clientes = read_csv("clientes")
deuda = read_csv("deuda")
pagos = read_csv("pagos")
promesas_pago = read_csv("promesas_pago")
gestiones = read_csv("gestiones_cobranza")
productos = read_csv("productos")

# ============================================================
# Identify key columns (tolerante a nombres distintos)
# ============================================================
cliente_id_col = pick_col(clientes, ["customer_id", "cliente_id", "id_cliente", "idcustomer", "id"])
deuda_cliente_col = pick_col(deuda, ["customer_id", "cliente_id", "id_cliente", "idcustomer", "id"])
pagos_cliente_col = pick_col(pagos, ["customer_id", "cliente_id", "id_cliente", "idcustomer", "id"])

monto_deuda_col = pick_col(deuda, ["monto_deuda", "deuda", "importe_deuda", "amount_debt", "monto"])
monto_pago_col  = pick_col(pagos, ["monto_pagado", "monto_pago", "pago", "importe_pago", "amount_paid", "monto"])

if not cliente_id_col:
    raise ValueError("No se encontró columna de ID en clientes. Revisa nombres de columnas.")
if not deuda_cliente_col or not pagos_cliente_col:
    raise ValueError("No se encontró customer_id/cliente_id en deuda o pagos.")
if not monto_deuda_col or not monto_pago_col:
    raise ValueError("No se encontró monto en deuda o pagos (monto_deuda / monto_pagado).")

# normalizamos tipos numéricos
deuda = to_double(deuda, monto_deuda_col)
pagos = to_double(pagos, monto_pago_col)

# ============================================================
# Silver (limpieza + escritura Parquet)
# ============================================================
# Si quieres reglas de calidad más estrictas, acá es donde van.
clientes_s = clientes.dropDuplicates([cliente_id_col])
deuda_s = deuda
pagos_s = pagos
promesas_s = promesas_pago
gestiones_s = gestiones
productos_s = productos

# Write Silver (Parquet)
clientes_s.write.mode("overwrite").parquet(f"{silver}/clientes")
deuda_s.write.mode("overwrite").parquet(f"{silver}/deuda")
pagos_s.write.mode("overwrite").parquet(f"{silver}/pagos")
promesas_s.write.mode("overwrite").parquet(f"{silver}/promesas_pago")
gestiones_s.write.mode("overwrite").parquet(f"{silver}/gestiones_cobranza")
productos_s.write.mode("overwrite").parquet(f"{silver}/productos")

# ============================================================
# Gold (KPIs)
# KPI principal: kpi_cliente (para BI)
# ============================================================
deuda_by_cliente = (
    deuda_s.groupBy(F.col(deuda_cliente_col).alias("customer_id"))
    .agg(F.sum(F.col(monto_deuda_col)).alias("total_deuda"))
)

pagos_by_cliente = (
    pagos_s.groupBy(F.col(pagos_cliente_col).alias("customer_id"))
    .agg(F.sum(F.col(monto_pago_col)).alias("total_pagado"))
)

kpi_cliente = (
    deuda_by_cliente.join(pagos_by_cliente, on="customer_id", how="left")
    .na.fill({"total_pagado": 0.0})
    .withColumn("deuda_pendiente", F.col("total_deuda") - F.col("total_pagado"))
    .withColumn(
        "tasa_recuperacion",
        F.when(F.col("total_deuda") > 0, F.col("total_pagado") / F.col("total_deuda")).otherwise(F.lit(0.0))
    )
)

# Write Gold Parquet
kpi_cliente.write.mode("overwrite").parquet(f"{gold}/kpi_cliente")

# ============================================================
# (Opcional pero recomendado) Publicar KPI en BigQuery
# Esto hace que exista ${DATASET}.kpi_cliente para que tu step bq funcione siempre.
# ============================================================
# Requiere conector BigQuery (Dataproc normalmente lo incluye)
try:
    if PROJECT_ID:
        (
            kpi_cliente.write.format("bigquery")
            .option("table", f"{PROJECT_ID}.{DATASET}.kpi_cliente")
            .option("temporaryGcsBucket", BUCKET)
            .mode("overwrite")
            .save()
        )
        print(f"[OK] Tabla BigQuery creada/actualizada: {PROJECT_ID}.{DATASET}.kpi_cliente")
    else:
        print("[WARN] No se pudo detectar PROJECT_ID (GOOGLE_CLOUD_PROJECT). Se omitió escritura a BigQuery.")
except Exception as e:
    print(f"[WARN] No se pudo escribir a BigQuery (se mantiene Gold en GCS). Detalle: {e}")

spark.stop()