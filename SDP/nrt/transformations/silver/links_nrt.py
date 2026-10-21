# Silver NRT — Links del Data Vault (streaming incremental)
# Relaciones deduplicadas por par de hash keys.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Parámetros dinámicos del pipeline ---
SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()   # tus iniciales, ej. dsh = Danny Salas Hernández
if not SUFFIX.isalnum():
    raise ValueError("Configura pipelines.suffix con tus iniciales (ej. dsh) en Settings → Configuration del pipeline")
RAW = spark.conf.get("pipelines.raw_catalog", "raw_data") + "." + spark.conf.get("pipelines.raw_schema", "bronze")
SILVER = spark.conf.get("pipelines.silver_catalog", "data_vault") + "." + spark.conf.get("pipelines.silver_schema", "plata")


@dp.table(
    name=f"{SILVER}.nrt_link_clientes_prestamos_{SUFFIX}",
    comment="Link NRT entre clientes y préstamos",
)
def nrt_link_clientes_prestamos():
    return (
        spark.readStream.table(f"{RAW}.deal")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("DEAL").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "deal_hk"])
    )


@dp.table(
    name=f"{SILVER}.nrt_link_prestamos_transacciones_{SUFFIX}",
    comment="Link NRT entre préstamos y transacciones",
)
def nrt_link_prestamos_transacciones():
    return (
        spark.readStream.table(f"{RAW}.trans")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["deal_hk", "trx_hk"])
    )


@dp.table(
    name=f"{SILVER}.nrt_link_clientes_transacciones_{SUFFIX}",
    comment="Link NRT entre clientes y transacciones",
)
def nrt_link_clientes_transacciones():
    return (
        spark.readStream.table(f"{RAW}.trans")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "trx_hk"])
    )
