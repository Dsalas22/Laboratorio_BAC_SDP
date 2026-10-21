# Silver — Hubs del Data Vault
# Cada hub contiene la business key deduplicada y su hash SHA-256.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Parámetros dinámicos del pipeline ---
SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()   # tus iniciales, ej. dsh = Danny Salas Hernández
if not SUFFIX.isalnum():
    raise ValueError("Configura pipelines.suffix con tus iniciales (ej. dsh) en Settings → Configuration del pipeline")
RAW = spark.conf.get("pipelines.raw_catalog", "raw_data") + "." + spark.conf.get("pipelines.raw_schema", "bronze")
SILVER = spark.conf.get("pipelines.silver_catalog", "data_vault") + "." + spark.conf.get("pipelines.silver_schema", "plata")


@dp.materialized_view(
    name=f"{SILVER}.hub_clientes_{SUFFIX}",
    comment="Hub de clientes — business key deduplicada",
)
@dp.expect_or_fail("pk_no_nulo", "customer_bk IS NOT NULL")
def hub_clientes():
    return (
        spark.read.table(f"{RAW}.cumst")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.col("customer_id").alias("customer_bk"),
            F.lit("CUMST").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["customer_bk"])
    )


@dp.materialized_view(
    name=f"{SILVER}.hub_prestamos_{SUFFIX}",
    comment="Hub de préstamos — business key deduplicada",
)
@dp.expect_or_fail("pk_no_nulo", "deal_bk IS NOT NULL")
def hub_prestamos():
    return (
        spark.read.table(f"{RAW}.deal")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.col("deal_id").alias("deal_bk"),
            F.lit("DEAL").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["deal_bk"])
    )


@dp.materialized_view(
    name=f"{SILVER}.hub_transacciones_{SUFFIX}",
    comment="Hub de transacciones — business key deduplicada",
)
@dp.expect_or_fail("pk_no_nulo", "trx_bk IS NOT NULL")
def hub_transacciones():
    return (
        spark.read.table(f"{RAW}.trans")
        .select(
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.col("trx_id").alias("trx_bk"),
            F.lit("TRANS").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["trx_bk"])
    )
