# Silver — Links del Data Vault
# Cada link representa una relación entre entidades, deduplicada por par de hash keys.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Parámetros dinámicos del pipeline ---
SUFFIX = spark.conf.get("pipelines.suffix", "dsh")
RAW = spark.conf.get("pipelines.raw_catalog", "raw_data") + "." + spark.conf.get("pipelines.raw_schema", "bronze")
SILVER = spark.conf.get("pipelines.silver_catalog", "data_vault") + "." + spark.conf.get("pipelines.silver_schema", "plata")


@dp.materialized_view(
    name=f"{SILVER}.link_clientes_prestamos_{SUFFIX}",
    comment="Link entre clientes y préstamos",
)
def link_clientes_prestamos_dsh():
    return (
        spark.read.table(f"{RAW}.deal")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("DEAL").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "deal_hk"])
    )


@dp.materialized_view(
    name=f"{SILVER}.link_prestamos_transacciones_{SUFFIX}",
    comment="Link entre préstamos y transacciones",
)
def link_prestamos_transacciones_dsh():
    return (
        spark.read.table(f"{RAW}.trans")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["deal_hk", "trx_hk"])
    )


@dp.materialized_view(
    name=f"{SILVER}.link_clientes_transacciones_{SUFFIX}",
    comment="Link entre clientes y transacciones",
)
def link_clientes_transacciones_dsh():
    return (
        spark.read.table(f"{RAW}.trans")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "trx_hk"])
    )
