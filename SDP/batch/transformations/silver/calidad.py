# Silver — Verificaciones de integridad referencial
# MVs privadas que validan FKs de trans hacia cumst y deal.
# Si hay registros huérfanos, el pipeline falla.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Parámetro dinámico del pipeline ---
RAW = spark.conf.get("pipelines.raw_catalog", "raw_data") + "." + spark.conf.get("pipelines.raw_schema", "bronze")


@dp.materialized_view(private=True)
@dp.expect_or_fail("fk_trans_customer_valida", "tiene_cliente")
def fk_trans_customer_check():
    """Verifica que todo customer_id en trans exista en cumst."""
    return (
        spark.read.table(f"{RAW}.trans")
        .where("customer_id IS NOT NULL")
        .join(
            spark.read.table(f"{RAW}.cumst"),
            "customer_id",
            "left_anti",
        )
        .select("customer_id", F.lit(False).alias("tiene_cliente"))
    )


@dp.materialized_view(private=True)
@dp.expect_or_fail("fk_trans_deal_valida", "tiene_prestamo")
def fk_trans_deal_check():
    """Verifica que todo deal_id en trans exista en deal."""
    return (
        spark.read.table(f"{RAW}.trans")
        .where("deal_id IS NOT NULL")
        .join(
            spark.read.table(f"{RAW}.deal"),
            "deal_id",
            "left_anti",
        )
        .select("deal_id", F.lit(False).alias("tiene_prestamo"))
    )
