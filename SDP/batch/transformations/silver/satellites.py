# Silver — Satélites del Data Vault
# Cada satélite contiene los atributos descriptivos vinculados al hash key.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Parámetros dinámicos del pipeline ---
SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()   # tus iniciales, ej. dsh = Danny Salas Hernández
if not SUFFIX.isalnum():
    raise ValueError("Configura pipelines.suffix con tus iniciales (ej. dsh) en Settings → Configuration del pipeline")
RAW = spark.conf.get("pipelines.raw_catalog", "raw_data") + "." + spark.conf.get("pipelines.raw_schema", "bronze")
SILVER = spark.conf.get("pipelines.silver_catalog", "data_vault") + "." + spark.conf.get("pipelines.silver_schema", "plata")


@dp.materialized_view(
    name=f"{SILVER}.sat_clientes_{SUFFIX}",
    comment="Satélite de clientes — atributos descriptivos",
)
@dp.expect_or_fail("hk_no_nulo", "customer_hk IS NOT NULL")
def sat_clientes():
    return (
        spark.read.table(f"{RAW}.cumst")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.current_timestamp().alias("load_ts"),
            "batch_id",
            "full_name",
            "country",
            "city",
            "email",
            "phone",
            "status",
            "source_system",
        )
    )


@dp.materialized_view(
    name=f"{SILVER}.sat_prestamos_{SUFFIX}",
    comment="Satélite de préstamos — atributos de producto",
)
@dp.expect_or_fail("hk_no_nulo", "deal_hk IS NOT NULL")
def sat_prestamos():
    return (
        spark.read.table(f"{RAW}.deal")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.current_timestamp().alias("load_ts"),
            "batch_id",
            "customer_id",
            "product_code",
            "currency",
            "principal_balance",
            "interest_rate",
            "open_date",
            "maturity_date",
            "status",
            "source_system",
        )
    )


@dp.materialized_view(
    name=f"{SILVER}.sat_transacciones_{SUFFIX}",
    comment="Satélite de transacciones — detalle transaccional",
)
@dp.expect_or_fail("hk_no_nulo", "trx_hk IS NOT NULL")
@dp.expect("monto_positivo", "traamt > 0")
def sat_transacciones():
    return (
        spark.read.table(f"{RAW}.trans")
        .select(
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            "batch_id",
            "customer_id",
            "deal_id",
            "tracde",
            "tradcc",
            "traamt",
            "traccy",
            "travdm",
            "source_system",
        )
    )
