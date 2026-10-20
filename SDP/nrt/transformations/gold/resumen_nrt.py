# Gold NRT — Tablas analíticas (materialized views sobre streaming tables)
# Lectura batch desde Silver STs para agregaciones, publicadas en ods.gold.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Parámetros dinámicos del pipeline ---
SUFFIX = spark.conf.get("pipelines.suffix", "dsh")
SILVER = spark.conf.get("pipelines.silver_catalog", "data_vault") + "." + spark.conf.get("pipelines.silver_schema", "plata")
GOLD = spark.conf.get("pipelines.gold_catalog", "ods") + "." + spark.conf.get("pipelines.gold_schema", "gold")


@dp.materialized_view(
    name=f"{GOLD}.nrt_resumen_clientes_{SUFFIX}",
    comment="Resumen NRT de clientes por país y estado",
)
def nrt_resumen_clientes_dsh():
    return (
        spark.read.table(f"{SILVER}.nrt_sat_clientes_{SUFFIX}")
        .groupBy("country", "status")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("email").alias("distinct_emails"),
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.nrt_resumen_prestamos_{SUFFIX}",
    comment="Resumen NRT de préstamos por moneda y estado",
)
def nrt_resumen_prestamos_dsh():
    return (
        spark.read.table(f"{SILVER}.nrt_sat_prestamos_{SUFFIX}")
        .groupBy("currency", "status")
        .agg(
            F.count("*").alias("deal_count"),
            F.sum("principal_balance").alias("principal_balance_total"),
            F.avg("interest_rate").alias("avg_interest_rate"),
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.nrt_resumen_transacciones_{SUFFIX}",
    comment="Resumen NRT de transacciones por mes, código, tipo y moneda",
)
def nrt_resumen_transacciones_dsh():
    return (
        spark.read.table(f"{SILVER}.nrt_sat_transacciones_{SUFFIX}")
        .withColumn("transaction_month", F.date_format("travdm", "yyyy-MM"))
        .groupBy("transaction_month", "tracde", "tradcc", "traccy")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("traamt").alias("total_amount"),
            F.avg("traamt").alias("avg_amount"),
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.nrt_kpi_general_{SUFFIX}",
    comment="KPIs NRT mensuales generales",
)
def nrt_kpi_general_dsh():
    return (
        spark.read.table(f"{GOLD}.nrt_resumen_transacciones_{SUFFIX}")
        .groupBy("transaction_month")
        .agg(
            F.sum("transaction_count").alias("monthly_transactions"),
            F.sum("total_amount").alias("monthly_total_amount"),
        )
    )
