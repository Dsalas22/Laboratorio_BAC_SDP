# Guía Paso a Paso: Spark Declarative Pipelines (SDP)

## Para Ingenieros de Datos Jr — Laboratorio BAC

---

## Duración estimada: 3 horas (1:00 p.m. – 4:00 p.m.)

| Bloque | Contenido | Tiempo |
|---|---|---:|
| **Hora 1** | Conceptos + Bronze + Silver Batch (hubs, satellites, links, calidad) | 60 min |
| **Hora 2** | Gold Batch + Dry Run + Ejecución + Verificación SQL | 60 min |
| **Hora 3** | Pipeline NRT completo + Verificar incrementalidad + Retos | 60 min |

> **Instrucción para el facilitador**: Los ingenieros deben **tipear el código manualmente**, no copiar y pegar. El objetivo es interiorizar la sintaxis SDP.

---

## ¿Por qué SDP en vez de notebooks?

En este laboratorio existen dos enfoques: notebooks manuales (`Batch/notebooks/`) y pipelines declarativos (SDP). **Para producción se recomienda SDP**:

| Aspecto | Notebooks manuales | SDP (Spark Declarative Pipelines) |
|---|---|---|
| Escritura de datos | Tú manejas `.write.saveAsTable()` | El framework escribe automáticamente |
| Dependencias | Tú decides el orden de ejecución | Se resuelven automáticamente por el DAG |
| Calidad de datos | Código custom de validación | Expectations declarativas integradas |
| Procesamiento incremental | Lógica manual con filtros por ventana | Streaming Tables nativas |
| Reprocesamiento | Borrar y volver a ejecutar todo | Un clic: "Full Refresh" |
| Monitoreo | `print()` y conteos manuales | UI visual con métricas, lineage y logs |
| Producción | Requiere orquestación externa (Jobs) | Un solo pipeline auto-gestionado |

> **Regla de oro**: Los notebooks son excelentes para explorar y aprender PySpark. Pero cuando vas a producción, SDP es el estándar en Databricks.

---

## Conceptos clave antes de empezar

### Arquitectura Medallion (Bronze → Silver → Gold)

```
Bronze (datos crudos)  →  Silver (limpio/modelado)  →  Gold (analítico/negocio)
   raw_data.bronze          data_vault.plata              ods.gold
```

- **Bronze**: Datos tal como llegan del sistema fuente (AS400 en nuestro caso). Sin transformación.
- **Silver**: Datos limpios y modelados — usamos **Data Vault** (hubs, satélites, links). Deduplicados y con hash keys.
- **Gold**: Agregados de negocio listos para dashboards y reportes. Consumidos por analistas y aplicaciones.

### Data Vault — ¿Qué son Hubs, Satélites y Links?

El **Data Vault** es un patrón de modelado diseñado para auditoría y trazabilidad:

- **Hub**: Una tabla con la **business key** única (ej: `customer_id`) y su **hash SHA-256**. Nunca se modifica. Es la identidad.
- **Satélite**: Los **atributos descriptivos** vinculados al hub (ej: nombre, email, teléfono). Puede tener múltiples versiones históricas.
- **Link**: Una tabla que **conecta dos hubs** (ej: qué cliente tiene qué préstamo). Modela relaciones.

```
  ┌──────────┐         ┌──────────────┐         ┌──────────┐
  │ hub_     │◄────────│ link_        │────────►│ hub_     │
  │ clientes │         │ clientes_    │         │ préstamos│
  │ (PK)     │         │ préstamos    │         │ (PK)     │
  └────┬─────┘         └──────────────┘         └────┬─────┘
       │                                              │
  ┌────▼─────┐                                   ┌────▼─────┐
  │ sat_     │                                   │ sat_     │
  │ clientes │                                   │ préstamos│
  │ (attrs)  │                                   │ (attrs)  │
  └──────────┘                                   └──────────┘
```

### ¿Por qué usar SHA-256 como hash key?

- El hash key (`customer_hk`) es un identificador **determinístico** derivado de la business key (`customer_id`)
- Permite hacer joins entre hubs, satélites y links sin depender del valor original
- SHA-256 genera un hash de 64 caracteres hexadecimales, prácticamente sin colisiones
- Es un estándar de la industria para Data Vault en producción

### Tipos de dataset en SDP

| Tipo | Decorador | Lectura | Comportamiento |
|---|---|---|---|
| **Materialized View (MV)** | `@dp.materialized_view()` | `spark.read.table()` | Recalcula **todo** en cada ejecución |
| **Streaming Table (ST)** | `@dp.table()` | `spark.readStream.table()` | Procesa solo datos **nuevos** |
| **Expectation** | `@dp.expect*()` | (se aplica sobre el dataset) | Regla de calidad por registro |

### Tipos de Expectations

| Decorador | Qué pasa si un registro falla la condición |
|---|---|
| `@dp.expect("nombre", "condición")` | **Registra la violación** pero el registro se incluye. Para monitoreo. |
| `@dp.expect_or_drop("nombre", "condición")` | **Descarta el registro** silenciosamente. Para limpieza automática. |
| `@dp.expect_or_fail("nombre", "condición")` | **Detiene el pipeline**. Para reglas críticas (PKs no nulas). |

### Entidades del laboratorio

| Entidad Bronze | Descripción | Filas | Columnas clave |
|---|---|---:|---|
| `raw_data.bronze.cumst` | Clientes | 500 | customer_id (PK), full_name, country, city, email, status |
| `raw_data.bronze.deal` | Préstamos | 500 | deal_id (PK), customer_id (FK), product_code, principal_balance, currency |
| `raw_data.bronze.trans` | Transacciones | 1,500 | trx_id (PK), customer_id (FK), deal_id (FK), traamt, travdm, traccy |

> **Nota**: Cada cliente tiene 1 préstamo y 3 transacciones en los datos sintéticos.

---

## Prerrequisitos

1. Acceso a un Databricks Workspace con Unity Catalog habilitado
2. Permisos para crear catálogos y esquemas (o que ya existan `raw_data`, `data_vault`, `ods`)
3. Las tablas Bronze generadas por el instructor con `Batch/notebooks/01_bronze_generate_sdp` en modo `inicial` (500 / 500 / 1,500)
4. Serverless compute habilitado en el workspace

---

# PARTE 1: Pipeline Batch (Materialized Views)

> En esta parte construimos un pipeline que **recalcula todas las tablas** en cada ejecución usando Materialized Views.

## Paso 1 — Revisar los datos Bronze ⏱️ 10 min

> **Antes del taller (solo el instructor):** ejecutar `Batch/notebooks/01_bronze_generate_sdp` con `modo = inicial` y `multiplicador_espera = 0` (tarda ~1-2 min). Si ya existen pipelines NRT de pruebas anteriores, hacerles **Full refresh** después, porque el modo inicial reescribe Bronze.
>
> **Bronze es compartido.** Los ingenieros **no** ejecutan el notebook generador: si alguien lo corre en modo `inicial`, reescribe las tablas de todos y rompe los pipelines NRT del resto.

1. Abre el notebook `Batch/notebooks/01_bronze_generate_sdp` y lee la primera celda: explica los modos `inicial` (reinicia, `overwrite`) e `incremental` (agrega un lote, `append`).
2. Verifica las tablas:

```sql
SELECT 'cumst' AS tabla, COUNT(*) AS filas FROM raw_data.bronze.cumst
UNION ALL
SELECT 'deal', COUNT(*) FROM raw_data.bronze.deal
UNION ALL
SELECT 'trans', COUNT(*) FROM raw_data.bronze.trans;
```

Resultado esperado:

| tabla | filas |
|---|---:|
| cumst | 500 |
| deal | 500 |
| trans | 1,500 |

3. Mira los lotes cargados. Cada ejecución del generador tiene su propio `batch_id` (`INI-...` para inicial, `INC-...` para incremental):

```sql
SELECT batch_id, MIN(ingest_ts) AS ingest_ts, COUNT(*) AS filas
FROM raw_data.bronze.trans
GROUP BY batch_id
ORDER BY ingest_ts;
```

> **¿Qué acaba de pasar?** Revisaste los datos sintéticos que simulan un sistema bancario (AS400): clientes, préstamos y transacciones. Son la materia prima de los pipelines. En el Paso 18 llegará un segundo lote para probar el procesamiento incremental.

## Paso 2 — Crear el pipeline en Databricks ⏱️ 5 min

1. Ve a la barra lateral izquierda → **Pipelines** → **Create pipeline**
2. Configura:
   - **Nombre**: `SDP_Batch_Laboratorio_dsh` (cambia `dsh` por tus iniciales)
   - **Catalog**: `data_vault`
   - **Serverless**: activado
   - **Photon**: activado
3. Haz clic en **Create**
4. Databricks crea una carpeta con `transformations/` — ahí van tus archivos Python

> **¿Qué acaba de pasar?** Creaste un pipeline vacío. El pipeline tiene un catálogo default (`data_vault`) pero puedes publicar tablas en **cualquier catálogo** usando nombres fully-qualified como `ods.gold.mi_tabla`.

### ¿Qué es un nombre fully-qualified?

Es el nombre completo de una tabla: `catalogo.schema.tabla`. Ejemplo:

```
data_vault.plata.hub_clientes_dsh
│            │         │
│            │         └─ Nombre de la tabla
│            └─ Esquema (schema)
└─ Catálogo
```

## Paso 3 — Crear la estructura de archivos ⏱️ 5 min

Dentro de la carpeta `transformations/` del pipeline, crea esta estructura:

```
transformations/
├── silver/
│   ├── hubs.py          ← 3 hubs del Data Vault
│   ├── satellites.py    ← 3 satélites
│   ├── links.py         ← 3 links
│   └── calidad.py       ← 2 verificaciones de integridad referencial
└── gold/
    └── resumen.py       ← 4 tablas analíticas
```

Para crear los archivos:
1. En el panel izquierdo del editor, haz clic derecho en `transformations/`
2. **New folder** → `silver`
3. Dentro de `silver/`, clic derecho → **New file** → `hubs.py`
4. Repite para cada archivo

> **¿Por qué archivos separados?** Cada archivo es un módulo independiente. El pipeline los descubre automáticamente gracias al patrón `transformations/**` en la configuración. Esto facilita mantenimiento y colaboración.

---

## Paso 4 — Escribir los Hubs (Silver) ⏱️ 15 min

**Archivo**: `transformations/silver/hubs.py`

Este archivo crea los 3 **hubs** del Data Vault: la tabla con la identidad única de cada entidad.

```python
# Silver — Hubs del Data Vault
# Cada hub contiene la business key deduplicada y su hash SHA-256.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="data_vault.plata.hub_clientes_dsh",
    comment="Hub de clientes — business key deduplicada",
)
@dp.expect_or_fail("pk_no_nulo", "customer_bk IS NOT NULL")
def hub_clientes_dsh():
    return (
        spark.read.table("raw_data.bronze.cumst")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.col("customer_id").alias("customer_bk"),
            F.lit("CUMST").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["customer_bk"])
    )


@dp.materialized_view(
    name="data_vault.plata.hub_prestamos_dsh",
    comment="Hub de préstamos — business key deduplicada",
)
@dp.expect_or_fail("pk_no_nulo", "deal_bk IS NOT NULL")
def hub_prestamos_dsh():
    return (
        spark.read.table("raw_data.bronze.deal")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.col("deal_id").alias("deal_bk"),
            F.lit("DEAL").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["deal_bk"])
    )


@dp.materialized_view(
    name="data_vault.plata.hub_transacciones_dsh",
    comment="Hub de transacciones — business key deduplicada",
)
@dp.expect_or_fail("pk_no_nulo", "trx_bk IS NOT NULL")
def hub_transacciones_dsh():
    return (
        spark.read.table("raw_data.bronze.trans")
        .select(
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.col("trx_id").alias("trx_bk"),
            F.lit("TRANS").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["trx_bk"])
    )
```

### Explicación línea por línea

| Línea | Qué hace | Por qué |
|---|---|---|
| `from pyspark import pipelines as dp` | Importa el framework SDP | Necesario en **todo** archivo `.py` del pipeline |
| `from pyspark.sql import functions as F` | Importa funciones SQL de Spark | Para `sha2`, `col`, `lit`, `current_timestamp` |
| `@dp.materialized_view(name="...")` | Declara una tabla MV | Se recalcula completa en cada ejecución |
| `name="data_vault.plata.hub_clientes_dsh"` | Nombre fully-qualified | Publica en catálogo `data_vault`, esquema `plata` |
| `comment="..."` | Descripción en Unity Catalog | Aparece en el explorador de datos |
| `@dp.expect_or_fail("pk_no_nulo", ...)` | Expectation crítica | Si algún registro tiene BK nula → **falla el pipeline** |
| `spark.read.table("raw_data.bronze.cumst")` | Lee tabla Bronze completa | `spark.read` = lectura batch (todo de una vez) |
| `F.sha2(F.col("customer_id"), 256)` | Genera hash SHA-256 | Hash key estándar de Data Vault |
| `.alias("customer_hk")` | Renombra la columna | `hk` = hash key (convención Data Vault) |
| `F.col("customer_id").alias("customer_bk")` | Copia la business key | `bk` = business key (el valor original) |
| `F.lit("CUMST").alias("record_source")` | Registra la fuente | Trazabilidad: de qué tabla vino cada registro |
| `F.current_timestamp().alias("load_ts")` | Marca temporal de carga | Auditoría: cuándo se cargó |
| `.dropDuplicates(["customer_bk"])` | Elimina duplicados | Un hub tiene **una sola fila** por business key |

> **Importante**: Tú **NO** escribes `.write.saveAsTable(...)`. El pipeline lo hace por ti. Tu función solo retorna el DataFrame.

---

## Paso 5 — Escribir los Satélites (Silver) ⏱️ 10 min

**Archivo**: `transformations/silver/satellites.py`

Los satélites guardan los **atributos descriptivos** de cada entidad, vinculados por el hash key.

```python
# Silver — Satélites del Data Vault
# Cada satélite contiene los atributos descriptivos vinculados al hash key.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="data_vault.plata.sat_clientes_dsh",
    comment="Satélite de clientes — atributos descriptivos",
)
@dp.expect_or_fail("hk_no_nulo", "customer_hk IS NOT NULL")
def sat_clientes_dsh():
    return (
        spark.read.table("raw_data.bronze.cumst")
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
    name="data_vault.plata.sat_prestamos_dsh",
    comment="Satélite de préstamos — atributos de producto",
)
@dp.expect_or_fail("hk_no_nulo", "deal_hk IS NOT NULL")
def sat_prestamos_dsh():
    return (
        spark.read.table("raw_data.bronze.deal")
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
    name="data_vault.plata.sat_transacciones_dsh",
    comment="Satélite de transacciones — detalle transaccional",
)
@dp.expect_or_fail("hk_no_nulo", "trx_hk IS NOT NULL")
@dp.expect("monto_positivo", "traamt > 0")
def sat_transacciones_dsh():
    return (
        spark.read.table("raw_data.bronze.trans")
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
```

### Diferencia entre Hub y Satélite

| Aspecto | Hub | Satélite |
|---|---|---|
| Contenido | Solo business key + hash | Atributos descriptivos |
| Deduplicación | `dropDuplicates` por BK | Sin dedup (puede versionar) |
| Expectation | BK no nula | HK no nula |
| Tamaño | Mínimo (2-4 columnas) | Variable (muchos atributos) |

> **Nota**: `sat_transacciones_dsh` tiene **dos expectations**: una crítica (`expect_or_fail` para HK nula) y una de monitoreo (`expect` para montos positivos). La segunda no detiene el pipeline — solo registra violaciones.

---

## Paso 6 — Escribir los Links (Silver) ⏱️ 10 min

**Archivo**: `transformations/silver/links.py`

Los links modelan **relaciones entre entidades**, deduplicadas por par de hash keys.

```python
# Silver — Links del Data Vault
# Cada link representa una relación entre entidades, deduplicada por par de hash keys.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="data_vault.plata.link_clientes_prestamos_dsh",
    comment="Link entre clientes y préstamos",
)
def link_clientes_prestamos_dsh():
    return (
        spark.read.table("raw_data.bronze.deal")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("DEAL").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "deal_hk"])
    )


@dp.materialized_view(
    name="data_vault.plata.link_prestamos_transacciones_dsh",
    comment="Link entre préstamos y transacciones",
)
def link_prestamos_transacciones_dsh():
    return (
        spark.read.table("raw_data.bronze.trans")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["deal_hk", "trx_hk"])
    )


@dp.materialized_view(
    name="data_vault.plata.link_clientes_transacciones_dsh",
    comment="Link entre clientes y transacciones",
)
def link_clientes_transacciones_dsh():
    return (
        spark.read.table("raw_data.bronze.trans")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "trx_hk"])
    )
```

### ¿Por qué `dropDuplicates` con dos columnas?

Un link registra una **relación única** entre dos entidades. Si el mismo par (cliente, préstamo) aparece dos veces en Bronze, solo queremos un link. El `dropDuplicates(["customer_hk", "deal_hk"])` garantiza unicidad del par.

### ¿De dónde lee cada link?

| Link | Fuente Bronze | Relación que modela |
|---|---|---|
| `link_clientes_prestamos` | `deal` | ¿Qué cliente tiene qué préstamo? |
| `link_prestamos_transacciones` | `trans` | ¿Qué préstamo generó qué transacción? |
| `link_clientes_transacciones` | `trans` | ¿Qué cliente hizo qué transacción? |

---

## Paso 7 — Verificaciones de integridad referencial ⏱️ 10 min

**Archivo**: `transformations/silver/calidad.py`

Este archivo crea **MVs privadas** que validan que las foreign keys de `trans` existan en `cumst` y `deal`. Si hay huérfanos, el pipeline falla.

```python
# Silver — Verificaciones de integridad referencial
# MVs privadas que validan FKs de trans hacia cumst y deal.
# Si hay registros huérfanos, el pipeline falla.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(private=True)
@dp.expect_or_fail("fk_trans_customer_valida", "tiene_cliente")
def fk_trans_customer_check():
    # Verifica que todo customer_id en trans exista en cumst.
    return (
        spark.read.table("raw_data.bronze.trans")
        .where("customer_id IS NOT NULL")
        .join(
            spark.read.table("raw_data.bronze.cumst"),
            "customer_id",
            "left_anti",
        )
        .select("customer_id", F.lit(False).alias("tiene_cliente"))
    )


@dp.materialized_view(private=True)
@dp.expect_or_fail("fk_trans_deal_valida", "tiene_prestamo")
def fk_trans_deal_check():
    # Verifica que todo deal_id en trans exista en deal.
    return (
        spark.read.table("raw_data.bronze.trans")
        .where("deal_id IS NOT NULL")
        .join(
            spark.read.table("raw_data.bronze.deal"),
            "deal_id",
            "left_anti",
        )
        .select("deal_id", F.lit(False).alias("tiene_prestamo"))
    )
```

### Conceptos nuevos en este archivo

#### `private=True`

- Estas tablas **NO se publican** en Unity Catalog
- Son internas al pipeline, invisibles para usuarios externos
- Útiles para validaciones que no necesitan consulta externa
- No aparecen en el catálogo de datos

#### `left_anti` join — el detector de huérfanos

Un `left_anti` join devuelve filas de la tabla izquierda que **NO tienen match** en la tabla derecha:

```
trans (izq)          cumst (der)          left_anti (resultado)
───────────          ───────────          ─────────────────────
customer_id=1        customer_id=1        (vacío si todos existen)
customer_id=2        customer_id=2
customer_id=3        customer_id=3
customer_id=999                           customer_id=999  ← huérfano!
```

#### ¿Cómo funciona la validación?

1. El `left_anti` devuelve registros **huérfanos** (FKs que no existen en la tabla padre)
2. A cada huérfano le agrega `tiene_cliente = False`
3. La expectation `@dp.expect_or_fail` verifica que `tiene_cliente` sea `True`
4. Como todos los huérfanos tienen `False`, si hay **cualquier registro** → **falla el pipeline**
5. Si no hay huérfanos (0 filas), la expectation pasa exitosamente

> **Resultado esperado**: 0 filas en cada check (todos los FKs son válidos en nuestros datos sintéticos).

---

## Paso 8 — Escribir la capa Gold ⏱️ 15 min

**Archivo**: `transformations/gold/resumen.py`

Gold contiene **tablas analíticas** derivadas del Data Vault Silver. Son agregados listos para dashboards.

```python
# Gold — Tablas analíticas de negocio
# Agregados derivados del Data Vault Silver, publicados en ods.gold.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="ods.gold.resumen_clientes_dsh",
    comment="Resumen de clientes por país y estado",
)
def resumen_clientes_dsh():
    return (
        spark.read.table("data_vault.plata.sat_clientes_dsh")
        .groupBy("country", "status")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("email").alias("distinct_emails"),
        )
    )


@dp.materialized_view(
    name="ods.gold.resumen_prestamos_dsh",
    comment="Resumen de préstamos por moneda y estado",
)
def resumen_prestamos_dsh():
    return (
        spark.read.table("data_vault.plata.sat_prestamos_dsh")
        .groupBy("currency", "status")
        .agg(
            F.count("*").alias("deal_count"),
            F.sum("principal_balance").alias("principal_balance_total"),
            F.avg("interest_rate").alias("avg_interest_rate"),
        )
    )


@dp.materialized_view(
    name="ods.gold.resumen_transacciones_dsh",
    comment="Resumen de transacciones por mes, código, tipo y moneda",
)
def resumen_transacciones_dsh():
    return (
        spark.read.table("data_vault.plata.sat_transacciones_dsh")
        .withColumn("transaction_month", F.date_format("travdm", "yyyy-MM"))
        .groupBy("transaction_month", "tracde", "tradcc", "traccy")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("traamt").alias("total_amount"),
            F.avg("traamt").alias("avg_amount"),
        )
    )


@dp.materialized_view(
    name="ods.gold.kpi_general_dsh",
    comment="KPIs mensuales generales derivados de transacciones",
)
def kpi_general_dsh():
    return (
        spark.read.table("ods.gold.resumen_transacciones_dsh")
        .groupBy("transaction_month")
        .agg(
            F.sum("transaction_count").alias("monthly_transactions"),
            F.sum("total_amount").alias("monthly_total_amount"),
        )
    )
```

### ¿Por qué Gold lee de Silver y no de Bronze?

Porque Gold consume datos **ya limpios y modelados**. La capa Silver (Data Vault) ya aplicó hashing, dedup y validación de calidad. Gold solo agrega.

### ¿Cómo sabe el pipeline el orden de ejecución?

No necesitas especificarlo. El pipeline analiza las dependencias automáticamente creando un **DAG** (Directed Acyclic Graph):

```
raw_data.bronze.cumst  ──→  hub_clientes_dsh     ──┐
                        ──→  sat_clientes_dsh     ──┤──→  resumen_clientes_dsh
                                                    │
raw_data.bronze.deal   ──→  hub_prestamos_dsh    ──┤
                        ──→  sat_prestamos_dsh    ──┤──→  resumen_prestamos_dsh
                                                    │
raw_data.bronze.trans  ──→  hub_transacciones_dsh ──┤
                        ──→  sat_transacciones_dsh ──┤──→  resumen_transacciones_dsh
                        ──→  links (3)             ──┤         └──→  kpi_general_dsh
                                                    │
                        ──→  fk checks (2, privadas)
```

### ¿Por qué `kpi_general_dsh` lee de Gold y no de Silver?

Porque los KPIs son un **resumen de resúmenes**. Lee de `resumen_transacciones_dsh` (que ya tiene las agregaciones por mes/código/moneda) y lo condensa a nivel mensual. Es una cadena: Bronze → Silver → Gold (detalle) → Gold (KPI).

### Publicación multi-catálogo

Nota que Silver publica en `data_vault.plata.*` y Gold publica en `ods.gold.*`. Un mismo pipeline puede escribir en **múltiples catálogos y esquemas**. Solo necesitas usar nombres fully-qualified.

---

## Paso 9 — Dry Run (validación sin ejecutar) ⏱️ 5 min

1. En el editor del pipeline, haz clic en **Validate**
2. Esto compila el código, resuelve dependencias y verifica que las tablas fuente existan
3. Si hay errores, corrígelos antes de ejecutar
4. **Un dry run exitoso NO escribe datos** — solo confirma que el código es válido

> **Tip**: Siempre haz un dry run antes de ejecutar. Es mucho más rápido y te ahorra tiempo de debug.

### Errores comunes en el dry run

| Error | Causa típica | Solución |
|---|---|---|
| `ModuleNotFoundError` | Falta el import | Agrega `from pyspark import pipelines as dp` |
| `TABLE_OR_VIEW_NOT_FOUND` | Tabla fuente no existe | Verifica que Bronze esté generado |
| `UNRESOLVED_COLUMN` | Nombre de columna incorrecto | Revisa los nombres con `DESCRIBE TABLE` |
| `SyntaxError` | Error de Python | Revisa paréntesis, indentación, comas |

## Paso 10 — Ejecutar el pipeline ⏱️ 10 min

1. Haz clic en **Start**
2. Observa el **grafo visual** — cada tabla aparece como un nodo con estado
3. El pipeline resuelve dependencias y ejecuta en paralelo cuando puede
4. Al finalizar, revisa en cada nodo:
   - **Filas escritas** por cada tabla
   - **Expectation metrics** — cuántos registros pasaron/fallaron cada regla
   - **Duración** de cada etapa

### Resultados esperados

| Tabla | Filas | Destino | Expectations |
|---|---:|---|---|
| hub_clientes_dsh | 500 | data_vault.plata | pk_no_nulo: 500 pass |
| hub_prestamos_dsh | 500 | data_vault.plata | pk_no_nulo: 500 pass |
| hub_transacciones_dsh | 1,500 | data_vault.plata | pk_no_nulo: 1,500 pass |
| sat_clientes_dsh | 500 | data_vault.plata | hk_no_nulo: 500 pass |
| sat_prestamos_dsh | 500 | data_vault.plata | hk_no_nulo: 500 pass |
| sat_transacciones_dsh | 1,500 | data_vault.plata | hk_no_nulo + monto_positivo |
| link_clientes_prestamos_dsh | 500 | data_vault.plata | — |
| link_prestamos_transacciones_dsh | 1,500 | data_vault.plata | — |
| link_clientes_transacciones_dsh | 1,500 | data_vault.plata | — |
| fk_trans_customer_check | 0 | **privada** | 0 rows → pass |
| fk_trans_deal_check | 0 | **privada** | 0 rows → pass |
| resumen_clientes_dsh | 6 | ods.gold | — |
| resumen_prestamos_dsh | 4 | ods.gold | — |
| resumen_transacciones_dsh | 70 | ods.gold | — |
| kpi_general_dsh | 5 | ods.gold | — |

> **Total**: 15 datasets (9 Silver públicos + 2 checks privados + 4 Gold)

## Paso 11 — Verificar resultados con SQL ⏱️ 5 min

Abre un notebook o SQL editor y ejecuta estas consultas:

```sql
-- Silver: verificar un hub (debe tener hash key + business key)
SELECT * FROM data_vault.plata.hub_clientes_dsh LIMIT 5;

-- Silver: conteo por país (debe dar 3 países)
SELECT country, COUNT(*) AS n
FROM data_vault.plata.sat_clientes_dsh
GROUP BY country
ORDER BY n DESC;

-- Silver: verificar un link (relación cliente-préstamo)
SELECT * FROM data_vault.plata.link_clientes_prestamos_dsh LIMIT 5;

-- Gold: resumen de préstamos (debe dar 4 filas: 2 monedas x 2 estados)
SELECT * FROM ods.gold.resumen_prestamos_dsh;

-- Gold: KPIs mensuales (debe dar 5 meses)
SELECT * FROM ods.gold.kpi_general_dsh ORDER BY transaction_month;
```

> **Checkpoint**: Si todas las consultas retornan datos coherentes, tu pipeline Batch está completo.

---

# PARTE 2: Pipeline NRT (Streaming Tables)

> En esta parte construimos un **segundo pipeline** que procesa solo **datos nuevos** usando Streaming Tables.

## ¿Qué cambia respecto al Batch?

La diferencia es mínima en código, pero enorme en comportamiento:

| Concepto | Batch (Parte 1) | NRT (Parte 2) |
|---|---|---|
| Decorador Silver | `@dp.materialized_view()` | `@dp.table()` |
| Lectura Silver | `spark.read.table()` | `spark.readStream.table()` |
| Procesamiento Silver | Recalcula **todo** cada vez | Solo datos **nuevos** |
| Tipo de tabla Silver | Materialized View | Streaming Table |
| Decorador Gold | `@dp.materialized_view()` | `@dp.materialized_view()` (igual) |
| Lectura Gold | `spark.read.table()` | `spark.read.table()` (igual, batch) |
| Primera ejecución | Lee todo | Lee todo |
| Segunda ejecución | Lee todo **de nuevo** | Solo registros **nuevos** |
| Prefijo de tablas | (sin prefijo) | `nrt_` |

> **Regla clave**: Gold **siempre** usa `@dp.materialized_view()` + `spark.read.table()` porque las agregaciones (`GROUP BY`, `SUM`, `COUNT`) necesitan ver todo el dataset completo.

## Paso 12 — Crear el pipeline NRT ⏱️ 5 min

1. **Pipelines** → **Create pipeline**
2. Nombre: `SDP_NRT_Laboratorio_dsh` (cambia `dsh` por tus iniciales)
3. Catalog: `data_vault`, Serverless: on, Photon: on
4. Crea la estructura:

```
transformations/
├── silver/
│   ├── hubs_nrt.py
│   ├── satellites_nrt.py
│   └── links_nrt.py
└── gold/
    └── resumen_nrt.py
```

> **¿Por qué un pipeline separado?** Batch y NRT tienen ciclos de vida distintos. Batch se ejecuta típicamente cada hora o al cierre del día. NRT se ejecuta con mayor frecuencia (cada minuto, o en modo continuo).

## Paso 13 — Hubs NRT (Streaming Tables) ⏱️ 10 min

**Archivo**: `transformations/silver/hubs_nrt.py`

```python
# Silver NRT — Hubs del Data Vault (streaming incremental)
# Cada nuevo registro en Bronze se procesa y deduplica por business key.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="data_vault.plata.nrt_hub_clientes_dsh",
    comment="Hub de clientes NRT — streaming incremental",
)
@dp.expect_or_fail("pk_no_nulo", "customer_bk IS NOT NULL")
def nrt_hub_clientes_dsh():
    return (
        spark.readStream.table("raw_data.bronze.cumst")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.col("customer_id").alias("customer_bk"),
            F.lit("CUMST").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["customer_bk"])
    )


@dp.table(
    name="data_vault.plata.nrt_hub_prestamos_dsh",
    comment="Hub de préstamos NRT — streaming incremental",
)
@dp.expect_or_fail("pk_no_nulo", "deal_bk IS NOT NULL")
def nrt_hub_prestamos_dsh():
    return (
        spark.readStream.table("raw_data.bronze.deal")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.col("deal_id").alias("deal_bk"),
            F.lit("DEAL").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["deal_bk"])
    )


@dp.table(
    name="data_vault.plata.nrt_hub_transacciones_dsh",
    comment="Hub de transacciones NRT — streaming incremental",
)
@dp.expect_or_fail("pk_no_nulo", "trx_bk IS NOT NULL")
def nrt_hub_transacciones_dsh():
    return (
        spark.readStream.table("raw_data.bronze.trans")
        .select(
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.col("trx_id").alias("trx_bk"),
            F.lit("TRANS").alias("record_source"),
            F.current_timestamp().alias("load_ts"),
        )
        .dropDuplicates(["trx_bk"])
    )
```

### Los 3 cambios clave de Batch a NRT

Compara con `hubs.py` (Paso 4) — solo hay **3 diferencias**:

| # | Batch | NRT | Significado |
|---|---|---|---|
| 1 | `@dp.materialized_view()` | `@dp.table()` | Crea una **Streaming Table** en vez de una MV |
| 2 | `spark.read.table()` | `spark.readStream.table()` | Lee **incrementalmente** (solo datos nuevos) |
| 3 | `hub_clientes_dsh` | `nrt_hub_clientes_dsh` | Prefijo `nrt_` para evitar conflicto con Batch |

> **Todo lo demás es idéntico**: mismas columnas, mismo hash, mismo `dropDuplicates`, mismas expectations.

### ¿Cómo funciona el streaming internamente?

- **Primera ejecución**: `readStream` lee **toda** la tabla Bronze (como un batch inicial)
- **Siguientes ejecuciones**: Solo lee los **registros nuevos** agregados desde la última vez
- Usa **checkpoints internos** de Delta Lake para rastrear su posición
- No necesitas manejar offsets ni ventanas de tiempo manualmente

## Paso 14 — Satélites NRT (Streaming Tables) ⏱️ 5 min

**Archivo**: `transformations/silver/satellites_nrt.py`

```python
# Silver NRT — Satélites del Data Vault (streaming append)
# Cada registro nuevo se agrega con su hash key y atributos.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="data_vault.plata.nrt_sat_clientes_dsh",
    comment="Satélite de clientes NRT — atributos descriptivos",
)
@dp.expect_or_fail("hk_no_nulo", "customer_hk IS NOT NULL")
def nrt_sat_clientes_dsh():
    return (
        spark.readStream.table("raw_data.bronze.cumst")
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


@dp.table(
    name="data_vault.plata.nrt_sat_prestamos_dsh",
    comment="Satélite de préstamos NRT — atributos de producto",
)
@dp.expect_or_fail("hk_no_nulo", "deal_hk IS NOT NULL")
def nrt_sat_prestamos_dsh():
    return (
        spark.readStream.table("raw_data.bronze.deal")
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


@dp.table(
    name="data_vault.plata.nrt_sat_transacciones_dsh",
    comment="Satélite de transacciones NRT — detalle transaccional",
)
@dp.expect_or_fail("hk_no_nulo", "trx_hk IS NOT NULL")
@dp.expect("monto_positivo", "traamt > 0")
def nrt_sat_transacciones_dsh():
    return (
        spark.readStream.table("raw_data.bronze.trans")
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
```

## Paso 15 — Links NRT (Streaming Tables) ⏱️ 5 min

**Archivo**: `transformations/silver/links_nrt.py`

```python
# Silver NRT — Links del Data Vault (streaming incremental)
# Relaciones deduplicadas por par de hash keys.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="data_vault.plata.nrt_link_clientes_prestamos_dsh",
    comment="Link NRT entre clientes y préstamos",
)
def nrt_link_clientes_prestamos_dsh():
    return (
        spark.readStream.table("raw_data.bronze.deal")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("DEAL").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "deal_hk"])
    )


@dp.table(
    name="data_vault.plata.nrt_link_prestamos_transacciones_dsh",
    comment="Link NRT entre préstamos y transacciones",
)
def nrt_link_prestamos_transacciones_dsh():
    return (
        spark.readStream.table("raw_data.bronze.trans")
        .select(
            F.sha2(F.col("deal_id"), 256).alias("deal_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["deal_hk", "trx_hk"])
    )


@dp.table(
    name="data_vault.plata.nrt_link_clientes_transacciones_dsh",
    comment="Link NRT entre clientes y transacciones",
)
def nrt_link_clientes_transacciones_dsh():
    return (
        spark.readStream.table("raw_data.bronze.trans")
        .select(
            F.sha2(F.col("customer_id"), 256).alias("customer_hk"),
            F.sha2(F.col("trx_id"), 256).alias("trx_hk"),
            F.current_timestamp().alias("load_ts"),
            F.lit("TRANS").alias("record_source"),
        )
        .dropDuplicates(["customer_hk", "trx_hk"])
    )
```

## Paso 16 — Gold NRT (Materialized Views) ⏱️ 10 min

**Archivo**: `transformations/gold/resumen_nrt.py`

Gold **sigue siendo Materialized View** en NRT. Las agregaciones necesitan ver el dataset completo.

```python
# Gold NRT — Tablas analíticas (materialized views sobre streaming tables)
# Lectura batch desde Silver STs para agregaciones, publicadas en ods.gold.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="ods.gold.nrt_resumen_clientes_dsh",
    comment="Resumen NRT de clientes por país y estado",
)
def nrt_resumen_clientes_dsh():
    return (
        spark.read.table("data_vault.plata.nrt_sat_clientes_dsh")
        .groupBy("country", "status")
        .agg(
            F.count("*").alias("customer_count"),
            F.countDistinct("email").alias("distinct_emails"),
        )
    )


@dp.materialized_view(
    name="ods.gold.nrt_resumen_prestamos_dsh",
    comment="Resumen NRT de préstamos por moneda y estado",
)
def nrt_resumen_prestamos_dsh():
    return (
        spark.read.table("data_vault.plata.nrt_sat_prestamos_dsh")
        .groupBy("currency", "status")
        .agg(
            F.count("*").alias("deal_count"),
            F.sum("principal_balance").alias("principal_balance_total"),
            F.avg("interest_rate").alias("avg_interest_rate"),
        )
    )


@dp.materialized_view(
    name="ods.gold.nrt_resumen_transacciones_dsh",
    comment="Resumen NRT de transacciones por mes, código, tipo y moneda",
)
def nrt_resumen_transacciones_dsh():
    return (
        spark.read.table("data_vault.plata.nrt_sat_transacciones_dsh")
        .withColumn("transaction_month", F.date_format("travdm", "yyyy-MM"))
        .groupBy("transaction_month", "tracde", "tradcc", "traccy")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("traamt").alias("total_amount"),
            F.avg("traamt").alias("avg_amount"),
        )
    )


@dp.materialized_view(
    name="ods.gold.nrt_kpi_general_dsh",
    comment="KPIs NRT mensuales generales",
)
def nrt_kpi_general_dsh():
    return (
        spark.read.table("ods.gold.nrt_resumen_transacciones_dsh")
        .groupBy("transaction_month")
        .agg(
            F.sum("transaction_count").alias("monthly_transactions"),
            F.sum("total_amount").alias("monthly_total_amount"),
        )
    )
```

### ¿Por qué Gold usa `spark.read` (batch) aunque lee de una Streaming Table?

Porque las funciones de agregación (`GROUP BY`, `SUM`, `COUNT`, `AVG`) necesitan ver **todo** el dataset para calcular totales correctos. `spark.read.table()` toma una **foto completa** de la Streaming Table en ese momento, calcula el agregado y lo escribe completo.

Si usáramos `spark.readStream` en Gold, solo vería los datos nuevos desde la última ejecución, y los totales estarían incompletos.

## Paso 17 — Ejecutar y verificar NRT ⏱️ 10 min

1. Haz un **dry run** (Validate) primero
2. Haz clic en **Start**
3. Verifica los resultados:

### Resultados esperados NRT

| Tabla | Filas | Destino | Tipo |
|---|---:|---|---|
| nrt_hub_clientes_dsh | 500 | data_vault.plata | Streaming Table |
| nrt_hub_prestamos_dsh | 500 | data_vault.plata | Streaming Table |
| nrt_hub_transacciones_dsh | 1,500 | data_vault.plata | Streaming Table |
| nrt_sat_clientes_dsh | 500 | data_vault.plata | Streaming Table |
| nrt_sat_prestamos_dsh | 500 | data_vault.plata | Streaming Table |
| nrt_sat_transacciones_dsh | 1,500 | data_vault.plata | Streaming Table |
| nrt_link_clientes_prestamos_dsh | 500 | data_vault.plata | Streaming Table |
| nrt_link_prestamos_transacciones_dsh | 1,500 | data_vault.plata | Streaming Table |
| nrt_link_clientes_transacciones_dsh | 1,500 | data_vault.plata | Streaming Table |
| nrt_resumen_clientes_dsh | 6 | ods.gold | Materialized View |
| nrt_resumen_prestamos_dsh | 4 | ods.gold | Materialized View |
| nrt_resumen_transacciones_dsh | 70 | ods.gold | Materialized View |
| nrt_kpi_general_dsh | 5 | ods.gold | Materialized View |

> **Total**: 13 datasets (9 Streaming Tables + 4 Materialized Views)
>
> Las filas de Gold son **grupos**, no registros. El número de meses (`kpi_general` = 5, `resumen_transacciones` = 70) puede variar un poco según la fecha en que se generó Bronze, porque `travdm` cubre los últimos 120 días.

## Paso 18 — Probar la incrementalidad ⏱️ 5 min

Para demostrar que NRT procesa solo datos nuevos:

1. **El instructor agrega un lote nuevo a Bronze.** Hay dos opciones:
   - Ejecutar `Batch/notebooks/01_bronze_generate_sdp` con `modo = incremental` (agrega 50 clientes, 50 préstamos y 150 transacciones con IDs nuevos), o
   - Abrir el Job **`Lab_BAC_Bronze_Lote_Incremental`** y presionar **Run now**.

   Bronze pasa a **550 / 550 / 1,650**.
2. **Cada ingeniero ejecuta su pipeline NRT** con **Start** (un update normal; **no** uses Full refresh, porque reprocesaría todo y no verías la diferencia).
3. Observa en el grafo:
   - Las **Streaming Tables** Silver escribieron solo los registros nuevos: 50 en hubs, satélites y link de clientes/préstamos, y 150 en las de transacciones.
   - Las **Materialized Views** Gold se recalcularon completas con los datos actualizados.
4. Verifica con SQL que los conteos crecieron:

```sql
SELECT 'nrt_hub_clientes' AS tabla, COUNT(*) AS filas
FROM data_vault.plata.nrt_hub_clientes_dsh            -- esperado: 550
UNION ALL
SELECT 'nrt_sat_transacciones', COUNT(*)
FROM data_vault.plata.nrt_sat_transacciones_dsh;      -- esperado: 1,650
```

5. Revisa que Gold cambió en **valores**, no en número de filas (siguen siendo 6 / 4 / 70 / 5 grupos), por ejemplo:

```sql
SELECT * FROM ods.gold.nrt_kpi_general_dsh ORDER BY transaction_month;
```

6. **Comparación:** ejecuta también el pipeline Batch. Sus Materialized Views vuelven a leer las 550 / 1,650 filas completas; las Streaming Tables del NRT solo leyeron las 250 filas nuevas.

> **¿Qué acaba de pasar?** El checkpoint de cada Streaming Table recordaba hasta dónde había leído Bronze. Al llegar el lote nuevo, solo se procesó lo que faltaba. Eso es el procesamiento incremental.

---

# PARTE 3: Resumen y comparación

## Notebooks vs SDP — ¿Cuándo usar cada uno?

| Escenario | Notebooks | SDP |
|---|---|---|
| Exploración de datos | Si | |
| Prototipos rápidos | Si | |
| Pipeline de producción | | Si |
| Calidad de datos integrada | | Si |
| Procesamiento incremental | | Si |
| Monitoreo visual integrado | | Si |
| Aprender PySpark | Si | |
| Entrega a operaciones | | Si |

## Resumen de lo aprendido

1. **Materialized View** = Batch, recalcula todo → `@dp.materialized_view()` + `spark.read`
2. **Streaming Table** = NRT, solo datos nuevos → `@dp.table()` + `spark.readStream`
3. **Expectations** = Calidad declarativa → `expect`, `expect_or_fail`, `expect_or_drop`
4. **Integridad referencial** = MVs privadas con `left_anti` join
5. **Publicación multi-catálogo** = Nombres fully-qualified (`catalogo.schema.tabla`)
6. **Dependencias** = Automáticas por el grafo del pipeline (DAG)
7. **No usar** `.write`, `.save()`, `.saveAsTable()` — el framework maneja la escritura
8. **Gold siempre es MV** — incluso sobre Streaming Tables, porque las agregaciones necesitan todo el dataset

## Pipelines de este laboratorio

| Pipeline | ID | Tipo Silver | Tipo Gold | Tablas |
|---|---|---|---|---:|
| SDP_Batch_Laboratorio_dsh | `e972e246-542b-4b24-9179-55fd1f82815e` | Materialized Views | Materialized Views | 15 |
| SDP_NRT_Laboratorio_dsh | `8699d709-da8c-4b1e-803a-dfa041210cdd` | Streaming Tables | Materialized Views | 13 |

## Inventario de tablas final

> Conteos **después del Paso 18** (Bronze inicial + 1 lote incremental). Antes del Paso 18 son 500 / 1,500.

### Silver (data_vault.plata)

| Tabla Batch | Tabla NRT | Tipo | Filas |
|---|---|---|---:|
| hub_clientes_dsh | nrt_hub_clientes_dsh | Hub | 550 |
| hub_prestamos_dsh | nrt_hub_prestamos_dsh | Hub | 550 |
| hub_transacciones_dsh | nrt_hub_transacciones_dsh | Hub | 1,650 |
| sat_clientes_dsh | nrt_sat_clientes_dsh | Satélite | 550 |
| sat_prestamos_dsh | nrt_sat_prestamos_dsh | Satélite | 550 |
| sat_transacciones_dsh | nrt_sat_transacciones_dsh | Satélite | 1,650 |
| link_clientes_prestamos_dsh | nrt_link_clientes_prestamos_dsh | Link | 550 |
| link_prestamos_transacciones_dsh | nrt_link_prestamos_transacciones_dsh | Link | 1,650 |
| link_clientes_transacciones_dsh | nrt_link_clientes_transacciones_dsh | Link | 1,650 |

### Gold (ods.gold)

| Tabla Batch | Tabla NRT | Filas |
|---|---|---:|
| resumen_clientes_dsh | nrt_resumen_clientes_dsh | 6 |
| resumen_prestamos_dsh | nrt_resumen_prestamos_dsh | 4 |
| resumen_transacciones_dsh | nrt_resumen_transacciones_dsh | 70 |
| kpi_general_dsh | nrt_kpi_general_dsh | 5 |

---

## Retos extra (si queda tiempo)

### Reto 1: Romper la integridad referencial

1. Inserta un registro huérfano en `trans`:

```sql
INSERT INTO raw_data.bronze.trans
VALUES ('TRX-HUERFANO', 'CODE1', 'DC', 'CLIENTE-FAKE', 'DEAL-FAKE',
        'CUN1', 'GLN1', 'ACC1', 'BNK1', 'BRN1', 'USD', 100.00,
        DATE'2024-06-15', 'BD1', 'USR1', 'APC1', 'MOD1', 'PMN1',
        'CKN1', 'CCN1', 'TEST', CURRENT_TIMESTAMP(), 'B999',
        CURRENT_DATE());
```

2. Ejecuta el pipeline Batch. ¿Qué pasa?
3. Observa el error en `fk_trans_customer_check` — el `left_anti` encontró un huérfano
4. Elimina el registro y vuelve a ejecutar:

```sql
DELETE FROM raw_data.bronze.trans WHERE trx_id = 'TRX-HUERFANO';
```

### Reto 2: Agregar una nueva expectation

Agrega una expectation de monitoreo a `sat_clientes_dsh` que verifique que el email contenga `@`:

```python
@dp.expect("email_valido", "email LIKE '%@%'")
```

Ejecuta el pipeline y revisa en la UI cuántos registros pasaron/fallaron.

### Reto 3: Crear una nueva tabla Gold

Crea un nuevo archivo `transformations/gold/analisis_moneda.py` con una MV que muestre el balance total por moneda y mes de apertura:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="ods.gold.analisis_moneda_dsh",
    comment="Balance por moneda y mes de apertura",
)
def analisis_moneda_dsh():
    return (
        spark.read.table("data_vault.plata.sat_prestamos_dsh")
        .withColumn("open_month", F.date_format("open_date", "yyyy-MM"))
        .groupBy("currency", "open_month")
        .agg(
            F.sum("principal_balance").alias("total_balance"),
            F.count("*").alias("deal_count"),
        )
    )
```

---

## Glosario

| Término | Significado |
|---|---|
| SDP | Spark Declarative Pipelines (antes conocido como DLT / Delta Live Tables) |
| MV | Materialized View — tabla batch que se recalcula completa en cada ejecución |
| ST | Streaming Table — tabla incremental que procesa solo datos nuevos |
| Hub | Tabla de business keys únicas con hash key (Data Vault) |
| Satélite | Tabla de atributos descriptivos vinculados a un hub (Data Vault) |
| Link | Tabla de relaciones entre dos entidades/hubs (Data Vault) |
| BK | Business Key — el identificador de negocio original (ej: customer_id) |
| HK | Hash Key — hash SHA-256 derivado de la business key |
| FK | Foreign Key — clave foránea, referencia a otra tabla |
| Expectation | Regla de calidad declarativa aplicada a cada registro |
| Dry Run | Validación del pipeline sin escribir datos |
| DAG | Directed Acyclic Graph — grafo de dependencias entre datasets |
| Unity Catalog | Gobernanza centralizada de datos en Databricks |
| Fully-qualified | Nombre completo de una tabla: `catalogo.schema.tabla` |
| Delta Lake | Formato de almacenamiento con ACID, versioning y time travel |
| left_anti join | Join que devuelve filas de la izquierda sin match en la derecha |
| Checkpoint | Punto de control interno que rastrea la posición de lectura en streaming |
| `private=True` | Dataset interno al pipeline, no publicado en Unity Catalog |
| Serverless | Compute auto-escalable sin necesidad de configurar clusters |
| Photon | Motor de ejecución nativo optimizado para workloads SQL y Delta |

---

## Próximos pasos

Una vez dominado este laboratorio, puedes explorar:

1. **Auto CDC (Change Data Capture)**: Capturar cambios (inserts, updates, deletes) automáticamente
2. **Append Flows**: Combinar múltiples fuentes en una sola Streaming Table
3. **Modo continuo**: Pipeline que corre 24/7 procesando datos en tiempo real
4. **Sinks**: Escribir a destinos externos (Kafka, Event Hubs, tablas externas)
5. **Jobs de Lakeflow**: Orquestar pipelines con dependencias y schedules
6. **Liquid Clustering**: Optimizar el almacenamiento físico para consultas frecuentes

---

*Laboratorio creado para BAC — Ingeniería de Datos. Octubre 2026.*
