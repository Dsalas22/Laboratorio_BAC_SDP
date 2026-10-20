# Laboratorio BAC SDP

[![Databricks](https://img.shields.io/badge/Databricks-Compatible-FF3621?logo=databricks)](https://www.databricks.com/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-Compatible-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Compatible-00ADD8?logo=delta)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)

> Laboratorio práctico para construir Spark Declarative Pipelines (SDP) con datos sintéticos tipo AS400. El flujo usa las entidades CUMST, DEAL y TRANS para implementar un Data Vault completo desde Bronze hasta Gold, tanto en batch como en near real time.

## Índice

- [Descripción](#descripción)
- [Temario del Taller](#temario-del-taller)
- [Arquitectura](#arquitectura)
- [Parametrización Multi-Usuario](#parametrización-multi-usuario)
- [Inicio Rápido](#inicio-rápido)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Pipelines del Laboratorio](#pipelines-del-laboratorio)
- [Requisitos](#requisitos)
- [Documentación](#documentación)

## Descripción

Este repositorio es la base de un taller para ingenieros de datos donde cada participante:

- Genera datos sintéticos realistas sin información sensible
- Construye un Data Vault completo (hubs, satélites, links) con SDP
- Implementa validaciones de calidad con expectations
- Crea tablas Gold analíticas con KPIs
- Compara el enfoque Batch (Materialized Views) vs NRT (Streaming Tables)

## Temario del Taller

Horario: **1:00 p.m. – 4:00 p.m.** (3 horas, incluye receso). Cada bloque corresponde a los pasos de [docs/GUIA_SDP_PASO_A_PASO.md](docs/GUIA_SDP_PASO_A_PASO.md).

| Bloque | Horario | Duración | Temas | Pasos de la guía |
|---|---|---|---|---|
| **0. Introducción** | 1:00 – 1:10 p.m. | 10 min | ¿Por qué SDP vs notebooks?, arquitectura Medallion, Data Vault (hubs, satélites, links), tipos de dataset y expectations | Conceptos clave |
| **1. Bronze y setup** | 1:10 – 1:30 p.m. | 20 min | Generación de datos sintéticos (CUMST, DEAL, TRANS), creación del pipeline, parámetros (`pipelines.suffix`), estructura de archivos | Pasos 1 – 3 |
| **2. Silver Batch — Data Vault** | 1:30 – 2:15 p.m. | 45 min | Hubs con SHA-256, satélites, links, integridad referencial con `left_anti` + `expect_or_fail`, datasets `private=True` | Pasos 4 – 7 |
| *Receso* | 2:15 – 2:25 p.m. | 10 min | — | — |
| **3. Gold Batch y ejecución** | 2:25 – 3:00 p.m. | 35 min | Agregados y KPIs, publicación multi-catálogo, DAG automático, dry run, ejecución y verificación con SQL | Pasos 8 – 11 |
| **4. Pipeline NRT — Streaming** | 3:00 – 3:50 p.m. | 50 min | Streaming Tables (`@dp.table` + `readStream`), checkpoints, Gold MVs sobre STs, ejecución y prueba de incrementalidad | Pasos 12 – 18 |
| **5. Cierre y preguntas** | 3:50 – 4:00 p.m. | 10 min | Comparación Batch vs NRT, preguntas. Los retos de la Parte 3 quedan como tarea opcional | Parte 3 |

### Objetivos por bloque

- **Bloque 0:** entender qué problema resuelve SDP y el modelo Data Vault.
- **Bloque 1:** tener Bronze poblado y un pipeline propio configurado con sus iniciales.
- **Bloque 2:** construir las 9 tablas Silver y validar calidad de datos.
- **Bloque 3:** publicar 4 tablas Gold y verificar resultados en `ods.gold`.
- **Bloque 4:** replicar el flujo en modo streaming y observar el procesamiento incremental.
- **Bloque 5:** consolidar conceptos; los retos quedan como práctica opcional posterior.

## Arquitectura

### Pipeline Batch (Materialized Views)

```
raw_data.bronze (cumst/deal/trans)
    ↓
Silver — Data Vault (data_vault.plata)
    ├── Hubs:       hub_clientes, hub_prestamos, hub_transacciones
    ├── Satélites:  sat_clientes, sat_prestamos, sat_transacciones
    ├── Links:      link_clientes_prestamos, link_prestamos_transacciones, link_clientes_transacciones
    └── Calidad:    FK checks (private, expect_or_fail)
    ↓
Gold — Analítico (ods.gold)
    ├── resumen_clientes, resumen_prestamos, resumen_transacciones
    └── kpi_general
```

### Pipeline NRT (Streaming Tables + Materialized Views)

```
raw_data.bronze (streaming incremental)
    ↓
Silver — Streaming Tables (data_vault.plata.nrt_*)
    ↓
Gold — Materialized Views (ods.gold.nrt_*)
```

> **Diferencia clave:** Batch usa `@dp.materialized_view()` + `spark.read`; NRT usa `@dp.table()` + `spark.readStream` en Silver, pero Gold siempre usa `spark.read` para agregaciones.

## Parametrización Multi-Usuario

Cada ingeniero usa **sus propias iniciales** como sufijo en los nombres de tabla. Esto permite que múltiples personas trabajen en el mismo catálogo sin conflictos.

### Convención de nombres

```
Nombre del ingeniero:  Danny Salas Hernandez
Iniciales:             dsh
Ejemplo de tabla:      hub_prestamos_dsh
```

### Parámetros del pipeline

Cada participante configura estos parámetros en **Settings → Configuration** de su pipeline:

| Parámetro | Descripción | Default |
|---|---|---|
| `pipelines.suffix` | Iniciales del ingeniero (ej: `dsh`, `mlg`) | `dsh` |
| `pipelines.raw_catalog` | Catálogo Bronze | `raw_data` |
| `pipelines.raw_schema` | Schema Bronze | `bronze` |
| `pipelines.silver_catalog` | Catálogo Silver | `data_vault` |
| `pipelines.silver_schema` | Schema Silver | `plata` |
| `pipelines.gold_catalog` | Catálogo Gold | `ods` |
| `pipelines.gold_schema` | Schema Gold | `gold` |

### Cómo funciona en el código

```python
SUFFIX = spark.conf.get("pipelines.suffix", "dsh")
SILVER = spark.conf.get("pipelines.silver_catalog", "data_vault") + "." + \
         spark.conf.get("pipelines.silver_schema", "plata")

@dp.materialized_view(
    name=f"{SILVER}.hub_prestamos_{SUFFIX}",  # → data_vault.plata.hub_prestamos_dsh
)
```

## Inicio Rápido

1. Ejecuta `Batch/notebooks/01_bronze_generate_sdp` para generar los datos Bronze.
2. Sigue la guía completa en [docs/GUIA_SDP_PASO_A_PASO.md](docs/GUIA_SDP_PASO_A_PASO.md).
3. Crea tu pipeline Batch, configura tus iniciales en `pipelines.suffix`.
4. Escribe el código Silver y Gold (la guía tiene cada archivo paso a paso).
5. Ejecuta dry run + update y verifica resultados.
6. Repite para el pipeline NRT con Streaming Tables.

## Estructura del Proyecto

```
Laboratorio_BAC_SDP/
├── README.md                          ← Este archivo
├── .gitignore
├── Batch/
│   └── notebooks/
│       └── 01_bronze_generate_sdp     ← Genera datos sintéticos en Bronze
├── SDP/                               ← Código fuente de los pipelines ★
│   ├── batch/transformations/
│   │   ├── silver/
│   │   │   ├── hubs.py                ← 3 hubs del Data Vault
│   │   │   ├── satellites.py          ← 3 satélites
│   │   │   ├── links.py              ← 3 links
│   │   │   └── calidad.py            ← FK checks con expect_or_fail
│   │   └── gold/
│   │       └── resumen.py            ← 4 tablas analíticas
│   └── nrt/transformations/
│       ├── silver/
│       │   ├── hubs_nrt.py           ← 3 hubs streaming
│       │   ├── satellites_nrt.py     ← 3 satélites streaming
│       │   └── links_nrt.py          ← 3 links streaming
│       └── gold/
│           └── resumen_nrt.py        ← 4 MVs sobre Streaming Tables
└── docs/
    ├── GUIA_SDP_PASO_A_PASO.md       ★ Guía principal (3+ horas)
    ├── QUICKSTART.md                  ← Inicio rápido
    ├── STRUCTURE.md                   ← Mapa del repositorio
    └── DATABRICKS_REPO_GITHUB.md     ← Conexión Git
```

## Pipelines del Laboratorio

| Pipeline | Tipo | Silver | Gold |
|---|---|---|---|
| **SDP_Batch_Laboratorio_dsh** | Materialized Views | 9 MVs + 2 FK checks | 4 MVs |
| **SDP_NRT_Laboratorio_dsh** | Streaming + MVs | 9 Streaming Tables | 4 MVs |

### Catálogos y esquemas

| Capa | Catálogo | Schema | Ejemplo |
|---|---|---|---|
| Bronze | `raw_data` | `bronze` | `raw_data.bronze.cumst` |
| Silver | `data_vault` | `plata` | `data_vault.plata.hub_clientes_dsh` |
| Gold | `ods` | `gold` | `ods.gold.resumen_clientes_dsh` |

## Requisitos

- Databricks Workspace con Unity Catalog
- Permisos para crear tablas en `data_vault`, `ods` y leer de `raw_data`
- Serverless compute habilitado

## Documentación

- [docs/GUIA_SDP_PASO_A_PASO.md](docs/GUIA_SDP_PASO_A_PASO.md): guía paso a paso del taller SDP
- [docs/QUICKSTART.md](docs/QUICKSTART.md): inicio rápido
- [docs/STRUCTURE.md](docs/STRUCTURE.md): mapa del repositorio
- [docs/DATABRICKS_REPO_GITHUB.md](docs/DATABRICKS_REPO_GITHUB.md): conexión de Databricks con GitHub

