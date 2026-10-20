# Estructura del Repositorio

Mapa del laboratorio para ubicar rápidamente carpetas y archivos.

## Árbol de directorios

```text
Laboratorio_BAC_SDP/
├── README.md                              ← Entrada principal
├── .gitignore
├── Batch/
│   └── notebooks/
│       └── 01_bronze_generate_sdp         ← Genera datos sintéticos en Bronze
├── SDP/                                   ← Código fuente de los pipelines ★
│   ├── batch/transformations/             ← Pipeline Batch (5 archivos)
│   │   ├── silver/
│   │   │   ├── hubs.py                    ← 3 hubs (customer_hk, deal_hk, trx_hk)
│   │   │   ├── satellites.py              ← 3 satélites (atributos descriptivos)
│   │   │   ├── links.py                   ← 3 links (relaciones entre entidades)
│   │   │   └── calidad.py                 ← FK checks con expect_or_fail + left_anti
│   │   └── gold/
│   │       └── resumen.py                 ← 4 MVs analíticas + KPIs
│   └── nrt/transformations/               ← Pipeline NRT (4 archivos)
│       ├── silver/
│       │   ├── hubs_nrt.py                ← 3 hubs streaming (readStream)
│       │   ├── satellites_nrt.py          ← 3 satélites streaming
│       │   └── links_nrt.py               ← 3 links streaming
│       └── gold/
│           └── resumen_nrt.py             ← 4 MVs sobre Streaming Tables
└── docs/
    ├── GUIA_SDP_PASO_A_PASO.md            ★ Guía principal del taller (3+ horas)
    ├── QUICKSTART.md                      ← Inicio rápido
    ├── STRUCTURE.md                       ← Este archivo
    └── DATABRICKS_REPO_GITHUB.md          ← Conexión con GitHub

Total: 9 archivos Python + 1 notebook + 4 docs
```

## Lectura recomendada

1. [docs/GUIA_SDP_PASO_A_PASO.md](./GUIA_SDP_PASO_A_PASO.md) — Guía paso a paso
2. [README.md](../README.md) — Visión general y parametrización
3. [docs/QUICKSTART.md](./QUICKSTART.md) — Inicio rápido
