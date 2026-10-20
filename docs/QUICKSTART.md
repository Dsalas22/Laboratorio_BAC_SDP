# Quick Start

Guía rápida para poner en marcha el laboratorio.

## ★ Ruta recomendada: SDP (Spark Declarative Pipelines)

> **Esta es la ruta principal del laboratorio.** Duración estimada: 3 horas.

1. Abre [docs/GUIA_SDP_PASO_A_PASO.md](GUIA_SDP_PASO_A_PASO.md) — es la guía paso a paso completa.
2. Ejecuta `Batch/notebooks/01_bronze_generate_sdp` para generar los datos Bronze.
3. Sigue la guía para crear y ejecutar ambos pipelines (Batch y NRT).

### Pipelines del laboratorio

| Pipeline | ID | Tipo |
|---|---|---|
| SDP_Batch_Laboratorio_dsh | `e972e246-542b-4b24-9179-55fd1f82815e` | Materialized Views |
| SDP_NRT_Laboratorio_dsh | `8699d709-da8c-4b1e-803a-dfa041210cdd` | Streaming Tables + MVs |

### Código fuente SDP

```
SDP/batch/transformations/   ← Pipeline Batch (5 archivos Python)
SDP/nrt/transformations/     ← Pipeline NRT (4 archivos Python)
```

---

## Ruta alternativa: Notebooks manuales

Si prefieres aprender PySpark puro sin framework declarativo:

### Batch

1. Abre `Batch/notebooks/01_bronze_generate_sdp` — genera datos sintéticos.
2. Continúa con `Batch/notebooks/02_silver_datavault_sdp` — Data Vault manual.
3. Finaliza con `Batch/notebooks/03_gold_sdp` — agregados Gold.

### Near Real Time

1. Abre `Near real time/notebooks/01_setup_near_real_time`.
2. Ejecuta `02_micro_batch_silver` y `03_micro_batch_gold`.
3. Programa el job `job_near_real_time_2min.json` cada 2 minutos.

---

## Objetivo del laboratorio

Al terminar deberías poder:

- Construir un pipeline SDP completo (Bronze → Silver → Gold)
- Entender la diferencia entre Materialized View y Streaming Table
- Implementar Data Vault (hubs, satélites, links) en Databricks
- Usar expectations para validar calidad de datos
- Diferenciar el enfoque manual (notebooks) vs declarativo (SDP)
