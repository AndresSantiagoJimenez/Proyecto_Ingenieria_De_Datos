# Arquitectura de Datos — Lakehouse Medallion (Olist)

## Descripción General
Este documento detalla la arquitectura técnica de la plataforma de datos basada en el patrón Medallion (Bronze, Silver, Gold) utilizando PySpark y Delta Lake.

```mermaid
flowchart TD
    subgraph Data Lakehouse (Delta Lake)
        A[Fuentes CSV Kaggle Olist] --> B[Ingesta Bronze / Raw Delta]
        B --> C[Transformaciones & DQA Silver]
        C -->|Válidos| D[Capa Silver / Cleansed Delta]
        C -->|Rechazados| E[Capa Quarantine / Errors Delta]
        D --> F[Modelado Analítico Gold Delta]
        F --> G[Tablas Fact / Dim - RFM, Logística, Ventas]
    end
```

## Componentes Principales
- **Bronze (Raw Delta):** Almacenamiento append-only con metadatos de linaje y auditoría.
- **Silver (Cleansed Delta):** Tablas deduplicadas, tipadas y estructuradas con soporte de `MERGE INTO`.
- **Quarantine (Delta):** Almacén segregado para registros que fallen reglas de calidad con diagnóstico de rechazo.
- **Gold (Business Delta):** Esquemas dimensionales optimizados para analítica de negocio.
