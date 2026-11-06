# Diccionario de Datos — Arquitectura Medallion Lakehouse (Olist)

Este documento describe el esquema, tipado, reglas de negocio y metadatos técnicos de las entidades almacenadas a lo largo de las capas del Lakehouse (**Bronze**, **Cuarentena**, **Silver** y **Gold**), basado en el dataset brasileño de E-Commerce (Olist).

---

## 📑 Índice de Capas
1. [Capa Bronze — Raw Delta Lake](#1-capa-bronze--raw-delta-lake)
2. [Capa Cuarentena — Aislamiento de Registros Inválidos](#2-capa-cuarentena--aislamiento-de-registros-inválidos)
3. [Capa Silver — Cleansed & Deduplicated Delta Lake](#3-capa-silver--cleansed--deduplicated-delta-lake)
4. [Capa Gold — Modelo Dimensional y Analítico](#4-capa-gold--modelo-dimensional-y-analítico)
   - [4.1 dim_customers (Dimensión Clientes)](#41-dim_customers-dimensión-clientes)
   - [4.2 fact_sales (Hecho Ventas a Nivel Ítem)](#42-fact_sales-hecho-ventas-a-nivel-ítem)
   - [4.3 dim_customer_rfm (Segmentación RFM)](#43-dim_customer_rfm-segmentación-rfm)
   - [4.4 dim_logistics (Métricas de Rendimiento Logístico)](#44-dim_logistics-métricas-de-rendimiento-logístico)

---

## 1. Capa Bronze — Raw Delta Lake

Almacenamiento **Append-Only** fiel a los datos crudos extraídos de las fuentes CSV, enriquecido con metadatos de auditoría y linaje.

### Tabla: `bronze_customers` (Ruta: `data/bronze/customers/`)
| Columna | Tipo PySpark | Descripción | Ejemplo / Restricción |
|---|---|---|---|
| `customer_id` | `StringType` | Llave primaria de pedido/transacción del cliente | `06b8999e2f...` (32 chars) |
| `customer_unique_id` | `StringType` | Identificador único global de la persona | `861eff4711...` (32 chars) |
| `customer_zip_code_prefix` | `StringType` | Prefijo de código postal | `01443` |
| `customer_city` | `StringType` | Ciudad de residencia declarada | `franca` |
| `customer_state` | `StringType` | Sigla del estado federativo | `SP` |
| `_ingested_at` | `TimestampType` | Marca de tiempo UTC de la ingesta en Bronze | `2026-11-05 20:00:00` |
| `_batch_id` | `StringType` | Identificador único del lote de ingesta | `batch_customers_20261105_...` |
| `_source_filename` | `StringType` | Nombre del archivo fuente de origen | `olist_customers_dataset.csv` |

---

## 2. Capa Cuarentena — Aislamiento de Registros Inválidos

Almacena registros rechazados por las reglas de calidad (DQA) antes de alcanzar la capa Silver, garantizando que los datos corruptos no contaminen el Lakehouse analítico.

### Tabla: `quarantine_customers` (Ruta: `data/quarantine/customers/`)
| Columna | Tipo PySpark | Descripción | Ejemplo / Restricción |
|---|---|---|---|
| *[Columnas Originales]* | *Varios* | Los campos originales del registro rechazado | `customer_id`, etc. |
| `_quarantined_at` | `TimestampType` | Marca de tiempo UTC del rechazo y aislamiento | `2026-11-05 20:05:00` |
| `_quarantine_batch_id` | `StringType` | Lote durante el cual se detectó la falla | `batch_customers_...` |
| `_quarantine_reasons` | `ArrayType(StringType)` | Lista de motivos de rechazo | `["null_or_empty: customer_id"]` |
| `_quarantine_reasons_str` | `StringType` | Cadena concatenada para consultas SQL rápidas | `"null_or_empty: customer_id"` |
| `_quarantine_entity` | `StringType` | Nombre de la entidad evaluada | `customers` |

---

## 3. Capa Silver — Cleansed & Deduplicated Delta Lake

Tablas deduplicadas determinísticamente mediante ventanas (`Window.partitionBy`), con campos estandarizados en mayúsculas, ceros a la izquierda en códigos postales (`lpad`), tipado estricto y operaciones **ACID `MERGE INTO`**.

### Tabla: `silver_customers` (Ruta: `data/silver/customers/`)
| Columna | Tipo PySpark | Descripción | Regla de Transformación |
|---|---|---|---|
| `customer_id` | `StringType` | **PK.** Llave primaria del cliente por orden | Trim, NOT NULL, 32 caracteres |
| `customer_unique_id` | `StringType` | Llave única persistente del cliente | Trim, NOT NULL, 32 caracteres |
| `customer_zip_code_prefix` | `StringType` | Código postal formateado | `lpad(col, 5, '0')` (5 dígitos) |
| `customer_city` | `StringType` | Nombre de ciudad estandarizado | Trim, UpperCase, caracteres limpios |
| `customer_state` | `StringType` | Código de estado federativo | Trim, UpperCase (2 caracteres, ej. `SP`, `RJ`) |
| `_silver_processed_at` | `TimestampType` | Marca de tiempo UTC del procesamiento Silver | Generado automáticamente |
| `_ingested_at` | `TimestampType` | Metadato heredado de linaje Bronze | Preservado |

---

## 4. Capa Gold — Modelo Dimensional y Analítico

Tablas dimensionales y analíticas en estrella optimizadas para consultas SQL, tableros de BI (Power BI, Looker Studio) y segmentación de clientes.

### 4.1 `dim_customers` (Dimensión Clientes)
- **Ruta:** `data/gold/dim_customers/`
- **Granularidad:** 1 registro por persona única (`customer_unique_id`).
- **Población:** 96.096 perfiles únicos a partir de los 99.441 clientes de Silver.

| Columna | Tipo PySpark | Descripción |
|---|---|---|
| `customer_unique_id` | `StringType` | **PK.** Identificador único global de la persona |
| `customer_city` | `StringType` | Ciudad principal del cliente |
| `customer_state` | `StringType` | Estado principal del cliente |
| `customer_zip_code_prefix` | `StringType` | Prefijo postal principal |
| `total_orders_associated` | `LongType` | Total de pedidos/cuentas asociadas a este cliente |
| `_gold_updated_at` | `TimestampType` | Marca de tiempo UTC de consolidación en Gold |

---

### 4.2 `fact_sales` (Hecho Ventas a Nivel Ítem)
- **Ruta:** `data/gold/fact_sales/`
- **Granularidad:** 1 fila por ítem de pedido.

| Columna | Tipo PySpark | Descripción |
|---|---|---|
| `order_id` | `StringType` | **FK.** Identificador de la orden |
| `order_item_id` | `IntegerType` | Número de secuencia del ítem dentro de la orden |
| `product_id` | `StringType` | **FK.** Identificador del producto vendido |
| `seller_id` | `StringType` | **FK.** Identificador del vendedor |
| `customer_id` | `StringType` | **FK.** Identificador del cliente de la transacción |
| `price` | `DoubleType` | Precio unitario del ítem en Reales (BRL) |
| `freight_value` | `DoubleType` | Costo de flete del ítem en Reales (BRL) |
| `total_item_value` | `DoubleType` | Monto total (`price + freight_value`) |
| `payment_type` | `StringType` | Método de pago consolidado (`credit_card`, `boleto`, etc.) |
| `order_purchase_timestamp` | `TimestampType` | Fecha y hora de compra |

---

### 4.3 `dim_customer_rfm` (Segmentación RFM)
- **Ruta:** `data/gold/dim_customer_rfm/`
- **Granularidad:** 1 fila por cliente único analizado bajo Recencia, Frecuencia y Valor Monetario.

| Columna | Tipo PySpark | Descripción | Regla de Segmentación |
|---|---|---|---|
| `customer_unique_id` | `StringType` | **PK.** Identificador del cliente | Clave única |
| `recency_days` | `LongType` | Días transcurridos desde su última compra | Menor valor = más reciente |
| `frequency` | `LongType` | Cantidad total de compras realizadas | Recuento de pedidos únicos |
| `monetary_value` | `DoubleType` | Monto total gastado en la plataforma | Suma de pagos totales |
| `rfm_segment` | `StringType` | Segmento analítico asignado | `Champions`, `Loyal Customers`, `Recent Customers`, `Promising`, `At Risk`, `Hibernating` |

---

### 4.4 `dim_logistics` (Métricas de Rendimiento Logístico)
- **Ruta:** `data/gold/dim_logistics/`
- **Granularidad:** 1 fila por orden con trazabilidad de cumplimiento de entrega.

| Columna | Tipo PySpark | Descripción |
|---|---|---|
| `order_id` | `StringType` | **PK.** Identificador del pedido |
| `customer_id` | `StringType` | **FK.** Cliente receptor |
| `order_status` | `StringType` | Estado de la orden (`delivered`, `shipped`, etc.) |
| `actual_delivery_days` | `LongType` | Días reales entre compra y entrega al cliente |
| `estimated_delivery_days`| `LongType` | Días estimados originalmente para la entrega |
| `is_delayed` | `BooleanType` | `true` si la entrega real superó la fecha estimada; `false` si llegó a tiempo |
| `customer_state` | `StringType` | Estado de destino geográfico |
