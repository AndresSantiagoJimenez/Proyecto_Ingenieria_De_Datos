# Changelog

Todos los cambios notables en este proyecto serán documentados en este archivo.
El formato se basa en [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/) y este proyecto se adhiere a [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-25
### Added
- **Fase 0: Definición y Preparación**:
  - Estructura base del repositorio según especificación Lakehouse Medallion (Olist).
  - Configuración inicial de exclusión en `.gitignore`.
  - Plantilla de variables de entorno `.env.example`.
  - Especificación de dependencias de empaquetado y desarrollo en `pyproject.toml`, `requirements.txt` y `requirements-dev.txt`.
  - Configuración de análisis estático y métricas de calidad en `sonar-project.properties`.
  - Flujo de integración continua en GitHub Actions (`.github/workflows/ci.yml`) y pipeline declarativo en `Jenkinsfile`.
  - Documentación de arquitectura, plan de proyecto y políticas de seguridad.
- **Fase 1: Entorno Reproducible**:
  - Configuración de orquestación con Docker Compose en [`docker-compose.yml`](docker-compose.yml) con servicios de Airflow (Webserver, Scheduler, Postgres, Init), Spark Standalone (Master, Worker) y perfiles opcionales para SonarQube y Jenkins.
  - Creación de imagen Docker personalizada para Airflow con OpenJDK-17 y Delta Lake en [`docker/Dockerfile.airflow`](docker/Dockerfile.airflow).
  - Automatización de tareas de desarrollo y ciclo de vida mediante [`Makefile`](Makefile).
  - Factoría de sesiones Spark con extensiones de Delta Lake en [`src/data_platform/storage/session.py`](src/data_platform/storage/session.py).
  - Normalización de la estructura de datos en `data/raw/` y generación de muestra inicial en `data/sample/olist_customers_sample.csv`.
  - Pruebas unitarias para la inicialización y configuración de sesiones de Spark/Delta en [`tests/unit/test_session.py`](tests/unit/test_session.py).
- **Fase 2: Ingesta a Layer Bronze (Delta Lake)**:
  - Motor de ingesta genérico a Bronze Delta Lake en [`src/data_platform/ingestion/bronze.py`](src/data_platform/ingestion/bronze.py) con trazabilidad y linaje de auditoría (`_ingested_at`, `_batch_id`, `_source_filename`).
  - Script CLI ejecutable en [`scripts/run_bronze_ingestion.py`](scripts/run_bronze_ingestion.py) para parametrización de entidades y modos de escritura.
  - Script utilitario de inventario y descarga de datasets de Olist en [`scripts/download_olist_data.py`](scripts/download_olist_data.py).
  - Ingesta y persistencia verificada de la tabla Delta `data/bronze/customers` con 99.441 registros reales y control de versiones en `_delta_log`.
  - Tests unitarios y de integración end-to-end con PySpark y Delta Lake alcanzando 96% de cobertura de código.
- **Fase 3: Transformación a Layer Silver (Delta Lake & MERGE INTO)**:
  - Motor de transformación y limpieza en [`src/data_platform/transformations/silver.py`](src/data_platform/transformations/silver.py) con deduplicación determinista basada en ventanas (`Window.partitionBy`), casting estricto, estandarización de estados/ciudades y relleno de prefijos postales (`lpad`).
  - Implementación de operaciones ACID idempotentes `MERGE INTO` con Delta Lake (`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`) garantizando actualización in-place e inserción de nuevos registros sin duplicidad.
  - Script CLI ejecutable [`scripts/run_silver_transformation.py`](scripts/run_silver_transformation.py) para orquestación de la capa Silver.
  - Transformación y persistencia exitosa de la tabla real `data/silver/customers` (99.441 registros deduplicados y tipados).
  - Pruebas unitarias e integración end-to-end para upserts `MERGE INTO`, elevando la cobertura total al 98% (100% en el módulo Silver).
- **Fase 4: Calidad de Datos (DQA) y Aislamiento en Cuarentena (Delta Lake)**:
  - Motor de validación de reglas de calidad en [`src/data_platform/quality/dqa.py`](src/data_platform/quality/dqa.py) con soporte de comprobaciones de nulidad/vacío, longitud fija de cadenas, positividad numérica, valores categóricos permitidos y reporte consolidado [`DQAResult`](src/data_platform/quality/dqa.py).
  - Gestor de aislamiento de datos en [`src/data_platform/quality/quarantine.py`](src/data_platform/quality/quarantine.py) persistiendo registros rechazados en tablas Delta (`data/quarantine/{entity}`) con esquema auditado (`_quarantined_at`, `_quarantine_batch_id`, `_quarantine_reasons`, `_quarantine_reasons_str`, `_quarantine_entity`) y métricas de rechazo.
  - Especificación configurable de reglas en [`configs/quality_rules.yaml`](configs/quality_rules.yaml) y método de carga en [`src/data_platform/config.py`](src/data_platform/config.py).
  - Script CLI ejecutable [`scripts/run_quality_checks.py`](scripts/run_quality_checks.py) para evaluar calidad sobre tablas Delta, aislar inconsistencias y sincronizar registros limpios hacia Silver.
  - Ejecución real del motor DQA sobre 99.441 registros de `customers` validando cumplimiento total de reglas de negocio.
  - Cobertura de pruebas unitarias y de integración end-to-end con PySpark y Delta Lake con 21 tests aprobados y 98% de cobertura general (100% en el módulo de calidad).
- **Fase 5: Modelo Analítico Gold (Delta Lake & Spark SQL)**:
  - Motor de modelado dimensional y analítico en [`src/data_platform/modeling/gold.py`](src/data_platform/modeling/gold.py) para arquitectura Medallion en estrella y RFM.
  - Generación de tablas analíticas Gold en formato Delta Lake:
    - `dim_customers`: Consolidación de 96.096 perfiles únicos de clientes a partir de los 99.441 registros de Silver.
    - `fact_sales`: Consolidación analítica a nivel de ítem con ingresos, costos logísticos de flete e información de pagos.
    - `dim_customer_rfm`: Segmentación de clientes por Recencia, Frecuencia y Valor Monetario (segmentos Champions, Loyal Customers, Recent Customers, Promising, At Risk, Hibernating).
    - `dim_logistics`: Métricas de tiempos reales vs. estimados de entrega y detección de retrasos logísticos (`is_delayed`).
  - Integración nativa con consultas Spark SQL mediante vistas temporales y motor analítico interactivo.
  - Script CLI ejecutable [`scripts/run_gold_modeling.py`](scripts/run_gold_modeling.py) para construir el Lakehouse Gold y ejecutar reportes analíticos SQL.
  - Pruebas unitarias e integración end-to-end elevando el total a 27 pruebas automáticas y 99% de cobertura total del código (100% en Gold).
- **Fase 6: Orquestación con Apache Airflow**:
  - DAG completo de Airflow en [`dags/olist_delta_pipeline_dag.py`](dags/olist_delta_pipeline_dag.py) con 6 tareas secuenciales: `task_validate_sources → ingest_bronze → dqa_quarantine → transform_silver → build_gold → verify_pipeline`. Programado en cron diario (`0 2 * * *`), con reintentos automáticos y manejo de dependencias de task flow.
  - Guard de importación `AIRFLOW_AVAILABLE` para ejecución segura del DAG sin Airflow instalado localmente (modo standalone).
  - Runner de pipeline end-to-end [`scripts/run_pipeline.py`](scripts/run_pipeline.py) que ejecuta la cadena completa Bronze→DQA→Silver→Gold mediante importación directa de módulos de la plataforma de datos, con soporte de directorios configurables para aislamiento en pruebas y CI.
  - Corrección de binding de red en Windows: se añadió `spark.driver.host=127.0.0.1` y `spark.driver.bindAddress=127.0.0.1` a la factoría de sesiones [`src/data_platform/storage/session.py`](src/data_platform/storage/session.py) para resolver `NullPointerException` en heartbeat de loopback.
  - Pruebas unitarias del DAG en [`tests/unit/test_dags.py`](tests/unit/test_dags.py) (3 tests: importabilidad del DAG, existencia de tarea `task_validate_sources`, estructura mock de tareas).
  - Prueba de integración end-to-end en [`tests/integration/test_pipeline_integration.py`](tests/integration/test_pipeline_integration.py) usando `tmp_path` para aislamiento completo de directorios Lakehouse.
  - Ejecución real verificada del pipeline completo sobre 99.441 registros de Olist (exit code 0).
  - Suite total elevada a **31 pruebas automáticas** con **99% de cobertura** global (100% en módulos Bronze, Silver, DQA, Quarantine y Gold).
