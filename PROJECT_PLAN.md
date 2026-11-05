# PROJECT_PLAN.md

# Data Engineering Platform — Portafolio End-to-End (Brazilian E-Commerce / Olist)

> **Propósito:** Construir un proyecto demostrable de Ingeniería de Datos para GitHub, ejecutable localmente con Docker y orientado a entrevistas de Data Engineer de nivel intermedio.
>
> **Estado:** Plan de implementación. Las tecnologías indicadas como “planeadas” se marcarán como implementadas una vez ejecutadas y consolidadas con pruebas automatizadas.
>
> **Principio rector:** Cada componente debe aportar una capacidad verificable. No se añadirán tecnologías solo para ampliar la lista del README.

## 1. Visión del Proyecto

Construir una plataforma de datos local (Lakehouse) que ingiera la fuente pública **Brazilian E-Commerce Dataset by Olist (Kaggle)** complementada con un generador sintético para pruebas extremas, procese los datos en capas **Bronze, Silver y Gold** en formato **Delta Lake**, aplique reglas de calidad de datos (DQA) con cuarentena, genere modelos analíticos, aplique análisis estático de código / seguridad (SAST) y orqueste el pipeline tanto de datos como de integración continua.

El proyecto se ejecuta localmente mediante **Docker Compose**, sin requerir cuenta cloud ni generar costos de nube.

### 1.1 Objetivos

* Demostrar un pipeline ETL/ELT end-to-end con arquitectura **Medallion (Bronze -> Silver -> Gold)**.

* Integrar **Delta Lake (`delta-spark`)** sobre **PySpark** para transacciones ACID, soporte de upserts (`MERGE INTO`) y Time Travel en entorno local.

* Aplicar reglas explícitas de **Data Quality Assurance (DQA)** y aislamiento de registros rechazados en una capa de **Quarantine**.

* Orquestar dependencias y ejecuciones de datos con **Apache Airflow**.

* Automatizar la integración continua y el análisis de calidad de código mediante **SonarQube / SonarCloud** (métricas de mantenibilidad, code smells, cobertura y seguridad SAST) e integración con **Jenkins / GitHub Actions**.

* Implementar contenedores reproducibles con **Docker Compose**.

* Proveer cobertura de pruebas unitarias, de integración y de calidad con **pytest**.

* Garantizar la seguridad e inmutabilidad del repositorio mediante políticas de acceso estrictas y firma de commits.

### 1.2 Fuera de alcance para la primera versión

* Despliegue cloud obligatorio (AWS/Azure/GCP).

* Credenciales reales o datos confidenciales de empleadores.

* Kubernetes en la ruta principal.

* IA generativa en la lógica crítica de transformación.

## 2. Perfil de Conocimientos y Tecnologías Elegidas

### 2.1 Tecnologías Elegidas para la Ruta Principal

| Área | Tecnología Elegida | Propósito / Motivo | 
| ----- | ----- | ----- | 
| **Lenguaje** | Python 3.10+ | Ingesta, orquestación, scripts de utilidad y pruebas | 
| **Fuente de Datos** | Kaggle Olist E-Commerce | Dataset relacional público rico en datos transaccionales, logísticos y de satisfacción | 
| **Procesamiento** | PySpark | Procesamiento distribuido local | 
| **Formato de Almacenamiento** | **Delta Lake (`delta-spark`)** | Formato ACID local con soporte de upserts (`MERGE INTO`), control de versiones de tablas y esquemas | 
| **Consultas** | Spark SQL / SQL | Modelo analítico Gold y validaciones | 
| **Orquestación de Datos** | Apache Airflow | Definición de DAGs y dependencias de tareas de datos | 
| **Calidad de Código / SAST** | **SonarQube / SonarCloud** | Análisis estático de código, detección de vulnerabilidades, deuda técnica y Quality Gates | 
| **CI / CD Automation** | **GitHub Actions / Jenkins** | Automatización de pruebas, verificación de Quality Gates de SonarQube y ejecución del pipeline de compilación/test | 
| **Contenedores** | Docker Compose | Entorno local reproducible (Airflow + Spark + Jenkins/SonarScanner + Python) | 
| **Pruebas** | pytest | Pruebas unitarias, de integración y reglas de calidad con reporte de cobertura para SonarQube | 
| **Calidad y Cuarentena** | Reglas Python / Spark SQL | Reglas de nulidad, tipos, duplicados y claves foráneas | 
| **Versionamiento** | Git + GitHub | Historial, portafolio y control de versiones | 

## 3. Caso de Uso y Arquitectura de Datos (Olist E-Commerce)

### 3.1 Entidades del Dataset

* `orders`: Pedidos, estados y marcas temporales logísticas.

* `order_items`: Productos asociados a cada pedido, precios y fletes.

* `customers`: Ubicación y ID único de clientes.

* `products`: Atributos y categorías traducidas al inglés.

* `payments`: Métodos de pago y montos por pedido.

* `reviews`: Puntuaciones de satisfacción y comentarios.

* `sellers`: Vendedores y su localización.

### 3.2 Capas de Almacenamiento (Delta Lake)

* **Bronze Layer (Raw Delta):** Ingesta de archivos CSV/JSON a tablas Delta conservando metadatos de auditoría (`ingested_at`, `batch_id`, `source_filename`).

* **Silver Layer (Cleansed & Enriched Delta):** Registros deduplicados, con casting de tipos explícito, fechas parseadas y enriquecidos con reglas de validación.

* **Quarantine (Errors Delta):** Registros que violen reglas críticas de calidad (ej. importes negativos, claves foráneas inexistentes, fechas incoherentes) aislados con motivo de rechazo.

* **Gold Layer (Business Analytics Delta):** Tablas modeladas en estrella/copo de nieve para responder preguntas de negocio:

  * **Fact Sales:** Métricas consolidando pedidos, ventas y fletes.

  * **Dim Customer / RFM:** Segmentación de clientes por Recencia, Frecuencia y Valor Monetario.

  * **Dim Logistics:** Tiempos de entrega esperados vs. reales por estado geográfico.

## 4. Arquitectura Lógica

```
flowchart TD
    subgraph Data Pipeline
        A[Kaggle Olist Dataset CSVs / Synthetics] --> B[Ingesta Python / PySpark]
        B --> C[(Bronze: Delta Lake Raw)]
        C --> D[Transformaciones PySpark & Rules DQA]
        D --> E{Validación de Calidad}
        E -->|Válidos| F[(Silver: Delta Lake Clean)]
        E -->|Rechazados| Q[(Quarantine: Delta Lake Errors)]
        F --> G[Agregaciones & Spark SQL]
        G --> H[(Gold: Delta Lake Analytics)]
        H --> I[Consumo / BI / Queries]
        J[Apache Airflow] -. Orquesta .-> B
        J -. Orquesta .-> D
        J -. Orquesta .-> G
    end

    subgraph CI/CD & Code Quality Pipeline
        K[Commit / PR Git] --> L[Jenkins / GitHub Actions]
        L --> M[Ejecución de Pruebas pytest & Cobertura xml]
        M --> N[SonarQube / SonarCloud Analysis]
        N --> O{Quality Gate Compliance}
        O -->|Aprobado| P[Permite Merge / Deployment]
        O -->|Falla| R[Bloquea PR / Notifica Error]
    end
```

## 🔒 Sistema de Seguridad del Repositorio

Para garantizar la integridad del código, proteger la propiedad intelectual y asegurar que **ningún agente externo, colaborador invitado o usuario público pueda modificar la rama principal (`main`) o alterar el historial**, este repositorio implementa las siguientes políticas de seguridad:

### 1. Control de Acceso Estricto (GitHub Permissions)

* **Visibilidad Pública:** El repositorio es de tipo `Public` para que la comunidad y reclutadores puedan inspeccionar la arquitectura, el código y las pruebas.

* **Permisos de Escritura Restringidos:** Los permisos de escritura (`Push`) están configurados exclusivamente para el propietario (`Andres`). Ningún otro usuario externo tiene privilegios para realizar `git push` directo.

### 2. Protección de la Rama Principal (Branch Protection Rules & Sonar Quality Gates)

La rama `main` cuenta con reglas estrictas de bloqueo configuradas en GitHub:

* **Requerir Pull Requests (PRs):** No se permiten commits directos (`git push origin main`). Todo cambio debe pasar por una rama secundaria y un PR.

* **Requerir Verificaciones de Estado (CI Checks & Quality Gate):** El flujo de CI (GitHub Actions / Jenkins) debe ejecutar los tests unitarios y validar el **Quality Gate de SonarQube/SonarCloud** (ausencia de vulnerabilidades críticas, cobertura mínima y cero code smells de alta severidad) de forma automática antes de permitir cualquier fusión (`merge`).

* **Bloqueo de Force Pushes:** Está prohibido el uso de `git push --force` o `--force-with-lease` en la rama `main` para evitar sobrescrituras maliciosas o accidentales del historial.

* **Bloqueo de Eliminación:** La rama principal no puede ser eliminada.

### 3. Autenticación Criptográfica Local (SSH & 2FA)

* **Autenticación de Dos Factores (2FA):** Obligatoria en la cuenta de GitHub asociada al repositorio.

* **Llaves SSH Ed25519:** La comunicación con GitHub se realiza exclusivamente mediante pares de claves SSH seguras. No se permiten contraseñas de cuenta para operaciones de Git.

### 4. Firma Digital de Commits (GPG / SSH Commit Signing)

* Todos los commits realizados en el repositorio están firmados criptográficamente con clave SSH/GPG verificada. Esto garantiza a cualquier auditor o reclutador que el código fue escrito y firmado por ti, previniendo la suplantación de identidad en el historial de commits.

### 5. Prevención de Fugas de Secretos (Secret Scanning & Environment Variables)

* **Uso de `.env.example`:** Las credenciales y configuraciones sensibles nunca se suben al repositorio.

* **Variables de Entorno:** Se gestionan mediante contenedores locales y archivos ignorados en `.gitignore`.

* **SonarQube Security Hotspots & GitHub Secret Scanning:** Doble capa de escaneo de código estático para prevenir la inclusión inadvertida de tokens, credenciales de conexión o claves API.

## 5. Estructura Propuesta del Repositorio

```
data-engineering-platform/
├── README.md
├── PROJECT_PLAN.md
├── CHANGELOG.md
├── .gitignore
├── .env.example
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── sonar-project.properties      # Configuración de proyectos y métricas para SonarQube/SonarCloud
├── Jenkinsfile                   # Pipeline declarativo de CI/CD para Jenkins
├── configs/
│   ├── app.yaml
│   └── quality_rules.yaml
├── data/
│   ├── sample/                   # CSVs pequeños de muestra de Olist
│   └── .gitkeep
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── runbook.md
├── dags/
│   └── olist_delta_pipeline_dag.py
├── scripts/
│   ├── download_olist_data.py    # Script para descargar / preparar datos
│   └── run_pipeline.py
├── src/
│   └── data_platform/
│       ├── __init__.py
│       ├── config.py
│       ├── ingestion/
│       ├── storage/              # Manejo de Delta Lake escrituras/lecturas
│       ├── transformations/       # Transformaciones PySpark por capa
│       ├── quality/              # Reglas DQA y gestión de cuarentena
│       └── modeling/             # Construcción de capa Gold
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data_quality/
└── .github/
    └── workflows/
        └── ci.yml                # CI Integration (pytest + SonarCloud Analysis)

```

## 6. Plan de Implementación por Fases

### Fase 0 — Definición y Preparación

* Confirmación del repositorio, estructura base, `.gitignore`, `.env.example`, `PROJECT_PLAN.md`, `sonar-project.properties` y reglas de seguridad.

### Fase 1 — Entorno Reproducible

* Configuración de `docker-compose.yml` con Airflow + PySpark + soporte local de `delta-spark` (y servicio opcional/módulo de Jenkins/SonarQube).

### Fase 2 — Ingesta a Layer Bronze (Delta)

* Descarga/carga de CSVs de Olist y persistencia en Bronze Delta con metadatos de auditoría.

### Fase 3 — Transformación a Layer Silver (Delta)

* Parseo de tipos, deduplicación determinista, escritura en tablas Delta usando `MERGE INTO`.

### Fase 4 — Calidad de Datos (DQA) y Cuarentena

* Ejecución de reglas de validación en Silver y aislamiento de inconsistencias en la tabla Delta de Cuarentena.

### Fase 5 — Modelo Analítico Gold (Delta & Spark SQL)

* Construcción de vistas y tablas Gold para métricas de E-Commerce (RFM, Ventas, Logística).

### Fase 6 — Orquestación con Airflow

* Automatización del flujo Bronze -> Silver -> DQA -> Gold mediante un DAG parametrizado.

### Fase 7 — Calidad de Código (SonarQube / SonarCloud) y CI (Jenkins / GitHub Actions)

* Configuración de informes de cobertura (`coverage.xml`) con `pytest-cov`.
* Definición de `sonar-project.properties` y reglas de Quality Gate.
* Automatización del Pipeline mediante `Jenkinsfile` / GitHub Actions para asegurar que ningún cambio sea integrado sin cumplir con las métricas de calidad de código y pruebas de software.

### Fase 8 — Observabilidad y Documentación

* Registro de métricas por lote, logs estructurados y actualización de Runbook/Diccionario de Datos.

### Fase 9 — Presentación Profesional en GitHub

* Publicación del README final con insignias (badges) de estado de CI, cobertura de SonarQube, diagramas y evidencias de ejecuciones pasadas.

## 7. Módulos Opcionales

Implementar únicamente después de completar el pipeline principal.

* **Módulo A — Extensión Cloud (S3/Azure):** Adaptador de almacenamiento externo.

* **Módulo B — Neo4j:** Exportación de entidades relacionales a un modelo de grafos.

* **Módulo C — Dashboard BI:** Conexión de tablas Gold con Looker Studio o Power BI.

## 8. Instrucciones Operativas para Antigravity u Otro Agente

1. Lee `PROJECT_PLAN.md` completo antes de realizar cambios.

2. Trabaja en una sola fase o tarea delimitada por iteración.

3. Antes de programar, resume el objetivo, archivos a modificar y criterios de aceptación.

4. No implementes todas las fases de una vez.

5. Ejecuta las pruebas disponibles después de cada cambio y comunica los resultados reales.

6. Si no puedes ejecutar un comando, proporciona la instrucción exacta para ejecución manual.

7. No agregues credenciales ni secretos al repositorio.

## 9. Seguimiento del Proyecto

| Fase | Estado | Evidencia Requerida | 
| ----- | ----- | ----- | 
| **0. Definición y preparación** | Pendiente | Dataset Olist confirmado, estructura base y seguridad documentada | 
| **1. Entorno reproducible** | Pendiente | Docker Compose con Airflow + PySpark + Delta Spark validado | 
| **2. Ingesta a Bronze (Delta)** | Pendiente | Archivos CSV cargados en formato Delta con metadatos | 
| **3. Transformación a Silver (Delta)** | Pendiente | Tablas Delta limpias, deduplicadas y probadas | 
| **4. Calidad y Cuarentena** | Pendiente | Reglas DQA en ejecución y registros rechazados aislados | 
| **5. Modelo Gold (Delta & SQL)** | Pendiente | Modelo en estrella / RFM generado en Gold | 
| **6. Airflow** | Pendiente | DAG ejecutado exitosamente con reintentos e idempotencia | 
| **7. Calidad de Código y CI** | Pendiente | SonarQube Quality Gate cumplido, `coverage.xml` reportado y `Jenkinsfile`/GitHub Actions validados | 
| **8. Observabilidad** | Pendiente | Logs estructurados, métricas por lote y runbook | 
| **9. Presentación GitHub** | Pendiente | README profesional con Badges de SonarQube, diagramas y ejecuciones | 
