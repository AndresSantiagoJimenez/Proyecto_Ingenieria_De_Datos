# Runbook de Operaciones — Plataforma de Datos Lakehouse Medallion (Olist)

Este documento describe los procedimientos operativos para desplegar, ejecutar, monitorear y mantener la plataforma de ingeniería de datos. El sistema está diseñado con flexibilidad para ser operado bajo dos modalidades: **Modo CLI Local** (ultrarrápido, sin contenedores) y **Modo Interfaces Gráficas Web** (orquestación y auditoría visual mediante Docker).

---

## 📑 Tabla de Contenidos
1. [Requisitos Previos](#1-requisitos-previos)
2. [Modalidad 1: Operación por Terminal / CLI (Sin Docker)](#2-modalidad-1-operación-por-terminal--cli-sin-docker)
3. [Modalidad 2: Operación con Interfaces Gráficas Web (Docker)](#3-modalidad-2-operación-con-interfaces-gráficas-web-docker)
   - [3.1 Apache Airflow — Orquestación de Pipelines (Puerto 8080)](#31-apache-airflow--orquestación-de-pipelines-puerto-8080)
   - [3.2 SonarQube — Calidad Estática y Quality Gate (Puerto 9000)](#32-sonarqube--calidad-estática-y-quality-gate-puerto-9000)
   - [3.3 Jenkins — Servidor CI/CD On-Premise (Puerto 8082)](#33-jenkins--servidor-cicd-on-premise-puerto-8082)
   - [3.4 GitHub Actions — Integración Continua en la Nube](#34-github-actions--integración-continua-en-la-nube)
4. [Observabilidad, Métricas y Auditoría por Lote](#4-observabilidad-métricas-y-auditoría-por-lote)
5. [Guía de Solución de Problemas (Troubleshooting)](#5-guía-de-solución-de-problemas-troubleshooting)

---

## 1. Requisitos Previos

- **Python:** 3.10 o 3.11 instalado en el sistema.
- **Java JDK:** OpenJDK 17 instalado y configurado en `JAVA_HOME`.
- **Docker Desktop:** Instalado (solo requerido para la Modalidad 2).
- **Hadoop Winutils (En Windows):** `winutils.exe` y `hadoop.dll` en `C:\winutils\bin` (variable `HADOOP_HOME=C:\winutils`).

---

## 2. Modalidad 1: Operación por Terminal / CLI (Sin Docker)

Ideal para desarrollo local, depuración rápida y ejecución de pruebas sin consumo intensivo de memoria RAM.

### 2.1 Preparación del Entorno
```bash
# 1. Crear y activar entorno virtual
python -m venv .venv

# En Windows PowerShell:
.venv\Scripts\Activate.ps1
# En Linux/macOS:
source .venv/bin/activate

# 2. Instalar dependencias de desarrollo y producción
pip install --upgrade pip
pip install -r requirements-dev.txt
pip install -e .
```

### 2.2 Análisis Estático y Calidad de Código
```bash
# Ejecutar linter con Ruff (revisa src/, tests/, scripts/, dags/)
ruff check src/ tests/ scripts/ dags/
```

### 2.3 Ejecución de Suite de Pruebas y Cobertura
```bash
# Ejecutar todas las pruebas generando reportes XML
pytest tests/ --cov=src --cov-report=xml:coverage.xml --cov-report=term --junitxml=test-results.xml
```

### 2.4 Auditoría Local del Quality Gate
```bash
# Valida cobertura >= 80%, 0 fallos en tests y configuración de Sonar
python scripts/validate_quality_gate.py
```

### 2.5 Ejecución del Pipeline Medallion End-to-End
```bash
# Ejecuta la cadena completa: Bronze -> DQA/Cuarentena -> Silver -> Gold
python scripts/run_pipeline.py --entity customers
```

---

## 3. Modalidad 2: Operación con Interfaces Gráficas Web (Docker)

Esta modalidad permite visualizar y operar la plataforma a través de consolas web interactivas.

---

### 3.1 Apache Airflow — Orquestación de Pipelines (Puerto 8080)

Permite programar, disparar y auditar la ejecución de las 6 etapas del pipeline de datos con soporte de reintentos e idempotencia.

#### A. Inicio de Servicios de Airflow
```powershell
# Levantar base de datos e inicializador
docker compose up -d postgres airflow-init

# Levantar servidor web y scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

#### B. Acceso a la Interfaz Web
- **URL:** [http://localhost:8080](http://localhost:8080)
- **Usuario:** `airflow`
- **Contraseña:** `airflow`

#### C. Cómo Utilizar y Monitorear el DAG
1. En la lista principal de DAGs, busca: **`olist_delta_lakehouse_pipeline`**.
2. **Despausar el DAG:** Haz clic en el interruptor a la izquierda del nombre para ponerlo en estado activo (**Unpause**).
3. **Disparar Ejecución Manual:** Haz clic en el botón de reproducción **Trigger DAG (▶)** ubicado a la derecha.
4. **Inspección Visual de Tareas:**
   - Haz clic en el nombre del DAG y accede a la pestaña **Grid** o **Graph**.
   - Verás la progresión secuencial de las tareas en color verde (**Success**):
     - `task_validate_sources`: Verifica existencia e integridad de archivos fuente.
     - `task_ingest_bronze`: Ingesta CSV a Delta Lake Bronze con metadatos de linaje.
     - `task_dqa_quarantine`: Ejecuta reglas DQA y aísla registros no conformes.
     - `task_transform_silver`: Limpieza, deduplicación determinista y `MERGE INTO`.
     - `task_build_gold`: Modelado analítico dimensional (estrella, RFM y logística).
     - `task_verify_pipeline`: Verificación final de integridad de tablas Delta.
5. **Ver Logs de Tareas:** Haz clic sobre cualquier nodo/cuadro de tarea y selecciona **Log** para inspeccionar la salida en vivo de Spark y Delta Lake.

#### D. Detener Airflow
```powershell
docker compose down
```

---

### 3.2 SonarQube — Calidad Estática y Quality Gate (Puerto 9000)

Permite inspeccionar la salud técnica del código, cobertura de pruebas, deuda técnica y cumplimiento de Quality Gate.

#### A. Inicio del Servidor SonarQube
```powershell
docker compose --profile quality up -d sonarqube
```

#### B. Acceso a la Interfaz Web
- **URL:** [http://localhost:9000](http://localhost:9000)
- **Usuario inicial:** `admin`
- **Contraseña inicial:** `admin` *(te solicitará actualizarla en el primer ingreso)*.

#### C. Cómo Ver el Proyecto y sus Métricas
- Accede a la pestaña **Projects** o entra directamente a:  
  `http://localhost:9000/dashboard?id=data-engineering-platform-olist`
- En el panel principal podrás auditar:
  - **Quality Gate:** Estado consolidado (`Passed` en verde).
  - **Coverage:** Cobertura de código superior al 95%.
  - **Reliability (Confiabilidad):** Calificación `A` con 0 Bugs.
  - **Security (Seguridad):** Calificación `A` con 0 Vulnerabilidades.
  - **Maintainability (Mantenibilidad):** Calificación `A` con 0 Code Smells y 0.0% de duplicaciones.

#### D. Cómo Re-ejecutar un Análisis desde la Terminal
Para enviar un nuevo análisis tras modificar código:
```powershell
docker run --rm --network proyecto_ingenieria_datos_lakehouse-net -v "${PWD}:/usr/src" sonarsource/sonar-scanner-cli "-Dsonar.host.url=http://lakehouse-sonarqube:9000" "-Dsonar.token=<TU_TOKEN_DE_SONAR>"
```

#### E. Detener SonarQube
```powershell
docker compose --profile quality down
```

---

### 3.3 Jenkins — Servidor CI/CD On-Premise (Puerto 8082)

Permite ejecutar pipelines automatizados de construcción, pruebas y despliegue usando especificación declarativa en [`Jenkinsfile`](../Jenkinsfile).

#### A. Inicio del Servidor Jenkins
```powershell
docker compose --profile ci up -d jenkins
```

#### B. Acceso a la Interfaz Web y Desbloqueo Inicial
- **URL:** [http://localhost:8082](http://localhost:8082)
- **Contraseña de Desbloqueo (Unlock Jenkins):** Obtén la clave ejecutando:
  ```powershell
  docker exec lakehouse-jenkins cat /var/jenkins_home/secrets/initialAdminPassword
  ```
- Ingresa la clave, crea tu usuario administrador y accede al panel principal.

#### C. Cómo Crear y Poner a Correr una Tarea Pipeline
1. En el panel principal, haz clic en **Nueva Tarea** (o **+ Create a job**).
2. Asigna un nombre (ejemplo: `lakehouse-ci-pipeline`) y selecciona el tipo **Pipeline**. Haz clic en **OK**.
3. En la sección **Pipeline**:
   - En *Definition*, selecciona **Pipeline script**.
   - Pega tu script o conecta el repositorio con **Pipeline script from SCM** apuntando a tu Git.
4. Haz clic en **Guardar** (Save).
5. En el menú lateral izquierdo, haz clic en **Construir ahora** (Build Now).
6. Observa la ejecución en el panel de etapas:
   - `Checkout`
   - `Static Lint & Code Style`
   - `Unit Tests & Coverage`
   - `Validate Quality Gate Local`
   - `SonarQube Analysis`
   - `Quality Gate Approval`
7. Haz clic sobre el número de ejecución (`#1`, `#2`, etc.) y luego en **Salida de consola** (Console Output) para auditar los logs detallados.

#### D. Detener Jenkins
```powershell
docker compose --profile ci down
```

---

### 3.4 GitHub Actions — Integración Continua en la Nube

Para portafolios públicos, GitHub Actions ejecuta todo el ciclo CI de manera serverless:
- Configuración en: [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).
- Se dispara automáticamente en cada `git push` o `pull_request` a la rama `main`.
- Pasos automatizados:
  1. Configuración de Python 3.11 y OpenJDK 17.
  2. Linter estricto con Ruff.
  3. Ejecución de suite de 39+ tests con PySpark y Delta Lake.
  4. Generación y almacenamiento de artefactos descargables (`coverage.xml` y `test-results.xml`).
  5. Escaneo de SonarCloud y validación de Quality Gate.

---

## 4. Observabilidad, Métricas y Auditoría por Lote

La plataforma cuenta con un subsistema dedicado de telemetría y métricas estructuradas en `src/data_platform/observability/`:

### 4.1 Registro de Métricas por Lote (`BatchMetricsTracker`)
Cada ejecución del pipeline persiste un registro estructurado en:
📁 `data/lakehouse/metrics/pipeline_metrics.jsonl`

**Ejemplo de registro JSON generado:**
```json
{
  "batch_id": "batch_customers_20261105_221000",
  "status": "SUCCESS",
  "start_time": "2026-11-05T22:10:00.123456+00:00",
  "end_time": "2026-11-05T22:11:45.654321+00:00",
  "total_duration_seconds": 105.528,
  "total_bronze_records": 99441,
  "total_quarantined_records": 0,
  "total_silver_records": 99441,
  "total_gold_records": 96101,
  "quarantine_rejection_rate_pct": 0.0,
  "stages": {
    "bronze_ingestion": {
      "status": "SUCCESS",
      "duration_seconds": 12.34,
      "records_out": 99441
    },
    "data_quality_assurance": {
      "status": "SUCCESS",
      "duration_seconds": 18.21,
      "records_out": 99441
    },
    "silver_transformation": {
      "status": "SUCCESS",
      "duration_seconds": 22.15,
      "records_out": 99441
    },
    "gold_modeling": {
      "status": "SUCCESS",
      "duration_seconds": 52.82,
      "records_out": 96101
    }
  }
}
```

### 4.2 Logging Estructurado (`JSONFormatter`)
Los logs pueden ser emitidos en formato estándar de consola o en formato JSON estructurado listo para ser ingestado por herramientas como Datadog, Splunk o CloudWatch.

---

## 5. Guía de Solución de Problemas (Troubleshooting)

| Síntoma | Causa Probable | Solución |
|---|---|---|
| `NullPointerException` en Spark en Windows | Problema de resolución de loopback local | Se solucionó configurando `spark.driver.host=127.0.0.1` y `spark.driver.bindAddress=127.0.0.1` en `src/data_platform/storage/session.py`. |
| Puerto ocupado (ej. 8080 o 9000) | Otro proceso tiene tomado el puerto | Cambiar el puerto en `docker-compose.yml` o detener el proceso competidor con `netstat -ano`. |
| Error `Cannot resolve file path` en SonarQube | Rutas absolutas del host Windows en `coverage.xml` | Se configuró `relative_files = true` en `pyproject.toml` para resolución universal en Linux y contenedores. |
| Fallo temporal al instalar plugins en Jenkins | Latencia de red con `updates.jenkins.io` | Presionar el botón **Retry** en la pantalla de instalación o presionar **Continue** e instalarlos desde *Manage Jenkins*. |
