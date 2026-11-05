# Data Engineering Platform — Olist E-Commerce Lakehouse

[![CI & Quality](https://github.com/your-username/data-engineering-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/data-engineering-platform/actions/workflows/ci.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=data-engineering-platform-olist&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=data-engineering-platform-olist)

Portafolio profesional de Ingeniería de Datos implementando una arquitectura Lakehouse Medallion (**Bronze -> Silver -> Gold**) con **Delta Lake (`delta-spark`)** sobre PySpark, orquestado con **Apache Airflow**, y asegurado mediante calidad estática y Quality Gates con **SonarQube / SonarCloud**.

---

## 🏗️ Arquitectura General

El pipeline procesa los datos públicos de **Brazilian E-Commerce (Olist)** a través de capas Delta Lake con soporte ACID, Time Travel y upserts (`MERGE INTO`), además de aislamiento de inconsistencias en una capa de **Quarantine**:

1. **Bronze Layer (Raw Delta):** Ingesta de archivos CSV/JSON preservando metadatos de auditoría (`ingested_at`, `batch_id`, `source_filename`).
2. **Silver Layer (Clean & Enriched Delta):** Deduplicación determinista, tipado estricto y validaciones con `MERGE INTO`.
3. **Quarantine Layer (Errors Delta):** Aislamiento de registros anómalos o con fallos de integridad referencial.
4. **Gold Layer (Analytics Delta):** Vistas y tablas analíticas modeladas (RFM, Rendimiento logístico, Ventas).

---

## 🔒 Políticas de Seguridad del Repositorio

Este repositorio aplica controles estrictos para garantizar integridad, trazabilidad y control de acceso:
- **Branch Protection:** La rama `main` requiere Pull Requests y validación automática del CI/Quality Gate de SonarQube para cualquier merge. Commits directos o force push están estrictamente bloqueados.
- **Acceso Exclusivo:** Permisos de escritura restringidos exclusivamente al propietario.
- **Autenticación Criptográfica & Commits Firmados:** Operaciones vía llaves SSH (Ed25519) con 2FA y commits firmados criptográficamente (GPG/SSH).
- **Prevención de Fugas de Secretos:** Configuración exclusivamente vía variables de entorno (`.env.example`), exclusión rigurosa en `.gitignore` y escaneo SAST / Secret Scanning.

---

## 🚀 Inicio Rápido (Fase 0)

### Prerrequisitos
- Python 3.10+
- Git
- Docker & Docker Compose (para Fases posteriores)

### Instalación Local
```bash
# 1. Clonar el repositorio
git clone <repo-url>
cd PROYECTO_INGENIERIA_DATOS

# 2. Configurar entorno virtual
python -m venv .venv
# En Windows:
.venv\Scripts\activate
# En Linux/macOS:
source .venv/bin/activate

# 3. Instalar dependencias base
pip install --upgrade pip
pip install -r requirements-dev.txt

# 4. Configurar variables de entorno
cp .env.example .env
```

Para más detalles sobre la hoja de ruta y especificaciones técnicas, consultar [PROJECT_PLAN.md](PROJECT_PLAN.md).
