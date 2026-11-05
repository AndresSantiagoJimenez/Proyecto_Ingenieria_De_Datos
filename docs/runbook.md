# Runbook de Operación

## 1. Configuración del Entorno Local
1. Clonar el repositorio y acceder al directorio raíz.
2. Copiar `.env.example` a `.env`.
3. Crear el entorno virtual con Python 3.10+ e instalar dependencias:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # o en Windows: .venv\Scripts\activate
   pip install -r requirements-dev.txt
   ```

## 2. Ejecución de Pruebas y Cobertura
```bash
pytest --cov=src --cov-report=xml:coverage.xml --cov-report=term
```

## 3. Análisis Estático con SonarQube / SonarCloud
```bash
sonar-scanner -Dproject.settings=sonar-project.properties
```
