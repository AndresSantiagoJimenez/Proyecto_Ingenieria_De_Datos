.PHONY: help install lint test quality-gate up down logs clean spark-status sonar-up

SHELL := /bin/bash
PYTHON := python

help:
	@echo "Available commands:"
	@echo "  make install      - Install development dependencies in virtual environment"
	@echo "  make lint         - Run ruff static code analysis"
	@echo "  make test         - Run pytest test suite with coverage report"
	@echo "  make quality-gate - Run Quality Gate validation (coverage + test results + Sonar config)"
	@echo "  make up           - Start Airflow, Spark and Lakehouse core services with Docker Compose"
	@echo "  make down         - Stop Docker Compose services"
	@echo "  make logs         - Tail Docker Compose container logs"
	@echo "  make sonar-up     - Start SonarQube server container (quality profile)"
	@echo "  make clean        - Remove caches, temporary test artifacts, and compiled bytecode"

install:
	pip install --upgrade pip
	pip install -r requirements-dev.txt

lint:
	ruff check src/ tests/ scripts/ dags/

test:
	pytest tests/ --cov=src --cov-report=xml:coverage.xml --cov-report=term --junitxml=test-results.xml

quality-gate: test
	$(PYTHON) scripts/validate_quality_gate.py

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

sonar-up:
	docker compose --profile quality up -d sonarqube

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -f coverage.xml .coverage
