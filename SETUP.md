# Airflow Setup Guide

Quick setup guide for Apache Airflow with Docker Compose.

## Setup Steps

1. Create required directories:
```bash
mkdir -p logs config
```

2. Start Airflow:
```bash
docker-compose up -d
```

3. Access UI: http://localhost:8080
   - Username: airflow
   - Password: airflow

4. Install dependencies:
```bash
docker-compose exec airflow-webserver pip install -r /opt/airflow/requirements.txt
docker-compose exec airflow-scheduler pip install -r /opt/airflow/requirements.txt
docker-compose restart
```

## Services

- Airflow UI: http://localhost:8080
- PostgreSQL: localhost:5432
- ChromaDB: http://localhost:8000
