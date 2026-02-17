@echo off
echo ===================================================
echo Starting VNExpress Pipeline...
echo ===================================================

REM Check for .env file
if not exist .env (
    echo [INFO] .env file not found. Copying from .env.example...
    copy .env.example .env
)

REM Create required directories
if not exist logs mkdir logs
if not exist plugins mkdir plugins
if not exist dags mkdir dags

echo [INFO] Building and starting Docker containers...
docker-compose up -d --build

echo ===================================================
echo Pipeline is running!
echo Access Airflow UI: http://localhost:8080
echo Login: airflow / airflow
echo ===================================================
pause
