@echo off
echo ===================================================
echo Stopping VNExpress Pipeline...
echo ===================================================

echo [INFO] Stopping containers...
docker-compose down

echo ===================================================
echo Pipeline stopped.
echo ===================================================
pause
