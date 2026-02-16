# Quick Fix Guide - Airflow Docker Issues

## ❌ Lỗi: "image already exists"

### Nguyên nhân:
Docker cache bị conflict khi rebuild image

### Giải pháp:

#### Option 1: Clean build (Recommended)
```bash
docker-compose down
docker system prune -f
docker-compose build --no-cache
docker-compose up -d
```

#### Option 2: Sử dụng script quản lý
```powershell
.\manage-airflow.ps1
# Chọn option 9 (Full reset)
```

#### Option 3: Manual cleanup
```bash
# Stop tất cả
docker-compose down -v

# Xóa images cũ
docker images | grep pipelinefornews | awk '{print $3}' | xargs docker rmi -f

# Build lại
docker-compose build --no-cache

# Start
docker-compose up -d
```

## ❌ Lỗi: Port already in use

### Giải pháp:
```bash
# Tìm process đang dùng port 8080
netstat -ano | findstr :8080

# Kill process (thay <PID> bằng process ID)
taskkill /PID <PID> /F

# Hoặc đổi port trong docker-compose.yaml
# ports:
#   - "8081:8080"  # Thay 8080 thành 8081
```

## ❌ Lỗi: Permission denied

### Giải pháp:
```bash
# Chạy PowerShell as Administrator
# Sau đó:
docker-compose down -v
docker-compose up -d
```

## ❌ Lỗi: Dependencies not found

### Giải pháp:
```bash
# Re-export requirements
pip freeze > requirements-local.txt

# Rebuild
docker-compose build --no-cache
docker-compose up -d
```

## ❌ Lỗi: DAG not showing up

### Giải pháp:
```bash
# 1. Check DAG syntax
docker-compose exec airflow-webserver airflow dags list

# 2. Check scheduler logs
docker-compose logs -f airflow-scheduler

# 3. Restart scheduler
docker-compose restart airflow-scheduler
```

## 🔧 Useful Commands

### View logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
docker-compose logs -f postgres
```

### Check service status
```bash
docker-compose ps
```

### Execute commands in container
```bash
# Airflow CLI
docker-compose exec airflow-webserver airflow <command>

# Python shell
docker-compose exec airflow-webserver python

# Bash shell
docker-compose exec airflow-webserver bash
```

### Database access
```bash
# PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow
```

## 🚀 Quick Start (From Scratch)

```bash
# 1. Clean everything
docker-compose down -v
docker system prune -f

# 2. Build
docker-compose build --no-cache

# 3. Start
docker-compose up -d

# 4. Wait for init (30-60 seconds)
docker-compose logs -f airflow-init

# 5. Access UI
# http://localhost:8080
# Username: airflow
# Password: airflow
```

## 📝 Best Practices

1. **Always check logs first**
   ```bash
   docker-compose logs -f
   ```

2. **Clean build when changing dependencies**
   ```bash
   docker-compose build --no-cache
   ```

3. **Use volumes for data persistence**
   - Don't use `-v` flag unless you want to delete data

4. **Monitor resource usage**
   ```bash
   docker stats
   ```

5. **Regular cleanup**
   ```bash
   docker system prune -f
   ```
