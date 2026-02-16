# Hướng dẫn Build và Deploy Airflow với Dependencies Local

## 📦 Files đã tạo:

1. **Dockerfile** - Custom image extend từ Apache Airflow
2. **requirements-local.txt** - Dependencies từ local venv
3. **.dockerignore** - Loại trừ files không cần thiết
4. **docker-compose.yaml** - Đã update để build custom image

## 🚀 Cách sử dụng:

### Bước 1: Export dependencies từ local (đã làm)
```bash
pip freeze > requirements-local.txt
```

### Bước 2: Build Docker image
```bash
docker-compose build
```

### Bước 3: Khởi động services
```bash
docker-compose up -d
```

### Bước 4: Kiểm tra logs
```bash
docker-compose logs -f airflow-webserver
```

### Bước 5: Truy cập Airflow UI
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

## 🔄 Update dependencies:

Khi thêm thư viện mới vào local:

```bash
# 1. Cài thư viện mới
pip install <package-name>

# 2. Export lại requirements
pip freeze > requirements-local.txt

# 3. Rebuild Docker image
docker-compose build

# 4. Restart services
docker-compose up -d
```

## 🛠️ Troubleshooting:

### Build lỗi
```bash
# Clean build
docker-compose build --no-cache
```

### Xem logs build
```bash
docker-compose build --progress=plain
```

### Reset toàn bộ
```bash
docker-compose down -v
docker-compose build
docker-compose up -d
```

## 📝 Notes:

- **pickle** không phải là package riêng, nó là built-in module của Python
- Các dependencies được cài tự động khi build image
- Không cần chạy `pip install` thủ công trong container
