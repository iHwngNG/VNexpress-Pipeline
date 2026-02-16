# Extend Apache Airflow image với custom dependencies
FROM apache/airflow:2.8.1-python3.11

# Switch to root để cài system packages
USER root

# Install system dependencies nếu cần
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch về airflow user
USER airflow

# Copy requirements file
COPY requirements-local.txt /tmp/requirements-local.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements-local.txt

# Set working directory
WORKDIR /opt/airflow
