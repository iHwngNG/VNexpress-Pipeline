# ✅ Dependencies & Installation Guide

## 📦 Requirements Files

### **1. requirements.txt** (Development)

Flexible versions for development:
```
pandas>=2.0.0
pyarrow>=14.0.0
sentence-transformers>=2.2.0
chromadb>=0.4.22
...
```

### **2. requirements-local.txt** (Docker)

Pinned versions for reproducibility:
```
pandas==2.1.4
pyarrow==14.0.1
sentence-transformers==2.3.1
chromadb==0.4.22
...
```

## 📊 Dependency Categories

### **1. Core Data Processing**

| Package | Version | Purpose |
|---------|---------|---------|
| `pandas` | >=2.0.0 | DataFrame operations |
| `pyarrow` | >=14.0.0 | Parquet format, Arrow tables |
| `numpy` | >=1.24.0 | Numerical operations |

### **2. Web Scraping & HTTP**

| Package | Version | Purpose |
|---------|---------|---------|
| `requests` | >=2.31.0 | HTTP requests |
| `urllib3` | >=2.0.0 | URL handling |
| `beautifulsoup4` | >=4.12.0 | HTML parsing |
| `lxml` | >=4.9.0 | XML/HTML parsing (fast) |

### **3. Embeddings & Vector DB**

| Package | Version | Purpose |
|---------|---------|---------|
| `sentence-transformers` | >=2.2.0 | Text embeddings (CPU/GPU) |
| `torch` | >=2.0.0 | PyTorch backend |
| `transformers` | >=4.30.0 | Transformer models |
| `chromadb` | >=0.4.22 | Vector database |
| `openai` | >=1.0.0 | OpenAI API (optional) |

### **4. Utilities**

| Package | Version | Purpose |
|---------|---------|---------|
| `python-json-logger` | >=2.0.0 | JSON logging |
| `python-dateutil` | >=2.8.0 | Date parsing |
| `python-dotenv` | >=1.0.0 | Environment variables |

## 🚀 Installation

### **Local Development:**

```bash
# Install all dependencies
pip install -r requirements.txt

# Or install specific groups
pip install pandas pyarrow numpy  # Data processing
pip install requests beautifulsoup4 lxml  # Web scraping
pip install sentence-transformers chromadb  # Embeddings
```

### **Docker (Production):**

```bash
# Build Docker image (uses requirements-local.txt)
docker-compose build

# Or rebuild
docker-compose up --build
```

### **GPU Support (Optional):**

```bash
# Install PyTorch with CUDA support
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Then install other dependencies
pip install -r requirements.txt
```

## 🐳 Dockerfile

### **System Dependencies:**

```dockerfile
# Install system packages for:
# - build-essential: Compilation tools
# - libxml2-dev, libxslt-dev: XML/HTML parsing (lxml)
# - python3-dev: Python development headers

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
```

### **Python Dependencies:**

```dockerfile
# Copy requirements
COPY requirements-local.txt /tmp/requirements-local.txt

# Install with pip
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements-local.txt
```

## 📝 Usage by Component

### **RSS Feed Parser:**

```python
# Requires:
# - requests
# - lxml
# - beautifulsoup4

from processors.rss_feed_parser import RSSFeedParser
```

### **Article Scraper:**

```python
# Requires:
# - requests
# - beautifulsoup4
# - lxml

from scrapers.article_content_scraper import ArticleContentScraper
```

### **Text Chunker:**

```python
# Requires:
# - pandas
# - pyarrow

from processors.text_chunker import chunk_articles
```

### **Vectorizer:**

```python
# Requires:
# - sentence-transformers
# - torch
# - chromadb
# - pandas
# - pyarrow

from processors.vectorizer import create_vectorizer
```

### **Storage Manager:**

```python
# Requires:
# - pandas
# - pyarrow

from storage.short_memory_manager import save_output, load_output
```

## 🔧 Troubleshooting

### **Issue: lxml installation fails**

```bash
# Install system dependencies first
sudo apt-get install libxml2-dev libxslt-dev python3-dev

# Then install lxml
pip install lxml
```

### **Issue: torch too large**

```bash
# Use CPU-only version (smaller)
pip install torch --index-url https://download.pytorch.org/whl/cpu
```

### **Issue: ChromaDB errors**

```bash
# Upgrade ChromaDB
pip install --upgrade chromadb

# Or reinstall
pip uninstall chromadb
pip install chromadb==0.4.22
```

## 📊 Package Sizes

| Package | Size | Notes |
|---------|------|-------|
| `pandas` | ~50MB | Core data processing |
| `pyarrow` | ~30MB | Parquet support |
| `torch` (CPU) | ~200MB | CPU-only version |
| `torch` (GPU) | ~2GB | With CUDA support |
| `sentence-transformers` | ~10MB | + models (~80-500MB) |
| `chromadb` | ~20MB | Vector database |
| `lxml` | ~5MB | Fast XML parser |
| `beautifulsoup4` | ~1MB | HTML parser |

**Total (CPU)**: ~400MB + models
**Total (GPU)**: ~2.2GB + models

## ✅ Verification

### **Check Installed Packages:**

```bash
# List all installed packages
pip list

# Check specific package
pip show pandas

# Verify versions
python -c "import pandas; print(pandas.__version__)"
python -c "import torch; print(torch.__version__)"
python -c "import chromadb; print(chromadb.__version__)"
```

### **Test Imports:**

```python
# Test all critical imports
import pandas as pd
import pyarrow as pa
import requests
from bs4 import BeautifulSoup
from lxml import etree
from sentence_transformers import SentenceTransformer
import chromadb

print("✅ All imports successful!")
```

## 🔄 Update Dependencies

### **Update All:**

```bash
# Update all packages
pip install --upgrade -r requirements.txt
```

### **Update Specific:**

```bash
# Update specific package
pip install --upgrade pandas

# Update to specific version
pip install pandas==2.2.0
```

### **Freeze Current:**

```bash
# Generate current versions
pip freeze > requirements-frozen.txt
```

## 📁 Files

- ✅ **requirements.txt** - Development (flexible versions)
- ✅ **requirements-local.txt** - Docker (pinned versions)
- ✅ **Dockerfile** - Updated with system dependencies

## ✅ Summary

| Component | Dependencies |
|-----------|-------------|
| **Data Processing** | pandas, pyarrow, numpy |
| **Web Scraping** | requests, beautifulsoup4, lxml |
| **Embeddings** | sentence-transformers, torch |
| **Vector DB** | chromadb |
| **Utilities** | python-json-logger, python-dotenv |

**Total Packages**: ~15 core + dependencies
**Docker Image Size**: ~2-3GB (with all dependencies)

---

**Status**: ✅ CREATED
**Date**: 2026-02-17
**Files**: requirements.txt, requirements-local.txt, Dockerfile
**Result**: Ready to install! 🚀
