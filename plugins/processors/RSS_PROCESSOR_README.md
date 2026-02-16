# RSS Data Processor - PyArrow Version

## 🚀 Overview

High-performance RSS data processor sử dụng **PyArrow** để xử lý và làm sạch dữ liệu RSS feeds.

### ⚡ Tại sao PyArrow?

- **Nhanh hơn pandas** cho operations lớn
- **Memory efficient** - columnar format
- **Zero-copy** reads
- **Native integration** với Parquet, Arrow IPC
- **Compute functions** tối ưu cho filtering và transformations

## 📦 Features

### 1. **Remove Duplicates**
Loại bỏ duplicates, chỉ giữ lại 1 record duy nhất

```python
# Giữ record đầu tiên
table = processor.remove_duplicates(table, subset=["rss_url"], keep="first")

# Giữ record cuối cùng
table = processor.remove_duplicates(table, subset=["rss_url"], keep="last")
```

### 2. **Handle Missing Fields**
- Field thường thiếu → set `None`
- Field URL thiếu → **xóa record đó**

```python
table = processor.handle_missing_fields(table, required_url_field="rss_url")
```

### 3. **Validate URL Format**
Kiểm tra URL phải bắt đầu với `http://` hoặc `https://`

```python
table = processor.validate_url_format(table, remove_invalid=True)
```

### 4. **Clean Text Fields**
- Strip whitespace
- Remove extra spaces
- Handle 'nan' strings

```python
table = processor.clean_text_fields(table, text_fields=["category"])
```

## 🎯 Usage

### Basic Usage

```python
from processors.rss_data_processor import RSSDataProcessor

# Sample data
data = [
    {"category": "News", "rss_url": "https://example.com/rss"},
    {"category": "News", "rss_url": "https://example.com/rss"},  # Duplicate
    {"category": None, "rss_url": "https://example.com/rss2"},
    {"category": "Tech", "rss_url": None},  # Missing URL - will be removed
]

# Process
processor = RSSDataProcessor()
table_clean = processor.process(data)

# Convert to pandas if needed
df = table_clean.to_pandas()
```

### Convenience Function

```python
from processors.rss_data_processor import process_rss_data

# Return PyArrow Table
table = process_rss_data(data)

# Return pandas DataFrame
df = process_rss_data(data, return_pandas=True)

# Custom options
df = process_rss_data(
    data,
    remove_duplicates=True,
    validate_urls=True,
    clean_text=True,
    return_pandas=True
)
```

### In Airflow DAG

```python
from processors.rss_data_processor import process_rss_data
import pyarrow as pa

@task
def clean_rss_data(metadata: dict) -> dict:
    # Load data
    table = load_output(task_id=metadata["task_id"], run_id=metadata["run_id"])
    
    # Process with PyArrow
    table_clean = process_rss_data(
        table,
        remove_duplicates=True,
        validate_urls=True,
        clean_text=True
    )
    
    logger.info(f"✅ Cleaned: {len(table_clean)} records")
    
    # Save cleaned data
    save_output(task_id="clean_rss_data", data=table_clean, run_id=metadata["run_id"])
    
    return {"task_id": "clean_rss_data", "run_id": metadata["run_id"]}
```

## 📊 Example Output

### Input Data (7 records):
```
[0] {'category': 'Trang chủ ', 'rss_url': 'https://vnexpress.net/rss/tin-moi-nhat.rss'}
[1] {'category': 'Trang chủ ', 'rss_url': 'https://vnexpress.net/rss/tin-moi-nhat.rss'}  # Duplicate
[2] {'category': 'Thế giới', 'rss_url': 'https://vnexpress.net/rss/the-gioi.rss'}
[3] {'category': None, 'rss_url': 'https://vnexpress.net/rss/thoi-su.rss'}  # Missing category
[4] {'category': 'Kinh doanh', 'rss_url': None}  # Missing URL
[5] {'category': 'Giải trí', 'rss_url': 'invalid-url'}  # Invalid URL
[6] {'category': '  Thể thao  ', 'rss_url': 'https://vnexpress.net/rss/the-thao.rss'}  # Extra spaces
```

### Processing Steps:
```
[Step 1] Handling missing fields...
   ⚠️  Found 2 records with missing/empty URL
   🗑️  Removing records with missing/empty URL...
   ✅ Removed 2 record(s) due to missing URL
   Result: 5 records

[Step 2] Validating URL format...
   ⚠️  Found 1 invalid URL(s)
   Invalid URLs: ['invalid-url']
   🗑️  Removing invalid URLs...
   ✅ Removed 1 record(s) with invalid URL
   Result: 4 records

[Step 3] Removing duplicates...
   ⚠️  Found 2 duplicate records
   Duplicate URLs: ['https://vnexpress.net/rss/tin-moi-nhat.rss', ...]
   🗑️  Removed 1 duplicate(s)
   Kept: first
   Result: 3 records

[Step 4] Cleaning text fields...
   ✨ Cleaned text field: 'category'
   Result: 3 records

✅ Processing Complete: 3 records
```

### Output Data (3 records):
```
     category                                    rss_url
0   Trang chủ  https://vnexpress.net/rss/tin-moi-nhat.rss
1  Thế giới    https://vnexpress.net/rss/the-gioi.rss
2  Thể thao    https://vnexpress.net/rss/the-thao.rss
```

## 🔧 API Reference

### `RSSDataProcessor`

#### Methods:

- **`remove_duplicates(table, subset, keep)`**
  - Remove duplicate records
  - Args: `table` (PyArrow Table), `subset` (list of columns), `keep` ('first' or 'last')
  - Returns: PyArrow Table

- **`handle_missing_fields(table, required_url_field)`**
  - Handle missing fields
  - Args: `table` (PyArrow Table), `required_url_field` (str)
  - Returns: PyArrow Table

- **`validate_url_format(table, url_field, remove_invalid)`**
  - Validate URL format
  - Args: `table` (PyArrow Table), `url_field` (str), `remove_invalid` (bool)
  - Returns: PyArrow Table

- **`clean_text_fields(table, text_fields)`**
  - Clean text fields
  - Args: `table` (PyArrow Table), `text_fields` (list of str)
  - Returns: PyArrow Table

- **`process(data, remove_duplicates, validate_urls, clean_text)`**
  - Full processing pipeline
  - Args: `data` (PyArrow Table, DataFrame, or list), options (bool)
  - Returns: PyArrow Table

### `process_rss_data()`

Convenience function for quick processing.

```python
process_rss_data(
    data,                    # Input data
    remove_duplicates=True,  # Remove duplicates
    validate_urls=True,      # Validate URL format
    clean_text=True,         # Clean text fields
    return_pandas=False      # Return type
) -> Union[pa.Table, pd.DataFrame]
```

## ⚡ Performance

PyArrow is significantly faster than pandas for:
- Large datasets (>100K rows)
- Filtering operations
- Column-wise operations
- Memory usage

### Benchmark (example):
```
Dataset: 1M RSS records
Pandas: 2.5s
PyArrow: 0.8s (3x faster)
```

## 📝 Notes

- PyArrow Table is **immutable** - operations return new tables
- Use `.to_pandas()` to convert to pandas DataFrame
- Use `pa.Table.from_pandas()` to convert from pandas
- Some operations (regex) still use pandas internally

## 🔗 Dependencies

```python
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd
```

---

**Author**: Pipeline for News Team  
**Version**: 1.0.0  
**Last Updated**: 2026-02-16
