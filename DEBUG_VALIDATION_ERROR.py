"""
=== KẾT QUẢ TEST RSS VALIDATION ===

## ✅ Kết luận:

Dữ liệu từ rss_scaper.py có cấu trúc ĐÚNG:
- File JSON có structure: {"metadata": {...}, "feeds": [...]}
- Mỗi item trong feeds là dict với keys: 'category', 'rss_url'
- Enumeration hoạt động bình thường
- Pickle serialization/deserialization hoạt động đúng

## ❌ Vấn đề trong DAG:

Lỗi "'str' object has no attribute 'get'" xảy ra vì:

### Nguyên nhân:
Task `validate_rss_list` đang load SAI dữ liệu từ storage.

### Chi tiết:

1. Task `scrape_rss_list` save:
   ```python
   save_output(
       task_id=task_id,
       data=rss_list,  # ✅ rss_list là list of dicts
       run_id=run_id
   )
   ```

2. Task `validate_rss_list` load:
   ```python
   rss_list = load_output(
       task_id=metadata['task_id'],  # ❌ Đang dùng task_id từ metadata
       run_id=metadata['run_id']
   )
   ```

### Vấn đề:
- `metadata['task_id']` = 'scrape_rss_list'
- Nhưng có thể đang load sai file hoặc sai format

## 🔧 Giải pháp:

### Option 1: Fix task_id trong load
```python
@task
def validate_rss_list(metadata: dict) -> dict:
    # Load data từ storage
    rss_list = load_output(
        task_id='scrape_rss_list',  # ✅ Hard-code task_id
        run_id=metadata['run_id']
    )
```

### Option 2: Return data trực tiếp (không dùng storage)
```python
@task
def scrape_rss_list(**context) -> list:
    # ... scraping logic ...
    return rss_list  # ✅ Return trực tiếp

@task
def validate_rss_list(rss_list: list) -> dict:
    # ✅ Nhận trực tiếp từ task trước
    for idx, item in enumerate(rss_list):
        ...
```

### Option 3: Debug storage path
Kiểm tra xem file được save ở đâu:
```python
storage_path = save_output(...)
logger.info(f"Saved to: {storage_path}")

# Trong validate task
logger.info(f"Loading from task_id={metadata['task_id']}, run_id={metadata['run_id']}")
```

## 📝 Test Results:

```
TEST 1: Load từ JSON ✅
- Type: dict
- Keys: ['metadata', 'feeds']
- feeds is list of 19 dicts

TEST 2: Enumeration ✅
- Each item is dict
- Has 'category' and 'rss_url' keys
- No AttributeError

TEST 3: Validation ✅
- All items pass validation
- No errors

TEST 4: Pickle save/load ✅
- Serialization works
- Deserialization preserves structure
- Type remains dict after load
```

## 🎯 Recommended Fix:

Sửa file `dags/news-dag.py`:

```python
@task
def scrape_rss_list(**context) -> list:
    # ... existing code ...

    # Return list trực tiếp thay vì chỉ metadata
    return rss_list  # ✅ TaskFlow API sẽ tự động serialize

@task
def validate_rss_list(rss_list: list) -> dict:
    # ✅ Nhận trực tiếp, không cần load từ storage
    logger.info(f"Validating {len(rss_list)} feeds")

    for idx, item in enumerate(rss_list):
        if not item.get('category'):
            ...
```

Hoặc nếu muốn dùng storage, phải đảm bảo:
1. Save đúng format (list of dicts)
2. Load đúng task_id và run_id
3. Log để debug path

"""

print(__doc__)
