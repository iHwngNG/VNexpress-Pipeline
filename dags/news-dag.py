"""
VNExpress ETL Pipeline - TaskFlow API với Custom Storage
Không dùng XCom, thay vào đó dùng TaskStorageManager
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# Import custom modules
# Import từ plugins folder (Airflow tự động add plugins vào PYTHONPATH)
from scrapers.rss_scaper import RSSListScraper
from storage.short_memory_manager import (
    init_storage,
    StorageBackend,
    save_output,
    load_output,
    cleanup_output,
)

logger = logging.getLogger(__name__)


# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


# ============================================================================
# DAG DEFINITION
# ============================================================================


@dag(
    dag_id="vnexpress_etl_taskflow",
    default_args=default_args,
    description="VNExpress ETL Pipeline với TaskFlow API và custom storage",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["vnexpress", "etl", "taskflow", "stage-1"],
)
def vnexpress_etl_pipeline():
    """
    Main ETL Pipeline cho VNExpress
    Stage 1: Scrape RSS list
    """

    @task
    def init_storage_backend(**context):
        """
        Task 0: Initialize storage backend
        Có thể switch giữa FILE và REDIS tại đây
        """
        logger.info("🔧 Initializing storage backend...")

        # Option 1: File-based storage (default)
        init_storage(backend=StorageBackend.FILE, base_path="/tmp/airflow_task_storage")

        # Option 2: Redis storage (uncomment khi ready)
        # init_storage(
        #     backend=StorageBackend.REDIS,
        #     host='localhost',
        #     port=6379,
        #     db=0,
        #     ttl=86400  # 24 hours
        # )

        logger.info("✅ Storage backend initialized")
        return True

    @task
    def scrape_rss_list(**context) -> dict:
        """
        Task 1: Scrape RSS list từ VNExpress

        Returns:
            dict with metadata
        """
        logger.info("=" * 80)
        logger.info("TASK 1: Scraping RSS List")
        logger.info("=" * 80)

        # Get run_id từ context
        run_id = context["ds"]  # YYYY-MM-DD
        task_id = context["task"].task_id

        # Scrape RSS list
        scraper = RSSListScraper(timeout=30, max_retries=3)
        rss_list = scraper.scrape_rss_list()

        if not rss_list:
            raise Exception("Failed to scrape RSS list")

        logger.info(f"✅ Scraped {len(rss_list)} RSS feeds")

        # Save to storage thay vì XCom
        storage_path = save_output(task_id=task_id, data=rss_list, run_id=run_id)

        logger.info(f"💾 Saved to storage: {storage_path}")

        # Log sample
        for item in rss_list[:3]:
            logger.info(f"  - {item['category']}: {item['rss_url']}")

        # Return metadata (lightweight) thay vì full data
        return {
            "task_id": task_id,
            "run_id": run_id,
            "count": len(rss_list),
            "storage_path": storage_path,
        }

    @task
    def validate_rss_list(metadata: dict) -> dict:
        """
        Task 2: Validate RSS list

        Args:
            metadata: Metadata từ task trước

        Returns:
            Validation results
        """
        logger.info("=" * 80)
        logger.info("TASK 2: Validating RSS List")
        logger.info("=" * 80)

        # Load data từ storage
        rss_list = load_output(task_id=metadata["task_id"], run_id=metadata["run_id"])

        logger.info(f"📂 Loaded {len(rss_list)} feeds from storage")

        # Validation checks
        validation_errors = []

        # Check 1: Required fields
        for idx, item in enumerate(rss_list):
            if not item.get("category"):
                validation_errors.append(f"Feed {idx}: Missing category")
            if not item.get("rss_url") or not item["rss_url"].startswith("http"):
                validation_errors.append(f"Feed {idx}: Invalid URL")

        # Check 2: Duplicates
        urls = [item["rss_url"] for item in rss_list]
        if len(urls) != len(set(urls)):
            validation_errors.append("Duplicate RSS URLs found")

        if validation_errors:
            error_msg = "\n".join(validation_errors)
            raise Exception(f"Validation failed:\n{error_msg}")

        logger.info("✅ Validation passed")

        # Return metadata for next task
        return {
            "task_id": metadata["task_id"],
            "run_id": metadata["run_id"],
            "validated": True,
            "feed_count": len(rss_list),
        }

    @task
    def extract_rss_urls(metadata: dict) -> dict:
        """
        Task 3: Extract RSS URLs cho stage tiếp theo

        Args:
            metadata: Metadata từ validation task

        Returns:
            Metadata with RSS URLs task_id
        """
        logger.info("=" * 80)
        logger.info("TASK 3: Extracting RSS URLs")
        logger.info("=" * 80)

        # Load data từ storage
        rss_list = load_output(task_id=metadata["task_id"], run_id=metadata["run_id"])

        # Extract URLs
        rss_urls = [
            {"category": item["category"], "url": item["rss_url"]} for item in rss_list
        ]

        logger.info(f"📋 Extracted {len(rss_urls)} RSS URLs")

        # Save URLs list cho stage tiếp theo (parallel parsing)
        current_task_id = "extract_rss_urls"
        run_id = metadata["run_id"]

        storage_path = save_output(
            task_id=current_task_id, data=rss_urls, run_id=run_id
        )

        logger.info(f"💾 Saved RSS URLs to: {storage_path}")

        return {
            "task_id": current_task_id,
            "run_id": run_id,
            "url_count": len(rss_urls),
            "ready_for_parsing": True,
        }

    @task
    def cleanup_temp_data(metadata: dict) -> dict:
        """
        Task 4: Cleanup temporary data từ các task trước

        Args:
            metadata: Metadata từ task trước
        """
        logger.info("=" * 80)
        logger.info("TASK 4: Cleaning up temporary data")
        logger.info("=" * 80)

        run_id = metadata["run_id"]

        # Cleanup task 1 output (scrape_rss_list)
        deleted_1 = cleanup_output("scrape_rss_list", run_id)
        logger.info(f"🗑️  Cleaned scrape_rss_list: {deleted_1}")

        # Note: Không xóa extract_rss_urls vì stage tiếp theo cần

        logger.info("✅ Cleanup completed")

        return {
            "cleaned": True,
            "ready_for_next_stage": True,
        }

    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================

    # Chain tasks
    storage_init = init_storage_backend()
    rss_metadata = scrape_rss_list()
    validation_result = validate_rss_list(rss_metadata)
    extraction_result = extract_rss_urls(validation_result)
    cleanup_result = cleanup_temp_data(extraction_result)

    # Set dependencies
    (
        storage_init
        >> rss_metadata
        >> validation_result
        >> extraction_result
        >> cleanup_result
    )


# Instantiate DAG
dag_instance = vnexpress_etl_pipeline()


# ============================================================================
# DOCUMENTATION
# ============================================================================

dag_instance.doc_md = """
# VNExpress ETL Pipeline - Stage 1

## Architecture
Sử dụng TaskFlow API với custom storage layer (không dùng XCom).

## Storage Strategy
- **Backend**: File-based (Parquet) - có thể switch sang Redis
- **Location**: `/tmp/airflow_task_storage/{run_id}/{task_id}/`
- **Format**: Auto-detect (Parquet cho list/DataFrame, JSON cho dict, Pickle cho complex objects)
- **Cleanup**: Automatic cleanup sau khi task sử dụng xong

## Task Flow
```
init_storage_backend
    ↓
scrape_rss_list (save to storage)
    ↓
validate_rss_list (load from storage)
    ↓
extract_rss_urls (save URLs for next stage)
    ↓
cleanup_temp_data (cleanup used data)
```

## Data Flow
1. Task trả về **metadata** (lightweight) thay vì full data
2. Data được lưu vào **TaskStorageManager**
3. Task tiếp theo **load** data từ storage
4. Sau khi xử lý xong → **cleanup** để free space

## Switching to Redis
Chỉ cần thay đổi trong `init_storage_backend()`:
```python
init_storage(
    backend=StorageBackend.REDIS,
    host='localhost',
    port=6379,
    ttl=86400
)
```

## Benefits
✅ Không bị giới hạn XCom size  
✅ Scalable (dễ migrate Redis)  
✅ Automatic cleanup  
✅ Support nhiều data formats  
✅ Easy debugging (có thể inspect files)  

## Next Stage
Output: `extract_rss_urls` task tạo file chứa RSS URLs  
→ Stage 2 DAG sẽ load file này và parse parallel  
"""
