"""
VNExpress ETL Pipeline - TaskFlow API với Custom Storage
Không dùng XCom, thay vào đó dùng TaskStorageManager
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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
        Task 2: Validate and Clean RSS Data using PyArrow Processor

        Args:
            metadata: Metadata từ task trước

        Returns:
            Validation results với cleaned data
        """
        from processors.rss_data_processor import process_rss_data
        import pyarrow as pa

        logger.info("=" * 80)
        logger.info("TASK 2: Validating & Cleaning RSS Data (PyArrow)")
        logger.info("=" * 80)

        # Load data từ storage
        logger.info(
            f"Loading data from task: {metadata['task_id']}, run_id: {metadata['run_id']}"
        )

        try:
            data = load_output(
                task_id=metadata["task_id"],
                run_id=metadata["run_id"],
            )

            # 🔍 DEBUG: Check type của data
            logger.info(f"🔍 Type of loaded data: {type(data).__name__}")

            # Process data với PyArrow processor
            # Processor sẽ tự động:
            # - Handle missing fields (set None cho fields thường, xóa nếu thiếu URL)
            # - Validate URL format (xóa invalid URLs)
            # - Remove duplicates (giữ first)
            # - Clean text fields

            logger.info("\n🔄 Processing with PyArrow processor...")

            try:
                # Process - return PyArrow Table
                table_clean = process_rss_data(
                    data,
                    remove_duplicates=True,
                    validate_urls=True,
                    clean_text=True,
                    return_pandas=False,  # Keep as PyArrow for performance
                )

                logger.info(f"\n✅ Processing successful!")
                logger.info(f"   Final records: {len(table_clean)}")
                logger.info(f"   Schema: {table_clean.schema}")

                # Convert to pandas for compatibility với downstream tasks
                df_clean = table_clean.to_pandas()

                # Save cleaned data back to storage
                task_id = "validate_rss_list"
                run_id = metadata["run_id"]

                storage_path = save_output(
                    task_id=task_id, data=df_clean, run_id=run_id  # Save as DataFrame
                )

                logger.info(f"💾 Saved cleaned data to: {storage_path}")

                # Return metadata
                return {
                    "task_id": task_id,
                    "run_id": run_id,
                    "validated": True,
                    "cleaned": True,
                    "feed_count": len(df_clean),
                    "storage_path": storage_path,
                }

            except Exception as e:
                # Nếu processor fail, log warning nhưng vẫn tiếp tục
                logger.warning(f"⚠️  Processor encountered error: {e}")
                logger.info("📝 Attempting fallback validation...")

                # Fallback: Basic validation without cleaning
                if isinstance(data, pd.DataFrame):
                    df = data
                elif isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, pa.Table):
                    df = data.to_pandas()
                else:
                    df = pd.DataFrame(data)

                logger.info(f"⚠️  Using original data: {len(df)} records")

                # Save original data
                task_id = "validate_rss_list"
                run_id = metadata["run_id"]

                storage_path = save_output(task_id=task_id, data=df, run_id=run_id)

                return {
                    "task_id": task_id,
                    "run_id": run_id,
                    "validated": False,
                    "cleaned": False,
                    "feed_count": len(df),
                    "storage_path": storage_path,
                    "error": str(e),
                }

        except Exception as e:
            # Critical error - log but don't fail task
            logger.error(f"❌ Critical error in validation: {e}")
            logger.info("⚠️  Returning empty result to continue pipeline")

            # Return empty result
            return {
                "task_id": "validate_rss_list",
                "run_id": metadata["run_id"],
                "validated": False,
                "cleaned": False,
                "feed_count": 0,
                "error": str(e),
            }

    @task
    def parse_rss_feeds(metadata: dict) -> dict:
        """
        Task 3: Parse RSS Feeds và Extract Article Information

        Fetch từng RSS URL, parse XML content, và extract thông tin các bài báo

        Args:
            metadata: Metadata từ validation task

        Returns:
            Metadata with parsed articles count
        """
        from processors.rss_feed_parser import RSSFeedParser
        import pyarrow as pa

        logger.info("=" * 80)
        logger.info("TASK 3: Parsing RSS Feeds & Extracting Articles")
        logger.info("=" * 80)

        # Load cleaned RSS URLs từ validate task
        logger.info(
            f"Loading RSS URLs from task: {metadata['task_id']}, run_id: {metadata['run_id']}"
        )

        data = load_output(
            task_id=metadata["task_id"],
            run_id=metadata["run_id"],
        )

        # Convert to DataFrame if needed
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, pa.Table):
            df = data.to_pandas()
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame(data)

        logger.info(f"📂 Loaded {len(df)} RSS feeds")

        # Prepare feeds list for parser
        feeds = []
        for idx, row in df.iterrows():
            feeds.append({"url": row["rss_url"], "category": row["category"]})

        logger.info(f"🔄 Starting to parse {len(feeds)} RSS feeds...")

        # Parse RSS feeds
        parser = RSSFeedParser(timeout=30, max_retries=3)

        try:
            # Parse all feeds
            articles = parser.parse_multiple_feeds(
                feeds, max_articles_per_feed=None  # Get all articles
            )

            logger.info(f"\n✅ Successfully parsed {len(articles)} articles")

            if len(articles) == 0:
                logger.warning("⚠️  No articles found in any feed")
                # Return empty result but don't fail
                return {
                    "task_id": "parse_rss_feeds",
                    "run_id": metadata["run_id"],
                    "article_count": 0,
                    "feed_count": len(feeds),
                    "success": False,
                }

            # Convert to DataFrame
            df_articles = pd.DataFrame(articles)

            logger.info(f"� Articles DataFrame: {df_articles.shape}")
            logger.info(f"   Columns: {list(df_articles.columns)}")

            # Convert to PyArrow Table for efficient storage
            table_articles = pa.Table.from_pandas(df_articles)

            # Save articles to storage
            current_task_id = "parse_rss_feeds"
            run_id = metadata["run_id"]

            storage_path = save_output(
                task_id=current_task_id,
                data=table_articles,  # Save as PyArrow Table (parquet)
                run_id=run_id,
            )

            logger.info(f"\n💾 Saved {len(df_articles)} articles to: {storage_path}")
            logger.info(f"   Format: Parquet (PyArrow Table)")
            logger.info(f"   Schema: {table_articles.schema}")

            return {
                "task_id": current_task_id,
                "run_id": run_id,
                "article_count": len(df_articles),
                "feed_count": len(feeds),
                "storage_path": storage_path,
                "success": True,
            }

        except Exception as e:
            logger.error(f"❌ Failed to parse RSS feeds: {e}")
            logger.info("⚠️  Returning error result to continue pipeline")

            return {
                "task_id": "parse_rss_feeds",
                "run_id": metadata["run_id"],
                "article_count": 0,
                "feed_count": len(feeds),
                "success": False,
                "error": str(e),
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
    parsing_result = parse_rss_feeds(validation_result)
    cleanup_result = cleanup_temp_data(parsing_result)

    # Set dependencies
    (
        storage_init
        >> rss_metadata
        >> validation_result
        >> parsing_result
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
