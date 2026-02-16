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
        from scrapers.rss_scaper import scrape_vnexpress_rss_list

        logger.info("=" * 80)
        logger.info("TASK 1: Scraping RSS List")
        logger.info("=" * 80)

        # Get run_id từ context
        run_id = context["ds"]  # YYYY-MM-DD
        task_id = context["task"].task_id

        # Scrape RSS list using utility function
        rss_list = scrape_vnexpress_rss_list(timeout=30, max_retries=3)

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
        from processors.rss_feed_parser import parse_rss_feeds_parallel

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

        try:
            # Parse all feeds in parallel using utility function
            # TESTING: Limit to 3 articles per feed for faster testing
            articles = parse_rss_feeds_parallel(
                feeds=feeds,
                timeout=30,
                max_retries=3,
                max_articles_per_feed=3,  # Limit to 3 articles per feed for testing
                max_workers=3,  # Parallel workers
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

            # Deduplicate articles (optimized - exact match only)
            from processors.article_deduplicator import deduplicate_articles

            logger.info(f"\n🔄 Deduplicating articles...")
            articles_clean = deduplicate_articles(
                articles,
                by_url=True,  # Remove exact duplicate URLs
                by_guid=True,  # Remove exact duplicate GUIDs
                by_title_and_content=True,  # Remove exact duplicate title+content
                keep="first",
            )

            logger.info(
                f"✅ Deduplication complete: {len(articles_clean)} unique articles"
            )

            # Convert to DataFrame
            df_articles = pd.DataFrame(articles_clean)

            logger.info(f"📊 Articles DataFrame: {df_articles.shape}")
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
    def scrape_article_contents(metadata: dict) -> dict:
        """
        Task 4: Scrape Full Article Content & Metadata (Parallel)

        Fetch full content và metadata từ URL của từng bài báo
        Sử dụng parallel processing để tăng tốc độ

        Args:
            metadata: Metadata từ parse_rss_feeds task

        Returns:
            Metadata with scraped articles count
        """
        from scrapers.article_content_scraper import (
            scrape_multiple_article_contents_parallel,
        )

        logger.info("=" * 80)
        logger.info("TASK 4: Scraping Article Contents (Parallel)")
        logger.info("=" * 80)

        # Load articles from previous task
        logger.info(
            f"Loading articles from task: {metadata['task_id']}, run_id: {metadata['run_id']}"
        )

        data = load_output(
            task_id=metadata["task_id"],
            run_id=metadata["run_id"],
        )

        # Convert to DataFrame
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, pa.Table):
            df = data.to_pandas()
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame(data)

        logger.info(f"📂 Loaded {len(df)} articles to scrape")

        if len(df) == 0:
            logger.warning("⚠️  No articles to scrape")
            return {
                "task_id": "scrape_article_contents",
                "run_id": metadata["run_id"],
                "scraped_count": 0,
                "success": False,
            }

        # Convert to list of dicts
        articles_to_scrape = df.to_dict("records")

        try:
            # Scrape articles in parallel using utility function
            scraped_articles = scrape_multiple_article_contents_parallel(
                articles=articles_to_scrape,
                url_field="link",
                category_field="category",
                timeout=30,
                max_retries=3,
                max_workers=5,
            )

            if len(scraped_articles) == 0:
                logger.error("❌ No articles were scraped successfully")
                return {
                    "task_id": "scrape_article_contents",
                    "run_id": metadata["run_id"],
                    "scraped_count": 0,
                    "failed_count": len(articles_to_scrape),
                    "success": False,
                }

            # Convert to DataFrame
            df_scraped = pd.DataFrame(scraped_articles)

            logger.info(f"\n📊 Scraped DataFrame: {df_scraped.shape}")
            logger.info(f"   Columns: {list(df_scraped.columns)}")

            # Convert to PyArrow Table
            table_scraped = pa.Table.from_pandas(df_scraped)

            # Save to storage
            current_task_id = "scrape_article_contents"
            run_id = metadata["run_id"]

            storage_path = save_output(
                task_id=current_task_id,
                data=table_scraped,
                run_id=run_id,
            )

            logger.info(
                f"\n💾 Saved {len(df_scraped)} scraped articles to: {storage_path}"
            )
            logger.info(f"   Format: Parquet (PyArrow Table)")

            failed_count = len(articles_to_scrape) - len(scraped_articles)

            return {
                "task_id": current_task_id,
                "run_id": run_id,
                "scraped_count": len(df_scraped),
                "failed_count": failed_count,
                "storage_path": storage_path,
                "success": True,
            }

        except Exception as e:
            logger.error(f"❌ Failed to scrape articles: {e}")
            return {
                "task_id": "scrape_article_contents",
                "run_id": metadata["run_id"],
                "scraped_count": 0,
                "success": False,
                "error": str(e),
            }

    @task
    def chunk_articles(metadata: dict) -> dict:
        """
        Task 5: Chunk Articles into Semantic Chunks

        Split article content into meaningful chunks based on semantic boundaries

        Args:
            metadata: Metadata từ scrape_article_contents task

        Returns:
            Metadata with chunked articles count
        """
        from processors.text_chunker import chunk_articles_parallel as chunk_articles_fn

        logger.info("=" * 80)
        logger.info("TASK 5: Chunking Articles (Semantic)")
        logger.info("=" * 80)

        # Load scraped articles from previous task
        logger.info(
            f"Loading articles from task: {metadata['task_id']}, run_id: {metadata['run_id']}"
        )

        data = load_output(
            task_id=metadata["task_id"],
            run_id=metadata["run_id"],
        )

        # Convert to DataFrame
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, pa.Table):
            df = data.to_pandas()
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame(data)

        logger.info(f"📂 Loaded {len(df)} articles to chunk")

        if len(df) == 0:
            logger.warning("⚠️  No articles to chunk")
            return {
                "task_id": "chunk_articles",
                "run_id": metadata["run_id"],
                "chunk_count": 0,
                "success": False,
            }

        # Convert to list of dicts
        articles_to_chunk = df.to_dict("records")

        try:
            # Chunk articles using parallel utility function
            chunks = chunk_articles_fn(
                articles=articles_to_chunk,
                content_field="content",
                metadata_fields=[
                    "title",
                    "category",
                    "published_date",
                    "link",
                    "description",
                ],
                min_chunk_size=500,
                max_chunk_size=1500,
                target_chunk_size=1000,
                overlap_sentences=2,
                max_workers=5,  # Parallel workers
            )

            if len(chunks) == 0:
                logger.error("❌ No chunks were created")
                return {
                    "task_id": "chunk_articles",
                    "run_id": metadata["run_id"],
                    "chunk_count": 0,
                    "article_count": len(articles_to_chunk),
                    "success": False,
                }

            # Convert to DataFrame
            df_chunks = pd.DataFrame(chunks)

            logger.info(f"\n📊 Chunks DataFrame: {df_chunks.shape}")
            logger.info(f"   Columns: {list(df_chunks.columns)}")
            logger.info(f"   Total chunks: {len(df_chunks)}")
            logger.info(
                f"   Avg chunks per article: {len(df_chunks) / len(articles_to_chunk):.2f}"
            )

            # Convert to PyArrow Table
            table_chunks = pa.Table.from_pandas(df_chunks)

            # Save to storage
            current_task_id = "chunk_articles"
            run_id = metadata["run_id"]

            storage_path = save_output(
                task_id=current_task_id,
                data=table_chunks,
                run_id=run_id,
            )

            logger.info(f"\n💾 Saved {len(df_chunks)} chunks to: {storage_path}")
            logger.info(f"   Format: Parquet (PyArrow Table)")

            return {
                "task_id": current_task_id,
                "run_id": run_id,
                "chunk_count": len(df_chunks),
                "article_count": len(articles_to_chunk),
                "storage_path": storage_path,
                "success": True,
            }

        except Exception as e:
            logger.error(f"❌ Failed to chunk articles: {e}")
            return {
                "task_id": "chunk_articles",
                "run_id": metadata["run_id"],
                "chunk_count": 0,
                "success": False,
                "error": str(e),
            }

    @task
    def vectorize_chunks(metadata: dict) -> dict:
        """
        Task 6: Vectorize Chunks and Insert into ChromaDB (Parallel)

        Vectorize text chunks using embeddings and store in ChromaDB
        Uses batch processing for efficiency

        Args:
            metadata: Metadata từ chunk_articles task

        Returns:
            Metadata with vectorization stats
        """
        from processors.vectorizer import create_vectorizer
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info("=" * 80)
        logger.info("TASK 6: Vectorizing Chunks & Inserting to ChromaDB")
        logger.info("=" * 80)

        # Load chunks from previous task
        logger.info(
            f"Loading chunks from task: {metadata['task_id']}, run_id: {metadata['run_id']}"
        )

        data = load_output(
            task_id=metadata["task_id"],
            run_id=metadata["run_id"],
        )

        # Convert to DataFrame
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, pa.Table):
            df = data.to_pandas()
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame(data)

        logger.info(f"📂 Loaded {len(df)} chunks to vectorize")

        if len(df) == 0:
            logger.warning("⚠️  No chunks to vectorize")
            return {
                "task_id": "vectorize_chunks",
                "run_id": metadata["run_id"],
                "vectorized_count": 0,
                "success": False,
            }

        try:
            # Create vectorizer
            logger.info("🔄 Creating vectorizer...")
            vectorizer = create_vectorizer(
                model_type="sentence-transformers",
                model_name="all-MiniLM-L6-v2",  # CPU-friendly model
                device="cpu",
                chroma_path="./chroma_db",
                collection_name="news_articles",
            )

            # Convert to list of dicts
            chunks_list = df.to_dict("records")

            # Batch processing for parallel vectorization
            batch_size = 50  # Process 50 chunks at a time
            total_batches = (len(chunks_list) + batch_size - 1) // batch_size

            logger.info(
                f"🔄 Processing {len(chunks_list)} chunks in {total_batches} batches (batch_size={batch_size})..."
            )

            total_vectorized = 0
            total_inserted = 0
            failed_batches = 0

            def process_batch(batch_data):
                """Helper to process single batch"""
                batch_idx, batch = batch_data
                try:
                    result = vectorizer.vectorize_chunks(
                        chunks=batch,
                        text_field="chunk_text",
                        metadata_fields=[
                            "title",
                            "category",
                            "published_date",
                            "link",
                            "chunk_id",
                            "total_chunks",
                        ],
                    )
                    return (batch_idx, result)
                except Exception as e:
                    logger.error(f"Error processing batch {batch_idx}: {e}")
                    return (batch_idx, {"success": False, "error": str(e)})

            # Parallel batch processing
            with ThreadPoolExecutor(max_workers=3) as executor:
                # Create batches
                batches = []
                for i in range(0, len(chunks_list), batch_size):
                    batch = chunks_list[i : i + batch_size]
                    batches.append((i // batch_size, batch))

                # Submit all batch tasks
                future_to_batch = {
                    executor.submit(process_batch, batch_data): batch_data[0]
                    for batch_data in batches
                }

                # Process completed batches
                for future in as_completed(future_to_batch):
                    batch_idx = future_to_batch[future]
                    try:
                        idx, result = future.result()

                        if result.get("success"):
                            vectorized = result.get("vectorized", 0)
                            inserted = result.get("inserted", 0)
                            total_vectorized += vectorized
                            total_inserted += inserted
                            logger.info(
                                f"[Batch {idx+1}/{total_batches}] ✅ Vectorized: {vectorized}, Inserted: {inserted}"
                            )
                        else:
                            failed_batches += 1
                            logger.warning(f"[Batch {idx+1}/{total_batches}] ⚠️  Failed")

                    except Exception as e:
                        failed_batches += 1
                        logger.error(
                            f"[Batch {batch_idx+1}/{total_batches}] ❌ Error: {e}"
                        )

            logger.info(f"\n✅ Vectorization complete:")
            logger.info(f"   Total chunks: {len(chunks_list)}")
            logger.info(f"   Vectorized: {total_vectorized}")
            logger.info(f"   Inserted: {total_inserted}")
            logger.info(f"   Failed batches: {failed_batches}")

            # Get collection stats
            stats = vectorizer.get_collection_stats()
            logger.info(f"\n📊 ChromaDB Collection Stats:")
            logger.info(f"   Collection: {stats['collection_name']}")
            logger.info(f"   Total chunks in DB: {stats['total_chunks']}")
            logger.info(f"   Model: {stats['embedding_model']}")
            logger.info(f"   Dimension: {stats['embedding_dimension']}")

            return {
                "task_id": "vectorize_chunks",
                "run_id": metadata["run_id"],
                "total_chunks": len(chunks_list),
                "vectorized_count": total_vectorized,
                "inserted_count": total_inserted,
                "failed_batches": failed_batches,
                "collection_name": stats["collection_name"],
                "total_in_db": stats["total_chunks"],
                "success": True,
            }

        except Exception as e:
            logger.error(f"❌ Failed to vectorize chunks: {e}")
            return {
                "task_id": "vectorize_chunks",
                "run_id": metadata["run_id"],
                "vectorized_count": 0,
                "success": False,
                "error": str(e),
            }

    @task
    def cleanup_temp_data(metadata: dict) -> dict:
        """
        Task 7: Cleanup temporary data từ các task trước

        Args:
            metadata: Metadata từ task trước
        """
        logger.info("=" * 80)
        logger.info("TASK 7: Cleaning up temporary data")
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
    scraping_result = scrape_article_contents(parsing_result)
    chunking_result = chunk_articles(scraping_result)
    vectorization_result = vectorize_chunks(chunking_result)
    cleanup_result = cleanup_temp_data(vectorization_result)

    # Set dependencies
    (
        storage_init
        >> rss_metadata
        >> validation_result
        >> parsing_result
        >> scraping_result
        >> chunking_result
        >> vectorization_result
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
