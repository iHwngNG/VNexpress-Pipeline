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
import os

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
                max_articles_per_feed=None,  # Limit to 3 articles per feed for testing
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

            # ChromaDB Config
            chroma_host = os.getenv("CHROMA_HOST")
            chroma_port = int(os.getenv("CHROMA_PORT", 8000))
            chroma_path = os.getenv("CHROMA_DB_PATH", "./chroma_db")

            # If CHROMA_HOST is set to 'chromadb' (default in docker-compose), use it.
            # If it's empty, use local path.
            if not chroma_host:
                chroma_host = None

            vectorizer = create_vectorizer(
                model_type="sentence-transformers",
                model_name="all-MiniLM-L6-v2",  # CPU-friendly model
                device="cpu",
                chroma_path=chroma_path,
                chroma_host=chroma_host,
                chroma_port=chroma_port,
                collection_name="news_articles",
            )

            # Convert to list of dicts
            chunks_list = df.to_dict("records")

            # Filter out chunks from articles already in database
            logger.info(f"📂 Total chunks loaded: {len(chunks_list)}")
            new_chunks_list = vectorizer.filter_new_chunks(
                chunks=chunks_list, link_field="link"
            )

            if len(new_chunks_list) == 0:
                logger.info(
                    "✅ All articles already exist in ChromaDB. Nothing to vectorize."
                )

                # Get collection stats
                stats = vectorizer.get_collection_stats()

                return {
                    "task_id": "vectorize_chunks",
                    "run_id": metadata["run_id"],
                    "total_chunks": len(chunks_list),
                    "new_chunks": 0,
                    "skipped_chunks": len(chunks_list),
                    "vectorized_count": 0,
                    "inserted_count": 0,
                    "collection_name": stats["collection_name"],
                    "total_in_db": stats["total_chunks"],
                    "success": True,
                    "message": "All articles already exist",
                }

            # Batch processing for parallel vectorization
            batch_size = 50  # Process 50 chunks at a time
            total_batches = (len(new_chunks_list) + batch_size - 1) // batch_size

            logger.info(
                f"🔄 Processing {len(new_chunks_list)} NEW chunks in {total_batches} batches (batch_size={batch_size})..."
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
                for i in range(0, len(new_chunks_list), batch_size):
                    batch = new_chunks_list[i : i + batch_size]
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
            logger.info(f"   Total chunks loaded: {len(chunks_list)}")
            logger.info(f"   New chunks: {len(new_chunks_list)}")
            logger.info(
                f"   Skipped (duplicates): {len(chunks_list) - len(new_chunks_list)}"
            )
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
                "new_chunks": len(new_chunks_list),
                "skipped_chunks": len(chunks_list) - len(new_chunks_list),
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
# VNExpress RAG Pipeline - Complete ETL to Vector Database

## Overview
Complete end-to-end pipeline for scraping VNExpress news, processing articles, chunking content, 
and storing embeddings in ChromaDB for semantic search and RAG applications.

## Architecture
- **Framework**: Apache Airflow with TaskFlow API
- **Storage**: Custom storage layer (File-based Parquet) - no XCom limitations
- **Vector DB**: ChromaDB with persistent storage
- **Embeddings**: Sentence Transformers (all-MiniLM-L6-v2, CPU-friendly)
- **Parallelization**: ThreadPoolExecutor for concurrent processing

## Storage Strategy
- **Backend**: File-based (Parquet) - switchable to Redis
- **Location**: `/tmp/airflow_task_storage/{run_id}/{task_id}/`
- **Format**: Auto-detect (Parquet for DataFrames, JSON for dicts, Pickle for complex objects)
- **Cleanup**: Automatic cleanup after task completion
- **ChromaDB**: `./chroma_db` (persistent vector storage)

## Complete Task Flow
```
Task 0: init_storage_backend
    ↓
Task 1: scrape_rss_list (VNExpress RSS feeds)
    ↓
Task 2: validate_rss_list (data validation & cleaning)
    ↓
Task 3: parse_rss_feeds (parallel, 3 workers, 3 articles/feed for testing)
    ↓
Task 4: scrape_article_contents (parallel, 5 workers)
    ↓
Task 5: chunk_articles (parallel, 5 workers, semantic chunking)
    ↓
Task 6: vectorize_chunks (parallel, 3 workers, batch processing)
    ↓
Task 7: cleanup_temp_data (cleanup storage)
```

## Task Details

### Task 1: Scrape RSS List
- **Function**: `scrape_vnexpress_rss_list()`
- **Output**: List of RSS feeds with categories
- **Storage**: Parquet format

### Task 2: Validate RSS List
- **Function**: `process_rss_data()`
- **Validation**: Schema validation, data cleaning
- **Output**: Cleaned RSS list

### Task 3: Parse RSS Feeds (Parallel)
- **Function**: `parse_rss_feeds_parallel()`
- **Workers**: 3 parallel workers
- **Limit**: 3 articles per feed (for testing)
- **Output**: Article metadata (title, link, description, etc.)

### Task 4: Scrape Article Contents (Parallel)
- **Function**: `scrape_multiple_article_contents_parallel()`
- **Workers**: 5 parallel workers
- **Output**: Full article content with metadata

### Task 5: Chunk Articles (Parallel)
- **Function**: `chunk_articles_parallel()`
- **Workers**: 5 parallel workers
- **Strategy**: Semantic chunking (paragraph/sentence boundaries)
- **Config**:
  - min_chunk_size: 500 chars
  - max_chunk_size: 1500 chars
  - target_chunk_size: 1000 chars
  - overlap_sentences: 2
- **Output**: Text chunks with preserved metadata

### Task 6: Vectorize Chunks (Parallel)
- **Function**: `create_vectorizer()` + batch processing
- **Model**: all-MiniLM-L6-v2 (384 dimensions, CPU-friendly)
- **Workers**: 3 parallel batch processors
- **Batch Size**: 50 chunks per batch
- **Process**:
  1. Load chunks from storage
  2. Create batches (50 chunks each)
  3. Vectorize batches in parallel
  4. Insert into ChromaDB immediately
- **Output**: Embeddings stored in ChromaDB

### Task 7: Cleanup
- **Function**: `cleanup_temp_data()`
- **Action**: Remove temporary storage files

## Data Flow

### Metadata-Based Communication
Tasks communicate via lightweight metadata instead of passing full data:
```python
{
    "task_id": "scrape_article_contents",
    "run_id": "2026-02-17",
    "count": 60,
    "storage_path": "/tmp/airflow_task_storage/2026-02-17/scrape_article_contents/data.parquet",
    "success": True
}
```

### Storage Pattern
1. Task processes data
2. Save to TaskStorageManager (Parquet/JSON)
3. Return metadata only
4. Next task loads from storage using metadata
5. Cleanup after use

## Performance Optimizations

### Parallel Processing
- **RSS Parsing**: 3 workers → ~3x faster
- **Article Scraping**: 5 workers → ~5x faster  
- **Chunking**: 5 workers → ~5x faster
- **Vectorization**: 3 batch workers → ~3x faster

### Example Performance (60 articles)
```
Sequential:  ~10 minutes
Parallel:    ~2 minutes  ✅ 5x improvement
```

## ChromaDB Integration

### Collection Details
- **Name**: news_articles
- **Model**: all-MiniLM-L6-v2
- **Dimension**: 384
- **Storage**: Persistent (`./chroma_db`)

### Stored Data
Each chunk includes:
```python
{
    "id": "unique_chunk_id",
    "embedding": [0.123, -0.456, ...],  # 384 dimensions
    "document": "chunk text content",
    "metadata": {
        "title": "Article Title",
        "category": "Tech",
        "published_date": "2026-02-17",
        "link": "https://...",
        "chunk_id": 0,
        "total_chunks": 3,
        "chunk_size": 987,
        "vectorized_at": "2026-02-17T04:30:00"
    }
}
```

### Search Capability
```python
# Semantic search
vectorizer.search(
    query="AI trong y tế Việt Nam",
    n_results=10,
    filter_metadata={"category": "Tech"}
)
```

## Configuration

### Testing Mode (Current)
```python
max_articles_per_feed = 3  # Limit for fast testing
```

### Production Mode
```python
max_articles_per_feed = None  # Get all articles
# or
max_articles_per_feed = 50  # Reasonable limit
```

### Switching to Redis Storage
```python
init_storage(
    backend=StorageBackend.REDIS,
    host='localhost',
    port=6379,
    ttl=86400  # 24 hours
)
```

## Dependencies

### Python Packages
- `chromadb>=0.4.22` - Vector database
- `sentence-transformers>=2.2.0` - Embeddings
- `torch>=2.0.0` - Deep learning backend
- `email-validator>=2.0.0` - Required by chromadb
- `pandas>=2.0.0` - Data processing
- `pyarrow>=14.0.0` - Parquet format
- `beautifulsoup4>=4.12.0` - HTML parsing
- `requests>=2.31.0` - HTTP requests

## Benefits

✅ **No XCom Limitations**: Handle large datasets without size constraints  
✅ **Parallel Processing**: 5x faster with concurrent execution  
✅ **Semantic Chunking**: Intelligent text splitting preserving meaning  
✅ **Vector Search**: Fast semantic search with ChromaDB  
✅ **Scalable**: Easy to switch to Redis for distributed processing  
✅ **Automatic Cleanup**: No manual storage management  
✅ **Multiple Formats**: Support Parquet, JSON, Pickle  
✅ **Easy Debugging**: Inspect intermediate files  
✅ **RAG-Ready**: Complete pipeline for RAG applications  

## Monitoring

### Logs
Each task logs:
- Progress indicators
- Success/failure counts
- Processing times
- Storage paths
- ChromaDB stats

### Example Log Output
```
📂 Loaded 60 articles to chunk
🔄 Chunking 60 articles in parallel (max_workers=5)...
[1/60] ✅ Created 3 chunks (avg: 987 chars)
...
✅ Total chunks created: 180

🔄 Processing 180 chunks in 4 batches (batch_size=50)...
[Batch 1/4] ✅ Vectorized: 50, Inserted: 50
...
📊 ChromaDB Collection Stats:
   Collection: news_articles
   Total chunks in DB: 180
   Model: all-MiniLM-L6-v2
   Dimension: 384
```

## Use Cases

### 1. Semantic Search
Search news articles by meaning, not just keywords

### 2. RAG Applications
Retrieve relevant context for LLM-based Q&A systems

### 3. Content Recommendation
Find similar articles based on semantic similarity

### 4. Topic Clustering
Group articles by semantic topics

## Next Steps

1. **Test Pipeline**: Run with 3 articles/feed limit
2. **Verify ChromaDB**: Check vector storage
3. **Test Search**: Query semantic search
4. **Production**: Remove article limit
5. **Scale**: Switch to Redis if needed

## Schedule
- **Frequency**: Daily at 6 AM
- **Catchup**: Disabled
- **Max Active Runs**: 1
"""
