"""
Task Storage Manager - Scalable storage layer cho Airflow tasks
Hỗ trợ: File-based (Parquet) và Redis
Easy migration từ file → Redis khi cần scale
"""

import os
import shutil
from pathlib import Path
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
import json
import pickle
import logging
from abc import ABC, abstractmethod
from enum import Enum

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageBackend(Enum):
    """Storage backend types"""

    FILE = "file"
    REDIS = "redis"


class BaseStorage(ABC):
    """Abstract base class cho storage backends"""

    @abstractmethod
    def save(self, task_id: str, data: Any, run_id: str = None) -> str:
        """Save data and return path/key"""
        pass

    @abstractmethod
    def load(self, task_id: str, run_id: str = None) -> Any:
        """Load data from storage"""
        pass

    @abstractmethod
    def delete(self, task_id: str, run_id: str = None) -> bool:
        """Delete data from storage"""
        pass

    @abstractmethod
    def exists(self, task_id: str, run_id: str = None) -> bool:
        """Check if data exists"""
        pass

    @abstractmethod
    def cleanup_old_data(self, days: int = 7) -> int:
        """Cleanup data older than N days"""
        pass


class FileStorage(BaseStorage):
    """File-based storage sử dụng Parquet"""

    def __init__(self, base_path: str = "/tmp/airflow_task_storage"):
        """
        Args:
            base_path: Root directory cho task storage
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"FileStorage initialized at {self.base_path}")

    def _get_task_dir(self, task_id: str, run_id: str = None) -> Path:
        """Get directory path cho task"""
        if run_id:
            # /tmp/airflow_task_storage/run_2024-02-15_12-30-00/task_id/
            return self.base_path / run_id / task_id
        else:
            # /tmp/airflow_task_storage/task_id/
            return self.base_path / task_id

    def _serialize_data(self, data: Any) -> tuple[bytes, str]:
        """
        Serialize data và return (bytes, format)

        Returns:
            (serialized_bytes, format_type)
            format_type: 'parquet', 'json', 'pickle'
        """
        # Case 1: DataFrame/List of dicts → Parquet (tối ưu nhất)
        if isinstance(data, pd.DataFrame):
            # DataFrame
            table = pa.Table.from_pandas(data)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink, compression="snappy")
            return sink.getvalue().to_pybytes(), "parquet"

        elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            # List of dicts → DataFrame → Parquet
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink, compression="snappy")
            return sink.getvalue().to_pybytes(), "parquet"

        # Case 2: Simple JSON-serializable data → JSON
        elif isinstance(data, (dict, list, str, int, float, bool, type(None))):
            try:
                json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
                return json_bytes, "json"
            except (TypeError, ValueError):
                # Fallback to pickle
                pass

        # Case 3: Complex Python objects → Pickle
        return pickle.dumps(data), "pickle"

    def _deserialize_data(self, data_bytes: bytes, format_type: str) -> Any:
        """Deserialize data from bytes"""
        if format_type == "parquet":
            buffer = pa.py_buffer(data_bytes)
            table = pq.read_table(buffer)
            return table.to_pandas()

        elif format_type == "json":
            return json.loads(data_bytes.decode("utf-8"))

        elif format_type == "pickle":
            return pickle.loads(data_bytes)

        else:
            raise ValueError(f"Unknown format type: {format_type}")

    def save(self, task_id: str, data: Any, run_id: str = None) -> str:
        """
        Save data to file

        Args:
            task_id: Task identifier
            data: Data to save (DataFrame, list of dicts, dict, etc.)
            run_id: Optional run identifier (e.g., execution_date)

        Returns:
            Path to saved file
        """
        try:
            # Create task directory
            task_dir = self._get_task_dir(task_id, run_id)
            task_dir.mkdir(parents=True, exist_ok=True)

            # Serialize data
            data_bytes, format_type = self._serialize_data(data)

            # Save to file
            file_path = task_dir / f"data.{format_type}"
            with open(file_path, "wb") as f:
                f.write(data_bytes)

            # Save metadata
            metadata = {
                "task_id": task_id,
                "run_id": run_id,
                "format": format_type,
                "size_bytes": len(data_bytes),
                "saved_at": datetime.now().isoformat(),
            }
            metadata_path = task_dir / "metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            logger.info(
                f"✅ Saved {task_id} ({format_type}, {len(data_bytes)} bytes) to {file_path}"
            )
            return str(file_path)

        except Exception as e:
            logger.error(f"❌ Failed to save {task_id}: {e}")
            raise

    def load(self, task_id: str, run_id: str = None) -> Any:
        """
        Load data from file

        Args:
            task_id: Task identifier
            run_id: Optional run identifier

        Returns:
            Deserialized data
        """
        try:
            task_dir = self._get_task_dir(task_id, run_id)

            # Read metadata to get format
            metadata_path = task_dir / "metadata.json"
            if not metadata_path.exists():
                raise FileNotFoundError(f"Metadata not found for task {task_id}")

            with open(metadata_path, "r") as f:
                metadata = json.load(f)

            format_type = metadata["format"]
            file_path = task_dir / f"data.{format_type}"

            if not file_path.exists():
                raise FileNotFoundError(f"Data file not found: {file_path}")

            # Load and deserialize
            with open(file_path, "rb") as f:
                data_bytes = f.read()

            data = self._deserialize_data(data_bytes, format_type)

            logger.info(f"✅ Loaded {task_id} ({format_type}, {len(data_bytes)} bytes)")
            return data

        except Exception as e:
            logger.error(f"❌ Failed to load {task_id}: {e}")
            raise

    def delete(self, task_id: str, run_id: str = None) -> bool:
        """
        Delete task data

        Args:
            task_id: Task identifier
            run_id: Optional run identifier

        Returns:
            True if deleted successfully
        """
        try:
            task_dir = self._get_task_dir(task_id, run_id)

            if task_dir.exists():
                shutil.rmtree(task_dir)
                logger.info(f"✅ Deleted {task_id} from {task_dir}")
                return True
            else:
                logger.warning(f"⚠️  Task directory not found: {task_dir}")
                return False

        except Exception as e:
            logger.error(f"❌ Failed to delete {task_id}: {e}")
            return False

    def exists(self, task_id: str, run_id: str = None) -> bool:
        """Check if task data exists"""
        task_dir = self._get_task_dir(task_id, run_id)
        metadata_path = task_dir / "metadata.json"
        return metadata_path.exists()

    def cleanup_old_data(self, days: int = 7) -> int:
        """
        Cleanup data older than N days

        Args:
            days: Delete data older than this many days

        Returns:
            Number of directories deleted
        """
        cutoff_time = datetime.now() - timedelta(days=days)
        deleted_count = 0

        try:
            for item in self.base_path.iterdir():
                if item.is_dir():
                    # Check modification time
                    mtime = datetime.fromtimestamp(item.stat().st_mtime)

                    if mtime < cutoff_time:
                        shutil.rmtree(item)
                        deleted_count += 1
                        logger.info(f"🗑️  Deleted old data: {item.name}")

            logger.info(f"✅ Cleanup completed. Deleted {deleted_count} directories.")
            return deleted_count

        except Exception as e:
            logger.error(f"❌ Cleanup failed: {e}")
            return deleted_count


class RedisStorage(BaseStorage):
    """Redis-based storage (for future scaling)"""

    def __init__(
        self, host: str = "localhost", port: int = 6379, db: int = 0, ttl: int = 86400
    ):
        """
        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            ttl: Time-to-live in seconds (default 24h)
        """
        try:
            import redis

            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=False,  # Binary mode for pickle/parquet
            )
            self.redis_client.ping()  # Test connection
            self.ttl = ttl
            logger.info(f"RedisStorage connected to {host}:{port}")
        except ImportError:
            raise ImportError("Redis not installed. Run: pip install redis")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}")

    def _get_key(self, task_id: str, run_id: str = None) -> str:
        """Generate Redis key"""
        if run_id:
            return f"airflow:task:{run_id}:{task_id}"
        else:
            return f"airflow:task:{task_id}"

    def _serialize_data(self, data: Any) -> tuple[bytes, str]:
        """Same serialization logic as FileStorage"""
        # Reuse FileStorage logic
        file_storage = FileStorage()
        return file_storage._serialize_data(data)

    def _deserialize_data(self, data_bytes: bytes, format_type: str) -> Any:
        """Same deserialization logic as FileStorage"""
        file_storage = FileStorage()
        return file_storage._deserialize_data(data_bytes, format_type)

    def save(self, task_id: str, data: Any, run_id: str = None) -> str:
        """Save data to Redis"""
        try:
            key = self._get_key(task_id, run_id)

            # Serialize
            data_bytes, format_type = self._serialize_data(data)

            # Save to Redis với TTL
            # Store format type in separate key for deserialization
            format_key = f"{key}:format"

            pipeline = self.redis_client.pipeline()
            pipeline.set(key, data_bytes, ex=self.ttl)
            pipeline.set(format_key, format_type, ex=self.ttl)
            pipeline.execute()

            logger.info(
                f"✅ Saved {task_id} to Redis ({format_type}, {len(data_bytes)} bytes)"
            )
            return key

        except Exception as e:
            logger.error(f"❌ Failed to save to Redis: {e}")
            raise

    def load(self, task_id: str, run_id: str = None) -> Any:
        """Load data from Redis"""
        try:
            key = self._get_key(task_id, run_id)
            format_key = f"{key}:format"

            # Get data and format
            data_bytes = self.redis_client.get(key)
            format_type = self.redis_client.get(format_key)

            if data_bytes is None:
                raise KeyError(f"Data not found in Redis for task {task_id}")

            if format_type is None:
                raise KeyError(f"Format metadata not found for task {task_id}")

            format_type = format_type.decode("utf-8")

            # Deserialize
            data = self._deserialize_data(data_bytes, format_type)

            logger.info(f"✅ Loaded {task_id} from Redis ({format_type})")
            return data

        except Exception as e:
            logger.error(f"❌ Failed to load from Redis: {e}")
            raise

    def delete(self, task_id: str, run_id: str = None) -> bool:
        """Delete data from Redis"""
        try:
            key = self._get_key(task_id, run_id)
            format_key = f"{key}:format"

            deleted = self.redis_client.delete(key, format_key)

            if deleted > 0:
                logger.info(f"✅ Deleted {task_id} from Redis")
                return True
            else:
                logger.warning(f"⚠️  Key not found in Redis: {key}")
                return False

        except Exception as e:
            logger.error(f"❌ Failed to delete from Redis: {e}")
            return False

    def exists(self, task_id: str, run_id: str = None) -> bool:
        """Check if data exists in Redis"""
        key = self._get_key(task_id, run_id)
        return self.redis_client.exists(key) > 0

    def cleanup_old_data(self, days: int = 7) -> int:
        """
        Cleanup old data from Redis
        Note: Redis TTL handles this automatically
        """
        logger.info("Redis TTL handles automatic cleanup")
        return 0


class TaskStorageManager:
    """
    Main storage manager - facade pattern
    Easy migration giữa backends
    """

    def __init__(self, backend: StorageBackend = StorageBackend.FILE, **kwargs):
        """
        Args:
            backend: Storage backend to use
            **kwargs: Backend-specific config
        """
        self.backend_type = backend

        if backend == StorageBackend.FILE:
            base_path = kwargs.get("base_path", "/tmp/airflow_task_storage")
            self.storage = FileStorage(base_path=base_path)

        elif backend == StorageBackend.REDIS:
            self.storage = RedisStorage(
                host=kwargs.get("host", "localhost"),
                port=kwargs.get("port", 6379),
                db=kwargs.get("db", 0),
                ttl=kwargs.get("ttl", 86400),
            )

        else:
            raise ValueError(f"Unknown backend: {backend}")

        logger.info(f"TaskStorageManager initialized with {backend.value} backend")

    def save_task_output(self, task_id: str, data: Any, run_id: str = None) -> str:
        """Save task output"""
        return self.storage.save(task_id, data, run_id)

    def load_task_output(self, task_id: str, run_id: str = None) -> Any:
        """Load task output"""
        return self.storage.load(task_id, run_id)

    def delete_task_output(self, task_id: str, run_id: str = None) -> bool:
        """Delete task output"""
        return self.storage.delete(task_id, run_id)

    def task_exists(self, task_id: str, run_id: str = None) -> bool:
        """Check if task output exists"""
        return self.storage.exists(task_id, run_id)

    def cleanup(self, days: int = 7) -> int:
        """Cleanup old data"""
        return self.storage.cleanup_old_data(days)


# ============================================================================
# HELPER FUNCTIONS FOR AIRFLOW TASKFLOW API
# ============================================================================

# Global storage manager instance
_storage_manager = None


def get_storage_manager() -> TaskStorageManager:
    """Get global storage manager instance"""
    global _storage_manager
    if _storage_manager is None:
        # Default to file-based storage
        _storage_manager = TaskStorageManager(
            backend=StorageBackend.FILE, base_path="/tmp/airflow_task_storage"
        )
    return _storage_manager


def init_storage(backend: StorageBackend = StorageBackend.FILE, **kwargs):
    """
    Initialize storage manager globally
    Call this once in DAG file
    """
    global _storage_manager
    _storage_manager = TaskStorageManager(backend=backend, **kwargs)
    logger.info(f"Global storage manager initialized: {backend.value}")


def save_output(task_id: str, data: Any, run_id: str = None) -> str:
    """
    Save task output
    Helper function cho Airflow TaskFlow
    """
    manager = get_storage_manager()
    return manager.save_task_output(task_id, data, run_id)


def load_output(task_id: str, run_id: str = None) -> Any:
    """
    Load task output
    Helper function cho Airflow TaskFlow
    """
    manager = get_storage_manager()
    return manager.load_task_output(task_id, run_id)


def cleanup_output(task_id: str, run_id: str = None) -> bool:
    """
    Cleanup task output after use
    Helper function cho Airflow TaskFlow
    """
    manager = get_storage_manager()
    return manager.delete_task_output(task_id, run_id)


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    # Example 1: File-based storage
    print("\n" + "=" * 80)
    print("Example 1: File-based Storage")
    print("=" * 80)

    manager = TaskStorageManager(
        backend=StorageBackend.FILE, base_path="/tmp/test_storage"
    )

    # Save different data types
    test_data = [
        {"id": 1, "name": "Article 1", "category": "Tech"},
        {"id": 2, "name": "Article 2", "category": "Sports"},
    ]

    manager.save_task_output("scrape_rss", test_data, run_id="2024-02-15")

    # Load data
    loaded = manager.load_task_output("scrape_rss", run_id="2024-02-15")
    print(f"Loaded data: {loaded}")

    # Check exists
    exists = manager.task_exists("scrape_rss", run_id="2024-02-15")
    print(f"Task exists: {exists}")

    # Delete
    deleted = manager.delete_task_output("scrape_rss", run_id="2024-02-15")
    print(f"Deleted: {deleted}")

    # Example 2: Redis storage (nếu có Redis)
    # print("\n" + "="*80)
    # print("Example 2: Redis Storage")
    # print("="*80)
    #
    # manager_redis = TaskStorageManager(
    #     backend=StorageBackend.REDIS,
    #     host='localhost',
    #     port=6379,
    #     ttl=3600  # 1 hour
    # )
    #
    # manager_redis.save_task_output('test_task', test_data)
    # loaded_redis = manager_redis.load_task_output('test_task')
    # print(f"Loaded from Redis: {loaded_redis}")
