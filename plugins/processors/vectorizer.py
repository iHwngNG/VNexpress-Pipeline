"""
Text Vectorizer - Flexible Embedding with ChromaDB Integration
Supports multiple embedding models (CPU/GPU) with easy switching
"""

import logging
from typing import List, Dict, Optional, Union, Any
from abc import ABC, abstractmethod
import pandas as pd
import pyarrow as pa
from datetime import datetime
import chromadb
from chromadb.config import Settings

logger = logging.getLogger(__name__)


# ============================================================================
# ABSTRACT BASE CLASS FOR EMBEDDINGS
# ============================================================================


class BaseEmbedding(ABC):
    """
    Abstract base class for embedding models
    Easy to extend with new models
    """

    @abstractmethod
    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        Embed list of texts

        Args:
            texts: List of text strings

        Returns:
            List of embedding vectors
        """
        pass

    @abstractmethod
    def get_dimension(self) -> int:
        """Get embedding dimension"""
        pass

    @abstractmethod
    def get_model_name(self) -> str:
        """Get model name"""
        pass


# ============================================================================
# SENTENCE TRANSFORMERS (CPU-FRIENDLY)
# ============================================================================


class SentenceTransformerEmbedding(BaseEmbedding):
    """
    Sentence Transformers - Works well on CPU
    Popular models: all-MiniLM-L6-v2, paraphrase-multilingual-MiniLM-L12-v2
    """

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        device: str = "cpu",
        batch_size: int = 32,
    ):
        """
        Initialize Sentence Transformer

        Args:
            model_name: Model name from sentence-transformers
            device: 'cpu' or 'cuda'
            batch_size: Batch size for encoding
        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )

        self.model_name = model_name
        self.device = device
        self.batch_size = batch_size

        logger.info(f"Loading model: {model_name} on {device}...")
        self.model = SentenceTransformer(model_name, device=device)
        logger.info(f"✅ Model loaded: {model_name}")

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Embed texts using Sentence Transformers"""
        if not texts:
            return []

        # Encode in batches
        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=len(texts) > 100,
            convert_to_numpy=True,
        )

        # Convert to list of lists
        return embeddings.tolist()

    def get_dimension(self) -> int:
        """Get embedding dimension"""
        return self.model.get_sentence_embedding_dimension()

    def get_model_name(self) -> str:
        """Get model name"""
        return self.model_name


# ============================================================================
# OPENAI EMBEDDINGS (API-BASED)
# ============================================================================


class OpenAIEmbedding(BaseEmbedding):
    """
    OpenAI Embeddings - API-based (requires API key)
    Models: text-embedding-3-small, text-embedding-3-large
    """

    def __init__(
        self,
        model_name: str = "text-embedding-3-small",
        api_key: Optional[str] = None,
        batch_size: int = 100,
    ):
        """
        Initialize OpenAI Embeddings

        Args:
            model_name: OpenAI model name
            api_key: OpenAI API key (or set OPENAI_API_KEY env var)
            batch_size: Batch size for API calls
        """
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError(
                "openai not installed. " "Install with: pip install openai"
            )

        self.model_name = model_name
        self.batch_size = batch_size
        self.client = OpenAI(api_key=api_key)

        # Embedding dimensions
        self.dimensions = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        }

        logger.info(f"✅ OpenAI Embeddings initialized: {model_name}")

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Embed texts using OpenAI API"""
        if not texts:
            return []

        all_embeddings = []

        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]

            response = self.client.embeddings.create(input=batch, model=self.model_name)

            batch_embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(batch_embeddings)

        return all_embeddings

    def get_dimension(self) -> int:
        """Get embedding dimension"""
        return self.dimensions.get(self.model_name, 1536)

    def get_model_name(self) -> str:
        """Get model name"""
        return self.model_name


# ============================================================================
# VECTORIZER CLASS
# ============================================================================


class TextVectorizer:
    """
    Text Vectorizer with ChromaDB integration
    Supports multiple embedding models with easy switching
    """

    def __init__(
        self,
        embedding_model: BaseEmbedding = None,
        chroma_path: str = "./chroma_db",
        collection_name: str = "news_articles",
    ):
        """
        Initialize Text Vectorizer

        Args:
            embedding_model: Embedding model instance
            chroma_path: Path to ChromaDB storage
            collection_name: ChromaDB collection name
        """
        # Default to Sentence Transformers (CPU-friendly)
        if embedding_model is None:
            logger.info("No embedding model provided, using default (all-MiniLM-L6-v2)")
            embedding_model = SentenceTransformerEmbedding(
                model_name="all-MiniLM-L6-v2", device="cpu"
            )

        self.embedding_model = embedding_model
        self.chroma_path = chroma_path
        self.collection_name = collection_name

        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(
            path=chroma_path, settings=Settings(anonymized_telemetry=False)
        )

        # Get or create collection
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={
                "embedding_model": self.embedding_model.get_model_name(),
                "embedding_dimension": self.embedding_model.get_dimension(),
            },
        )

        logger.info(f"✅ Vectorizer initialized")
        logger.info(f"   Model: {self.embedding_model.get_model_name()}")
        logger.info(f"   Dimension: {self.embedding_model.get_dimension()}")
        logger.info(f"   ChromaDB: {chroma_path}")
        logger.info(f"   Collection: {collection_name}")

    def vectorize_chunks(
        self,
        chunks: Union[List[Dict], pd.DataFrame, pa.Table],
        text_field: str = "chunk_text",
        metadata_fields: List[str] = None,
    ) -> Dict:
        """
        Vectorize text chunks and insert into ChromaDB

        Args:
            chunks: List of chunk dicts or DataFrame
            text_field: Field name for text content
            metadata_fields: Fields to store as metadata

        Returns:
            Dict with vectorization stats
        """
        # Convert to list of dicts
        if isinstance(chunks, pd.DataFrame):
            chunks_list = chunks.to_dict("records")
        elif isinstance(chunks, pa.Table):
            chunks_list = chunks.to_pandas().to_dict("records")
        else:
            chunks_list = chunks

        if not chunks_list:
            logger.warning("No chunks to vectorize")
            return {"total_chunks": 0, "vectorized": 0, "inserted": 0, "success": False}

        logger.info(f"🔄 Vectorizing {len(chunks_list)} chunks...")

        # Default metadata fields
        if metadata_fields is None:
            metadata_fields = [
                "title",
                "category",
                "published_date",
                "chunk_id",
                "total_chunks",
            ]

        # Extract texts
        texts = []
        ids = []
        metadatas = []

        for i, chunk in enumerate(chunks_list):
            # Get text
            text = chunk.get(text_field, "")
            if not text:
                logger.warning(f"Chunk {i} has no text, skipping")
                continue

            texts.append(text)

            # Generate unique ID
            chunk_id = chunk.get("chunk_id", i)
            title = chunk.get("title", "unknown")
            doc_id = f"{title}_{chunk_id}_{i}_{datetime.now().timestamp()}"
            ids.append(doc_id)

            # Extract metadata
            metadata = {}
            for field in metadata_fields:
                if field in chunk:
                    value = chunk[field]
                    # ChromaDB only supports str, int, float, bool
                    if isinstance(value, (str, int, float, bool)):
                        metadata[field] = value
                    else:
                        metadata[field] = str(value)

            # Add chunk size
            metadata["chunk_size"] = len(text)
            metadata["vectorized_at"] = datetime.now().isoformat()

            metadatas.append(metadata)

        if not texts:
            logger.error("No valid texts to vectorize")
            return {
                "total_chunks": len(chunks_list),
                "vectorized": 0,
                "inserted": 0,
                "success": False,
            }

        # Vectorize texts
        logger.info(f"🔄 Embedding {len(texts)} texts...")
        embeddings = self.embedding_model.embed_texts(texts)
        logger.info(f"✅ Embedded {len(embeddings)} texts")

        # Insert into ChromaDB
        logger.info(f"🔄 Inserting into ChromaDB...")
        try:
            self.collection.add(
                ids=ids, embeddings=embeddings, documents=texts, metadatas=metadatas
            )
            logger.info(f"✅ Inserted {len(ids)} chunks into ChromaDB")

            return {
                "total_chunks": len(chunks_list),
                "vectorized": len(embeddings),
                "inserted": len(ids),
                "collection_name": self.collection_name,
                "success": True,
            }

        except Exception as e:
            logger.error(f"❌ Error inserting into ChromaDB: {e}")
            return {
                "total_chunks": len(chunks_list),
                "vectorized": len(embeddings),
                "inserted": 0,
                "error": str(e),
                "success": False,
            }

    def search(
        self, query: str, n_results: int = 10, filter_metadata: Optional[Dict] = None
    ) -> Dict:
        """
        Search for similar chunks

        Args:
            query: Search query
            n_results: Number of results to return
            filter_metadata: Metadata filter (e.g., {'category': 'Tech'})

        Returns:
            Search results
        """
        logger.info(f"🔍 Searching for: {query}")

        # Embed query
        query_embedding = self.embedding_model.embed_texts([query])[0]

        # Search
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            where=filter_metadata,
        )

        logger.info(f"✅ Found {len(results['ids'][0])} results")

        return {
            "query": query,
            "n_results": len(results["ids"][0]),
            "documents": results["documents"][0],
            "metadatas": results["metadatas"][0],
            "distances": results["distances"][0],
        }

    def get_collection_stats(self) -> Dict:
        """Get collection statistics"""
        count = self.collection.count()

        return {
            "collection_name": self.collection_name,
            "total_chunks": count,
            "embedding_model": self.embedding_model.get_model_name(),
            "embedding_dimension": self.embedding_model.get_dimension(),
        }

    def check_article_exists(self, article_link: str) -> bool:
        """
        Check if article already exists in ChromaDB

        Args:
            article_link: URL of the article

        Returns:
            True if article exists, False otherwise
        """
        try:
            # Query by link metadata
            results = self.collection.get(where={"link": article_link}, limit=1)

            # If we get any results, article exists
            exists = len(results["ids"]) > 0

            if exists:
                logger.debug(f"Article already exists: {article_link}")

            return exists

        except Exception as e:
            logger.error(f"Error checking article existence: {e}")
            return False

    def filter_new_chunks(
        self,
        chunks: Union[List[Dict], pd.DataFrame, pa.Table],
        link_field: str = "link",
    ) -> List[Dict]:
        """
        Filter out chunks from articles that already exist in ChromaDB

        Args:
            chunks: List of chunks or DataFrame
            link_field: Field name for article link

        Returns:
            List of chunks from new articles only
        """
        # Convert to list of dicts
        if isinstance(chunks, pd.DataFrame):
            chunks_list = chunks.to_dict("records")
        elif isinstance(chunks, pa.Table):
            chunks_list = chunks.to_pandas().to_dict("records")
        else:
            chunks_list = chunks

        if not chunks_list:
            return []

        logger.info(f"🔍 Checking {len(chunks_list)} chunks for duplicates...")

        # Group chunks by article link
        articles_map = {}
        for chunk in chunks_list:
            link = chunk.get(link_field)
            if link:
                if link not in articles_map:
                    articles_map[link] = []
                articles_map[link].append(chunk)

        logger.info(f"   Found {len(articles_map)} unique articles")

        # Check which articles are new
        new_chunks = []
        existing_count = 0
        new_count = 0

        for link, article_chunks in articles_map.items():
            if self.check_article_exists(link):
                existing_count += 1
                logger.info(f"   ⏭️  Skipping existing article: {link}")
            else:
                new_count += 1
                new_chunks.extend(article_chunks)

        logger.info(f"\n📊 Duplicate Check Results:")
        logger.info(f"   Total articles: {len(articles_map)}")
        logger.info(f"   Existing (skipped): {existing_count}")
        logger.info(f"   New (to process): {new_count}")
        logger.info(f"   Total chunks to vectorize: {len(new_chunks)}")

        return new_chunks


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def create_vectorizer(
    model_type: str = "sentence-transformers",
    model_name: str = None,
    device: str = "cpu",
    chroma_path: str = "./chroma_db",
    collection_name: str = "news_articles",
    **kwargs,
) -> TextVectorizer:
    """
    Factory function to create vectorizer with specific model

    Args:
        model_type: 'sentence-transformers' or 'openai'
        model_name: Specific model name
        device: 'cpu' or 'cuda'
        chroma_path: ChromaDB path
        collection_name: Collection name
        **kwargs: Additional model-specific arguments

    Returns:
        TextVectorizer instance

    Example:
        >>> # CPU-friendly model
        >>> vectorizer = create_vectorizer(
        ...     model_type='sentence-transformers',
        ...     model_name='all-MiniLM-L6-v2',
        ...     device='cpu'
        ... )

        >>> # GPU model (when available)
        >>> vectorizer = create_vectorizer(
        ...     model_type='sentence-transformers',
        ...     model_name='all-mpnet-base-v2',
        ...     device='cuda'
        ... )

        >>> # OpenAI model
        >>> vectorizer = create_vectorizer(
        ...     model_type='openai',
        ...     model_name='text-embedding-3-small',
        ...     api_key='your-api-key'
        ... )
    """
    # Create embedding model
    if model_type == "sentence-transformers":
        if model_name is None:
            model_name = "all-MiniLM-L6-v2"  # Default CPU-friendly model

        embedding_model = SentenceTransformerEmbedding(
            model_name=model_name, device=device, **kwargs
        )

    elif model_type == "openai":
        if model_name is None:
            model_name = "text-embedding-3-small"

        embedding_model = OpenAIEmbedding(model_name=model_name, **kwargs)

    else:
        raise ValueError(f"Unknown model_type: {model_type}")

    # Create vectorizer
    vectorizer = TextVectorizer(
        embedding_model=embedding_model,
        chroma_path=chroma_path,
        collection_name=collection_name,
    )

    return vectorizer


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    print("\n" + "=" * 80)
    print("Text Vectorizer Demo")
    print("=" * 80)

    # Example chunks
    chunks = [
        {
            "chunk_id": 0,
            "chunk_text": "Việt Nam phát triển công nghệ AI trong y tế. Hệ thống chẩn đoán tự động.",
            "title": "AI trong y tế",
            "category": "Tech",
            "published_date": "2026-02-17",
            "total_chunks": 2,
        },
        {
            "chunk_id": 1,
            "chunk_text": "Độ chính xác đạt 95%. Triển khai tại 50 bệnh viện năm 2026.",
            "title": "AI trong y tế",
            "category": "Tech",
            "published_date": "2026-02-17",
            "total_chunks": 2,
        },
    ]

    # Create vectorizer (CPU-friendly)
    print("\n📦 Creating vectorizer...")
    vectorizer = create_vectorizer(
        model_type="sentence-transformers",
        model_name="all-MiniLM-L6-v2",
        device="cpu",
        chroma_path="./demo_chroma_db",
        collection_name="demo_articles",
    )

    # Vectorize and insert
    print("\n🔄 Vectorizing chunks...")
    result = vectorizer.vectorize_chunks(chunks)

    print(f"\n✅ Vectorization complete:")
    print(f"   Total chunks: {result['total_chunks']}")
    print(f"   Vectorized: {result['vectorized']}")
    print(f"   Inserted: {result['inserted']}")

    # Search
    print("\n🔍 Searching...")
    search_results = vectorizer.search(query="AI y tế Việt Nam", n_results=2)

    print(f"\n✅ Search results:")
    for i, (doc, metadata, distance) in enumerate(
        zip(
            search_results["documents"],
            search_results["metadatas"],
            search_results["distances"],
        )
    ):
        print(f"\n[Result {i+1}] (distance: {distance:.4f})")
        print(f"Title: {metadata.get('title')}")
        print(f"Category: {metadata.get('category')}")
        print(f"Text: {doc[:100]}...")

    # Stats
    print("\n📊 Collection stats:")
    stats = vectorizer.get_collection_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
