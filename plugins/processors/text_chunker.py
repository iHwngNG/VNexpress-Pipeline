"""
Semantic Text Chunker - Smart Chunking Based on Meaning
Split text into chunks based on semantic boundaries, not fixed size
"""

import logging
from typing import List, Dict, Optional, Union
import re
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)


class SemanticTextChunker:
    """
    Smart chunker tự động điều chỉnh chunk size dựa trên ý nghĩa
    """

    def __init__(
        self,
        min_chunk_size: int = 500,
        max_chunk_size: int = 1500,
        target_chunk_size: int = 1000,
        overlap_sentences: int = 2,
    ):
        """
        Initialize Semantic Text Chunker

        Args:
            min_chunk_size: Minimum characters per chunk
            max_chunk_size: Maximum characters per chunk
            target_chunk_size: Target size (flexible)
            overlap_sentences: Number of sentences to overlap
        """
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.target_chunk_size = target_chunk_size
        self.overlap_sentences = overlap_sentences

        # Sentence boundary patterns
        self.sentence_endings = re.compile(r"([.!?]+[\s\n]+)")

    def _split_into_sentences(self, text: str) -> List[str]:
        """
        Split text into sentences

        Args:
            text: Text to split

        Returns:
            List of sentences
        """
        if not text:
            return []

        # Split by sentence endings
        sentences = self.sentence_endings.split(text)

        # Combine sentence with its ending punctuation
        result = []
        for i in range(0, len(sentences) - 1, 2):
            sentence = sentences[i]
            ending = sentences[i + 1] if i + 1 < len(sentences) else ""
            combined = (sentence + ending).strip()
            if combined:
                result.append(combined)

        # Add last sentence if exists
        if len(sentences) % 2 == 1 and sentences[-1].strip():
            result.append(sentences[-1].strip())

        return result

    def _split_into_paragraphs(self, text: str) -> List[str]:
        """
        Split text into paragraphs

        Args:
            text: Text to split

        Returns:
            List of paragraphs
        """
        if not text:
            return []

        # Split by double newline
        paragraphs = re.split(r"\n\s*\n", text)

        # Clean and filter
        result = [p.strip() for p in paragraphs if p.strip()]

        return result

    def _create_semantic_chunks(self, sentences: List[str]) -> List[str]:
        """
        Create chunks based on semantic boundaries (sentences)

        Strategy:
        1. Start with empty chunk
        2. Add sentences until reaching target size
        3. If next sentence would exceed max_size, start new chunk
        4. If chunk is below min_size, try to add more sentences

        Args:
            sentences: List of sentences

        Returns:
            List of chunks
        """
        if not sentences:
            return []

        chunks = []
        current_chunk = []
        current_size = 0

        for sentence in sentences:
            sentence_size = len(sentence)

            # If adding this sentence would exceed max_size
            if current_size + sentence_size > self.max_chunk_size:
                # Save current chunk if it meets min_size
                if current_size >= self.min_chunk_size:
                    chunks.append(" ".join(current_chunk))
                    current_chunk = []
                    current_size = 0
                # If current chunk is too small, add anyway
                elif current_chunk:
                    current_chunk.append(sentence)
                    chunks.append(" ".join(current_chunk))
                    current_chunk = []
                    current_size = 0
                    continue

            # Add sentence to current chunk
            current_chunk.append(sentence)
            current_size += sentence_size + 1  # +1 for space

            # If we've reached target size, consider starting new chunk
            if current_size >= self.target_chunk_size:
                chunks.append(" ".join(current_chunk))
                current_chunk = []
                current_size = 0

        # Add remaining sentences
        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks

    def _add_sentence_overlap(self, chunks: List[str]) -> List[str]:
        """
        Add sentence overlap between chunks for context

        Args:
            chunks: List of chunks

        Returns:
            List of chunks with overlap
        """
        if len(chunks) <= 1 or self.overlap_sentences == 0:
            return chunks

        overlapped_chunks = []

        for i, chunk in enumerate(chunks):
            if i == 0:
                # First chunk: no overlap
                overlapped_chunks.append(chunk)
            else:
                # Get sentences from previous chunk
                prev_chunk = chunks[i - 1]
                prev_sentences = self._split_into_sentences(prev_chunk)

                # Take last N sentences for overlap
                overlap_sents = (
                    prev_sentences[-self.overlap_sentences :]
                    if len(prev_sentences) >= self.overlap_sentences
                    else prev_sentences
                )
                overlap_text = " ".join(overlap_sents)

                # Combine overlap + current chunk
                overlapped_chunk = overlap_text + " " + chunk
                overlapped_chunks.append(overlapped_chunk)

        return overlapped_chunks

    def chunk_text(self, text: str) -> List[str]:
        """
        Chunk text based on semantic boundaries

        Args:
            text: Text to chunk

        Returns:
            List of text chunks
        """
        if not text:
            return []

        # If text is small enough, return as-is
        if len(text) <= self.max_chunk_size:
            return [text]

        # Strategy 1: Try paragraph-level chunking first
        paragraphs = self._split_into_paragraphs(text)

        if len(paragraphs) > 1:
            # Chunk by paragraphs
            chunks = []
            current_chunk = []
            current_size = 0

            for para in paragraphs:
                para_size = len(para)

                # If paragraph itself is too large, split by sentences
                if para_size > self.max_chunk_size:
                    # Save current chunk if exists
                    if current_chunk:
                        chunks.append("\n\n".join(current_chunk))
                        current_chunk = []
                        current_size = 0

                    # Split large paragraph by sentences
                    sentences = self._split_into_sentences(para)
                    para_chunks = self._create_semantic_chunks(sentences)
                    chunks.extend(para_chunks)
                    continue

                # If adding this paragraph would exceed max_size
                if current_size + para_size > self.max_chunk_size:
                    if current_chunk:
                        chunks.append("\n\n".join(current_chunk))
                        current_chunk = []
                        current_size = 0

                # Add paragraph to current chunk
                current_chunk.append(para)
                current_size += para_size + 2  # +2 for \n\n

                # If we've reached target size
                if current_size >= self.target_chunk_size:
                    chunks.append("\n\n".join(current_chunk))
                    current_chunk = []
                    current_size = 0

            # Add remaining paragraphs
            if current_chunk:
                chunks.append("\n\n".join(current_chunk))
        else:
            # Strategy 2: Single paragraph or no paragraph breaks
            # Split by sentences
            sentences = self._split_into_sentences(text)
            chunks = self._create_semantic_chunks(sentences)

        # Add overlap for context
        chunks_with_overlap = self._add_sentence_overlap(chunks)

        return chunks_with_overlap

    def chunk_article(
        self,
        article: Dict,
        content_field: str = "content",
        metadata_fields: List[str] = None,
    ) -> List[Dict]:
        """
        Chunk article content and preserve metadata

        Args:
            article: Article dict
            content_field: Field name for content
            metadata_fields: Fields to preserve in each chunk

        Returns:
            List of chunk dicts with metadata
        """
        # Default metadata fields
        if metadata_fields is None:
            metadata_fields = ["title", "category", "published_date", "description"]

        # Get content
        content = article.get(content_field, "")

        if not content:
            logger.warning("No content found in article")
            return []

        # Chunk content
        chunks = self.chunk_text(content)

        # Build chunk dicts with metadata
        chunk_dicts = []
        for i, chunk in enumerate(chunks):
            chunk_dict = {
                "chunk_id": i,
                "chunk_text": chunk,
                "chunk_size": len(chunk),
                "total_chunks": len(chunks),
            }

            # Add metadata
            for field in metadata_fields:
                if field in article:
                    chunk_dict[field] = article[field]

            chunk_dicts.append(chunk_dict)

        return chunk_dicts

    def chunk_multiple_articles(
        self,
        articles: Union[List[Dict], pd.DataFrame, pa.Table],
        content_field: str = "content",
        metadata_fields: List[str] = None,
    ) -> List[Dict]:
        """
        Chunk multiple articles

        Args:
            articles: List of articles or DataFrame
            content_field: Field name for content
            metadata_fields: Fields to preserve

        Returns:
            List of all chunks from all articles
        """
        # Convert to list of dicts
        if isinstance(articles, pd.DataFrame):
            articles_list = articles.to_dict("records")
        elif isinstance(articles, pa.Table):
            articles_list = articles.to_pandas().to_dict("records")
        else:
            articles_list = articles

        logger.info(f"🔄 Chunking {len(articles_list)} articles (semantic)...")

        all_chunks = []

        for idx, article in enumerate(articles_list):
            try:
                chunks = self.chunk_article(
                    article,
                    content_field=content_field,
                    metadata_fields=metadata_fields,
                )

                all_chunks.extend(chunks)

                # Log chunk sizes
                sizes = [c["chunk_size"] for c in chunks]
                avg_size = sum(sizes) / len(sizes) if sizes else 0
                logger.info(
                    f"[{idx+1}/{len(articles_list)}] Created {len(chunks)} chunks "
                    f"(avg: {avg_size:.0f} chars)"
                )

            except Exception as e:
                logger.error(f"[{idx+1}/{len(articles_list)}] Error: {e}")
                continue

        logger.info(f"\n✅ Total chunks created: {len(all_chunks)}")

        return all_chunks


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def chunk_text(
    text: str,
    min_chunk_size: int = 500,
    max_chunk_size: int = 1500,
    target_chunk_size: int = 1000,
    overlap_sentences: int = 2,
) -> List[str]:
    """
    Convenience function to chunk text semantically

    Args:
        text: Text to chunk
        min_chunk_size: Minimum characters per chunk
        max_chunk_size: Maximum characters per chunk
        target_chunk_size: Target size (flexible)
        overlap_sentences: Number of sentences to overlap

    Returns:
        List of text chunks

    Example:
        >>> text = "Long article content..."
        >>> chunks = chunk_text(text, target_chunk_size=1000)
        >>> print(f"Created {len(chunks)} chunks")
    """
    chunker = SemanticTextChunker(
        min_chunk_size=min_chunk_size,
        max_chunk_size=max_chunk_size,
        target_chunk_size=target_chunk_size,
        overlap_sentences=overlap_sentences,
    )
    return chunker.chunk_text(text)


def chunk_articles(
    articles: Union[List[Dict], pd.DataFrame, pa.Table],
    content_field: str = "content",
    metadata_fields: List[str] = None,
    min_chunk_size: int = 500,
    max_chunk_size: int = 1500,
    target_chunk_size: int = 1000,
    overlap_sentences: int = 2,
) -> List[Dict]:
    """
    Convenience function to chunk multiple articles semantically (Sequential)

    Args:
        articles: Articles to chunk
        content_field: Field name for content
        metadata_fields: Fields to preserve
        min_chunk_size: Minimum characters per chunk
        max_chunk_size: Maximum characters per chunk
        target_chunk_size: Target size (flexible)
        overlap_sentences: Number of sentences to overlap

    Returns:
        List of all chunks

    Example:
        >>> articles = [
        ...     {'title': 'News 1', 'content': 'Long content...', 'category': 'Tech'},
        ... ]
        >>> chunks = chunk_articles(articles, target_chunk_size=1000)
        >>> print(f"Created {len(chunks)} chunks")
    """
    chunker = SemanticTextChunker(
        min_chunk_size=min_chunk_size,
        max_chunk_size=max_chunk_size,
        target_chunk_size=target_chunk_size,
        overlap_sentences=overlap_sentences,
    )
    return chunker.chunk_multiple_articles(
        articles, content_field=content_field, metadata_fields=metadata_fields
    )


def chunk_articles_parallel(
    articles: Union[List[Dict], pd.DataFrame, pa.Table],
    content_field: str = "content",
    metadata_fields: List[str] = None,
    min_chunk_size: int = 500,
    max_chunk_size: int = 1500,
    target_chunk_size: int = 1000,
    overlap_sentences: int = 2,
    max_workers: int = 5,
) -> List[Dict]:
    """
    Convenience function to chunk multiple articles semantically (Parallel)

    Uses ThreadPoolExecutor for concurrent chunking

    Args:
        articles: Articles to chunk
        content_field: Field name for content
        metadata_fields: Fields to preserve
        min_chunk_size: Minimum characters per chunk
        max_chunk_size: Maximum characters per chunk
        target_chunk_size: Target size (flexible)
        overlap_sentences: Number of sentences to overlap
        max_workers: Number of parallel workers

    Returns:
        List of all chunks

    Example:
        >>> articles = [
        ...     {'title': 'News 1', 'content': 'Long content...', 'category': 'Tech'},
        ... ]
        >>> chunks = chunk_articles_parallel(articles, max_workers=5)
        >>> print(f"Created {len(chunks)} chunks")
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Convert to list of dicts
    if isinstance(articles, pd.DataFrame):
        articles_list = articles.to_dict("records")
    elif isinstance(articles, pa.Table):
        articles_list = articles.to_pandas().to_dict("records")
    else:
        articles_list = articles

    chunker = SemanticTextChunker(
        min_chunk_size=min_chunk_size,
        max_chunk_size=max_chunk_size,
        target_chunk_size=target_chunk_size,
        overlap_sentences=overlap_sentences,
    )

    logger.info(
        f"🔄 Chunking {len(articles_list)} articles in parallel (max_workers={max_workers})..."
    )

    all_chunks = []
    failed_count = 0

    def chunk_single_article(article_data):
        """Helper to chunk single article"""
        idx, article = article_data
        try:
            chunks = chunker.chunk_article(
                article,
                content_field=content_field,
                metadata_fields=metadata_fields,
            )

            # Log chunk sizes
            sizes = [c["chunk_size"] for c in chunks]
            avg_size = sum(sizes) / len(sizes) if sizes else 0

            return (idx, chunks, avg_size)

        except Exception as e:
            logger.error(f"Error chunking article {idx}: {e}")
            return (idx, [], 0)

    # Parallel chunking
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks with index
        future_to_article = {
            executor.submit(chunk_single_article, (idx, article)): idx
            for idx, article in enumerate(articles_list)
        }

        # Process completed tasks
        for future in as_completed(future_to_article):
            try:
                idx, chunks, avg_size = future.result()

                if chunks:
                    all_chunks.extend(chunks)
                    logger.info(
                        f"[{idx+1}/{len(articles_list)}] ✅ Created {len(chunks)} chunks "
                        f"(avg: {avg_size:.0f} chars)"
                    )
                else:
                    failed_count += 1
                    logger.warning(f"[{idx+1}/{len(articles_list)}] ⚠️  Failed")

            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Error processing result: {e}")

    logger.info(f"\n✅ Parallel chunking complete:")
    logger.info(f"   Total articles: {len(articles_list)}")
    logger.info(f"   Total chunks: {len(all_chunks)}")
    logger.info(f"   Failed: {failed_count}")

    return all_chunks


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Example text with multiple paragraphs and sentences
    text = """
Việt Nam đang phát triển công nghệ AI trong y tế. Các nhà khoa học đã nghiên cứu 
hệ thống chẩn đoán bệnh tự động. Hệ thống này sử dụng deep learning để phân tích 
hình ảnh y khoa.

Độ chính xác của hệ thống đạt 95%. Điều này cho thấy tiềm năng lớn của AI trong 
y tế. Các bác sĩ có thể sử dụng hệ thống để hỗ trợ chẩn đoán.

Dự kiến triển khai tại 50 bệnh viện trong năm 2026. Đây là bước tiến quan trọng 
trong y tế Việt Nam. Bệnh nhân sẽ được hưởng lợi từ công nghệ tiên tiến này.
"""

    print("\n" + "=" * 80)
    print("Semantic Text Chunking Demo")
    print("=" * 80)

    # Chunk text
    chunks = chunk_text(
        text,
        min_chunk_size=100,
        max_chunk_size=300,
        target_chunk_size=200,
        overlap_sentences=1,
    )

    print(f"\n✅ Created {len(chunks)} semantic chunks:")
    for i, chunk in enumerate(chunks):
        print(f"\n[Chunk {i+1}] ({len(chunk)} chars):")
        print(f"{chunk}")
        print("-" * 80)

    # Example with article
    print("\n" + "=" * 80)
    print("Article Chunking Demo")
    print("=" * 80)

    articles = [
        {
            "title": "AI trong y tế Việt Nam",
            "content": text,
            "category": "Tech",
            "published_date": "2026-02-17",
        }
    ]

    article_chunks = chunk_articles(
        articles,
        min_chunk_size=100,
        max_chunk_size=300,
        target_chunk_size=200,
        overlap_sentences=1,
        metadata_fields=["title", "category"],
    )

    print(f"\n✅ Created {len(article_chunks)} chunks from {len(articles)} article(s)")
    for chunk in article_chunks:
        print(f"\n[Chunk {chunk['chunk_id']+1}/{chunk['total_chunks']}]")
        print(f"Title: {chunk['title']}")
        print(f"Size: {chunk['chunk_size']} chars")
        print(f"Text: {chunk['chunk_text']}")
        print("-" * 80)
