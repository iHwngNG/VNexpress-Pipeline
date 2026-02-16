"""
Article Deduplicator - Optimized Version
Remove duplicate articles based on exact match criteria
"""

import pandas as pd
import pyarrow as pa
from typing import List, Dict, Optional, Union
import logging

logger = logging.getLogger(__name__)


class ArticleDeduplicator:
    """
    Optimized deduplicator để loại bỏ duplicate articles
    Sử dụng exact match thay vì similarity để tối ưu tốc độ
    """

    def __init__(self):
        """Initialize Article Deduplicator"""
        pass

    def remove_duplicates_by_url(
        self,
        articles: Union[List[Dict], pd.DataFrame, pa.Table],
        url_field: str = "link",
        keep: str = "first",
    ) -> Union[List[Dict], pd.DataFrame, pa.Table]:
        """
        Remove duplicates based on URL (exact match)

        Args:
            articles: Articles data
            url_field: Field name for URL
            keep: 'first' or 'last'

        Returns:
            Deduplicated articles (same type as input)
        """
        input_type = type(articles)

        # Convert to DataFrame
        if isinstance(articles, list):
            df = pd.DataFrame(articles)
        elif isinstance(articles, pa.Table):
            df = articles.to_pandas()
        else:
            df = articles.copy()

        original_count = len(df)

        # Check if field exists
        if url_field not in df.columns:
            logger.warning(
                f"⚠️  Field '{url_field}' not found, skipping URL deduplication"
            )
            return articles

        # Remove duplicates (fast pandas operation)
        df_clean = df.drop_duplicates(subset=[url_field], keep=keep)

        removed_count = original_count - len(df_clean)
        if removed_count > 0:
            logger.info(f"   🗑️  Removed {removed_count} duplicate(s) by URL")

        # Convert back to original type
        if input_type == list:
            return df_clean.to_dict("records")
        elif input_type == pa.Table:
            return pa.Table.from_pandas(df_clean.reset_index(drop=True))
        else:
            return df_clean.reset_index(drop=True)

    def remove_duplicates_by_guid(
        self,
        articles: Union[List[Dict], pd.DataFrame, pa.Table],
        guid_field: str = "guid",
        keep: str = "first",
    ) -> Union[List[Dict], pd.DataFrame, pa.Table]:
        """
        Remove duplicates based on GUID (exact match)

        Args:
            articles: Articles data
            guid_field: Field name for GUID
            keep: 'first' or 'last'

        Returns:
            Deduplicated articles
        """
        input_type = type(articles)

        # Convert to DataFrame
        if isinstance(articles, list):
            df = pd.DataFrame(articles)
        elif isinstance(articles, pa.Table):
            df = articles.to_pandas()
        else:
            df = articles.copy()

        original_count = len(df)

        # Check if field exists
        if guid_field not in df.columns:
            logger.warning(
                f"⚠️  Field '{guid_field}' not found, skipping GUID deduplication"
            )
            return articles

        # Only deduplicate non-null GUIDs
        # Keep null GUIDs as-is (don't remove them)
        mask_null = df[guid_field].isna()
        df_with_guid = df[~mask_null]
        df_null_guid = df[mask_null]

        # Remove duplicates from non-null GUIDs only
        df_clean = df_with_guid.drop_duplicates(subset=[guid_field], keep=keep)

        # Combine back with null GUIDs
        df_clean = pd.concat([df_clean, df_null_guid], ignore_index=True)

        removed_count = original_count - len(df_clean)
        if removed_count > 0:
            logger.info(f"   🗑️  Removed {removed_count} duplicate(s) by GUID")

        # Convert back to original type
        if input_type == list:
            return df_clean.to_dict("records")
        elif input_type == pa.Table:
            return pa.Table.from_pandas(df_clean.reset_index(drop=True))
        else:
            return df_clean.reset_index(drop=True)

    def remove_duplicates_by_title(
        self,
        articles: Union[List[Dict], pd.DataFrame, pa.Table],
        title_field: str = "title",
        keep: str = "first",
    ) -> Union[List[Dict], pd.DataFrame, pa.Table]:
        """
        Remove duplicates based on exact title match

        Args:
            articles: Articles data
            title_field: Field name for title
            keep: 'first' or 'last'

        Returns:
            Deduplicated articles
        """
        input_type = type(articles)

        # Convert to DataFrame
        if isinstance(articles, list):
            df = pd.DataFrame(articles)
        elif isinstance(articles, pa.Table):
            df = articles.to_pandas()
        else:
            df = articles.copy()

        original_count = len(df)

        # Check if field exists
        if title_field not in df.columns:
            logger.warning(
                f"⚠️  Field '{title_field}' not found, skipping title deduplication"
            )
            return articles

        # Only deduplicate non-null titles
        # Keep null titles as-is (don't remove them)
        mask_null = df[title_field].isna()
        df_with_title = df[~mask_null].copy()
        df_null_title = df[mask_null]

        # Normalize titles (strip whitespace, lowercase)
        df_with_title["_title_normalized"] = (
            df_with_title[title_field].str.strip().str.lower()
        )

        # Remove duplicates by normalized title (fast pandas operation)
        df_clean = df_with_title.drop_duplicates(
            subset=["_title_normalized"], keep=keep
        )

        # Drop temporary column
        df_clean = df_clean.drop(columns=["_title_normalized"])

        # Combine back with null titles
        df_clean = pd.concat([df_clean, df_null_title], ignore_index=True)

        removed_count = original_count - len(df_clean)
        if removed_count > 0:
            logger.info(
                f"   🗑️  Removed {removed_count} duplicate(s) by title (exact match)"
            )

        # Convert back to original type
        if input_type == list:
            return df_clean.to_dict("records")
        elif input_type == pa.Table:
            return pa.Table.from_pandas(df_clean.reset_index(drop=True))
        else:
            return df_clean.reset_index(drop=True)

    def remove_duplicates_by_content(
        self,
        articles: Union[List[Dict], pd.DataFrame, pa.Table],
        content_field: str = "description",
        keep: str = "first",
    ) -> Union[List[Dict], pd.DataFrame, pa.Table]:
        """
        Remove duplicates based on exact content match

        Args:
            articles: Articles data
            content_field: Field name for content/description
            keep: 'first' or 'last'

        Returns:
            Deduplicated articles
        """
        input_type = type(articles)

        # Convert to DataFrame
        if isinstance(articles, list):
            df = pd.DataFrame(articles)
        elif isinstance(articles, pa.Table):
            df = articles.to_pandas()
        else:
            df = articles.copy()

        original_count = len(df)

        # Check if field exists
        if content_field not in df.columns:
            logger.warning(
                f"⚠️  Field '{content_field}' not found, skipping content deduplication"
            )
            return articles

        # Only deduplicate non-null content
        # Keep null content as-is (don't remove them)
        mask_null = df[content_field].isna()
        df_with_content = df[~mask_null].copy()
        df_null_content = df[mask_null]

        # Normalize content (strip whitespace, lowercase)
        df_with_content["_content_normalized"] = (
            df_with_content[content_field].str.strip().str.lower()
        )

        # Remove duplicates by normalized content (fast pandas operation)
        df_clean = df_with_content.drop_duplicates(
            subset=["_content_normalized"], keep=keep
        )

        # Drop temporary column
        df_clean = df_clean.drop(columns=["_content_normalized"])

        # Combine back with null content
        df_clean = pd.concat([df_clean, df_null_content], ignore_index=True)

        removed_count = original_count - len(df_clean)
        if removed_count > 0:
            logger.info(
                f"   🗑️  Removed {removed_count} duplicate(s) by content (exact match)"
            )

        # Convert back to original type
        if input_type == list:
            return df_clean.to_dict("records")
        elif input_type == pa.Table:
            return pa.Table.from_pandas(df_clean.reset_index(drop=True))
        else:
            return df_clean.reset_index(drop=True)

    def remove_duplicates_by_title_and_content(
        self,
        articles: Union[List[Dict], pd.DataFrame, pa.Table],
        title_field: str = "title",
        content_field: str = "description",
        keep: str = "first",
    ) -> Union[List[Dict], pd.DataFrame, pa.Table]:
        """
        Remove duplicates based on exact match of both title AND content

        Args:
            articles: Articles data
            title_field: Field name for title
            content_field: Field name for content/description
            keep: 'first' or 'last'

        Returns:
            Deduplicated articles
        """
        input_type = type(articles)

        # Convert to DataFrame
        if isinstance(articles, list):
            df = pd.DataFrame(articles)
        elif isinstance(articles, pa.Table):
            df = articles.to_pandas()
        else:
            df = articles.copy()

        original_count = len(df)

        # Check if fields exist
        if title_field not in df.columns or content_field not in df.columns:
            logger.warning(
                f"⚠️  Required fields not found, skipping title+content deduplication"
            )
            return articles

        # Only deduplicate non-null values
        # Keep records with null title or content as-is (don't remove them)
        mask_null = df[title_field].isna() | df[content_field].isna()
        df_with_both = df[~mask_null].copy()
        df_with_null = df[mask_null]

        # Normalize both fields
        df_with_both["_title_normalized"] = (
            df_with_both[title_field].str.strip().str.lower()
        )
        df_with_both["_content_normalized"] = (
            df_with_both[content_field].str.strip().str.lower()
        )

        # Remove duplicates by both fields (fast pandas operation)
        df_clean = df_with_both.drop_duplicates(
            subset=["_title_normalized", "_content_normalized"], keep=keep
        )

        # Drop temporary columns
        df_clean = df_clean.drop(columns=["_title_normalized", "_content_normalized"])

        # Combine back with null records
        df_clean = pd.concat([df_clean, df_with_null], ignore_index=True)

        removed_count = original_count - len(df_clean)
        if removed_count > 0:
            logger.info(
                f"   🗑️  Removed {removed_count} duplicate(s) by title+content (exact match)"
            )

        # Convert back to original type
        if input_type == list:
            return df_clean.to_dict("records")
        elif input_type == pa.Table:
            return pa.Table.from_pandas(df_clean.reset_index(drop=True))
        else:
            return df_clean.reset_index(drop=True)

    def deduplicate(
        self,
        articles: Union[List[Dict], pd.DataFrame, pa.Table],
        by_url: bool = True,
        by_guid: bool = True,
        by_title: bool = False,
        by_content: bool = False,
        by_title_and_content: bool = False,
        keep: str = "first",
    ) -> Union[List[Dict], pd.DataFrame, pa.Table]:
        """
        Full deduplication pipeline (optimized for speed)

        Args:
            articles: Articles data
            by_url: Remove duplicates by URL (exact match)
            by_guid: Remove duplicates by GUID (exact match)
            by_title: Remove duplicates by title (exact match)
            by_content: Remove duplicates by content (exact match)
            by_title_and_content: Remove duplicates by both title AND content
            keep: 'first' or 'last'

        Returns:
            Deduplicated articles
        """
        logger.info("🔄 Starting Article Deduplication (Optimized)")

        # Get original count
        if isinstance(articles, list):
            original_count = len(articles)
        elif isinstance(articles, pa.Table):
            original_count = len(articles)
        else:
            original_count = len(articles)

        logger.info(f"   📊 Input: {original_count} articles")

        result = articles

        # Step 1: Remove by URL (fastest, most reliable)
        if by_url:
            logger.info("\n   [1] Removing duplicates by URL...")
            result = self.remove_duplicates_by_url(result, keep=keep)

        # Step 2: Remove by GUID (fast, reliable)
        if by_guid:
            logger.info("\n   [2] Removing duplicates by GUID...")
            result = self.remove_duplicates_by_guid(result, keep=keep)

        # Step 3: Remove by title AND content (exact match)
        if by_title_and_content:
            logger.info("\n   [3] Removing duplicates by title+content...")
            result = self.remove_duplicates_by_title_and_content(result, keep=keep)

        # Step 4: Remove by title only (if not using title+content)
        if by_title and not by_title_and_content:
            logger.info("\n   [4] Removing duplicates by title...")
            result = self.remove_duplicates_by_title(result, keep=keep)

        # Step 5: Remove by content only (if not using title+content)
        if by_content and not by_title_and_content:
            logger.info("\n   [5] Removing duplicates by content...")
            result = self.remove_duplicates_by_content(result, keep=keep)

        # Final count
        if isinstance(result, list):
            final_count = len(result)
        elif isinstance(result, pa.Table):
            final_count = len(result)
        else:
            final_count = len(result)

        removed_total = original_count - final_count

        logger.info(f"\n✅ Deduplication Complete")
        logger.info(f"   Original: {original_count} articles")
        logger.info(f"   Final: {final_count} articles")
        logger.info(f"   Removed: {removed_total} duplicates")

        return result


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def deduplicate_articles(
    articles: Union[List[Dict], pd.DataFrame, pa.Table],
    by_url: bool = True,
    by_guid: bool = True,
    by_title: bool = False,
    by_content: bool = False,
    by_title_and_content: bool = False,
    keep: str = "first",
) -> Union[List[Dict], pd.DataFrame, pa.Table]:
    """
    Convenience function to deduplicate articles (optimized)

    Args:
        articles: Articles data
        by_url: Remove duplicates by URL (exact match)
        by_guid: Remove duplicates by GUID (exact match)
        by_title: Remove duplicates by title (exact match)
        by_content: Remove duplicates by content (exact match)
        by_title_and_content: Remove duplicates by both title AND content
        keep: 'first' or 'last'

    Returns:
        Deduplicated articles

    Example:
        >>> articles = [
        ...     {"title": "News 1", "link": "http://example.com/1", "description": "Content 1"},
        ...     {"title": "News 1", "link": "http://example.com/1", "description": "Content 1"},  # Exact duplicate
        ...     {"title": "News 2", "link": "http://example.com/2", "description": "Content 2"},
        ... ]
        >>> clean_articles = deduplicate_articles(articles)
        # Result: 2 articles (removed 1 exact duplicate)
    """
    deduplicator = ArticleDeduplicator()
    return deduplicator.deduplicate(
        articles,
        by_url=by_url,
        by_guid=by_guid,
        by_title=by_title,
        by_content=by_content,
        by_title_and_content=by_title_and_content,
        keep=keep,
    )


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Example articles with exact duplicates
    articles = [
        {
            "title": "Việt Nam thắng 3-0",
            "link": "https://example.com/1",
            "guid": "1",
            "description": "Đội tuyển Việt Nam thắng 3-0",
        },
        {
            "title": "Việt Nam thắng 3-0",
            "link": "https://example.com/1",
            "guid": "1",
            "description": "Đội tuyển Việt Nam thắng 3-0",
        },  # Exact duplicate
        {
            "title": "Giá vàng tăng cao",
            "link": "https://example.com/2",
            "guid": "2",
            "description": "Giá vàng tăng mạnh",
        },
        {
            "title": "Giá vàng tăng cao",
            "link": "https://example.com/3",
            "guid": "3",
            "description": "Giá vàng tăng mạnh",
        },  # Same title+content, different URL
        {
            "title": "iPhone 16 ra mắt",
            "link": "https://example.com/4",
            "guid": "4",
            "description": "Apple ra mắt iPhone 16",
        },
    ]

    print("\n📊 Original articles:")
    print(f"Count: {len(articles)}")
    for i, article in enumerate(articles):
        print(f"  [{i}] {article['title']} - {article['link']}")

    # Deduplicate
    clean_articles = deduplicate_articles(
        articles,
        by_url=True,
        by_guid=True,
        by_title_and_content=True,  # Remove exact matches
    )

    print("\n✅ Deduplicated articles:")
    print(f"Count: {len(clean_articles)}")
    for i, article in enumerate(clean_articles):
        print(f"  [{i}] {article['title']} - {article['link']}")
