"""
RSS Feed Parser
Parse RSS feeds và extract article information
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime
import xml.etree.ElementTree as ET
from lxml import etree
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RSSFeedParser:
    """
    Parser để parse RSS feeds và extract article information
    """

    def __init__(self, timeout: int = 30, max_retries: int = 3):
        """
        Initialize RSS Feed Parser

        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic"""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_rss_feed(self, url: str) -> Optional[str]:
        """
        Fetch RSS feed content

        Args:
            url: RSS feed URL

        Returns:
            RSS feed content (XML string) or None if failed
        """
        try:
            logger.info(f"📡 Fetching RSS feed: {url}")

            response = self.session.get(
                url,
                timeout=self.timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
            )

            response.raise_for_status()

            logger.info(f"✅ Fetched successfully: {len(response.content)} bytes")
            return response.text

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to fetch {url}: {e}")
            return None

    def parse_rss_feed(self, xml_content: str, category: str = None) -> List[Dict]:
        """
        Parse RSS feed XML content và extract article information

        Args:
            xml_content: RSS feed XML content
            category: Category name (optional)

        Returns:
            List of article dictionaries
        """
        try:
            # Parse XML với lxml
            root = etree.fromstring(xml_content.encode("utf-8"))

            articles = []

            # Find all <item> elements (RSS 2.0) or <entry> (Atom)
            items = root.xpath("//item") or root.xpath("//entry")

            logger.info(f"📰 Found {len(items)} articles")

            for idx, item in enumerate(items):
                try:
                    article = self._parse_article_item(item, category)
                    if article:
                        articles.append(article)
                except Exception as e:
                    logger.warning(f"⚠️  Failed to parse article {idx}: {e}")
                    continue

            logger.info(f"✅ Parsed {len(articles)} articles successfully")
            return articles

        except Exception as e:
            logger.error(f"❌ Failed to parse RSS feed: {e}")
            return []

    def _parse_article_item(
        self, item: etree.Element, category: str = None
    ) -> Optional[Dict]:
        """
        Parse single article item từ RSS feed

        Args:
            item: XML element (item or entry)
            category: Category name

        Returns:
            Article dictionary
        """
        article = {}

        # Title
        title_elem = item.find("title")
        article["title"] = (
            title_elem.text.strip()
            if title_elem is not None and title_elem.text
            else None
        )

        # Link
        link_elem = item.find("link")
        if link_elem is not None:
            # RSS 2.0: <link>url</link>
            article["link"] = link_elem.text.strip() if link_elem.text else None
            # Atom: <link href="url"/>
            if not article["link"] and link_elem.get("href"):
                article["link"] = link_elem.get("href").strip()
        else:
            article["link"] = None

        # Description/Summary
        desc_elem = item.find("description") or item.find("summary")
        article["description"] = (
            desc_elem.text.strip() if desc_elem is not None and desc_elem.text else None
        )

        # Published date
        pubdate_elem = item.find("pubDate") or item.find("published")
        if pubdate_elem is not None and pubdate_elem.text:
            article["published_date"] = pubdate_elem.text.strip()
        else:
            article["published_date"] = None

        # Author
        author_elem = item.find("author") or item.find(
            "dc:creator", namespaces={"dc": "http://purl.org/dc/elements/1.1/"}
        )
        article["author"] = (
            author_elem.text.strip()
            if author_elem is not None and author_elem.text
            else None
        )

        # Category (from RSS or parameter)
        category_elem = item.find("category")
        if category_elem is not None and category_elem.text:
            article["category"] = category_elem.text.strip()
        elif category:
            article["category"] = category
        else:
            article["category"] = None

        # GUID (unique identifier)
        guid_elem = item.find("guid")
        article["guid"] = (
            guid_elem.text.strip()
            if guid_elem is not None and guid_elem.text
            else article["link"]
        )

        # Thumbnail/Image
        # Try multiple possible image tags
        image_elem = (
            item.find(
                "media:thumbnail", namespaces={"media": "http://search.yahoo.com/mrss/"}
            )
            or item.find(
                "media:content", namespaces={"media": "http://search.yahoo.com/mrss/"}
            )
            or item.find("enclosure")
        )

        if image_elem is not None:
            article["thumbnail"] = image_elem.get("url") or image_elem.get("href")
        else:
            article["thumbnail"] = None

        # Only return if has required fields
        if article.get("title") and article.get("link"):
            return article
        else:
            logger.warning(f"⚠️  Skipping article without title or link")
            return None

    def parse_rss_url(self, url: str, category: str = None) -> List[Dict]:
        """
        Fetch và parse RSS feed từ URL

        Args:
            url: RSS feed URL
            category: Category name

        Returns:
            List of article dictionaries
        """
        # Fetch RSS content
        xml_content = self.fetch_rss_feed(url)

        if not xml_content:
            logger.error(f"❌ No content fetched from {url}")
            return []

        # Parse RSS content
        articles = self.parse_rss_feed(xml_content, category)

        return articles

    def parse_multiple_feeds(
        self, feeds: List[Dict[str, str]], max_articles_per_feed: Optional[int] = None
    ) -> List[Dict]:
        """
        Parse multiple RSS feeds (sequential)

        Args:
            feeds: List of feed dicts with 'url' and 'category' keys
            max_articles_per_feed: Maximum articles per feed (None = unlimited)

        Returns:
            List of all articles from all feeds
        """
        all_articles = []

        logger.info(f"🔄 Parsing {len(feeds)} RSS feeds...")

        for idx, feed in enumerate(feeds):
            url = feed.get("url")
            category = feed.get("category")

            logger.info(f"\n[{idx+1}/{len(feeds)}] Processing: {category} - {url}")

            try:
                articles = self.parse_rss_url(url, category)

                # Limit articles if specified
                if max_articles_per_feed and len(articles) > max_articles_per_feed:
                    articles = articles[:max_articles_per_feed]
                    logger.info(f"   Limited to {max_articles_per_feed} articles")

                all_articles.extend(articles)
                logger.info(f"   ✅ Added {len(articles)} articles")

            except Exception as e:
                logger.error(f"   ❌ Failed to parse feed: {e}")
                continue

        logger.info(f"\n✅ Total articles parsed: {len(all_articles)}")
        return all_articles

    def parse_multiple_feeds_parallel(
        self,
        feeds: List[Dict[str, str]],
        max_articles_per_feed: Optional[int] = None,
        max_workers: int = 5,
    ) -> List[Dict]:
        """
        Parse multiple RSS feeds in parallel (concurrent)

        Args:
            feeds: List of feed dicts with 'url' and 'category' keys
            max_articles_per_feed: Maximum articles per feed (None = unlimited)
            max_workers: Maximum number of parallel workers

        Returns:
            List of all articles from all feeds
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        all_articles = []

        logger.info(
            f"🔄 Parsing {len(feeds)} RSS feeds in parallel (max_workers={max_workers})..."
        )

        def parse_single_feed(feed_data: tuple) -> tuple:
            """Helper function to parse a single feed"""
            idx, feed = feed_data
            url = feed.get("url")
            category = feed.get("category")

            try:
                logger.info(f"[{idx+1}/{len(feeds)}] Starting: {category} - {url}")
                articles = self.parse_rss_url(url, category)

                # Limit articles if specified
                if max_articles_per_feed and len(articles) > max_articles_per_feed:
                    articles = articles[:max_articles_per_feed]
                    logger.info(
                        f"[{idx+1}/{len(feeds)}] Limited to {max_articles_per_feed} articles"
                    )

                logger.info(
                    f"[{idx+1}/{len(feeds)}] ✅ Completed: {len(articles)} articles"
                )
                return (idx, articles, None)

            except Exception as e:
                logger.error(f"[{idx+1}/{len(feeds)}] ❌ Failed: {e}")
                return (idx, [], str(e))

        # Parse feeds in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_feed = {
                executor.submit(parse_single_feed, (idx, feed)): (idx, feed)
                for idx, feed in enumerate(feeds)
            }

            # Collect results as they complete
            results = []
            for future in as_completed(future_to_feed):
                idx, articles, error = future.result()
                results.append((idx, articles, error))

        # Sort results by original order
        results.sort(key=lambda x: x[0])

        # Collect all articles
        for idx, articles, error in results:
            if error:
                logger.warning(f"Feed {idx+1} failed: {error}")
            all_articles.extend(articles)

        logger.info(f"\n✅ Total articles parsed: {len(all_articles)}")
        return all_articles


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def parse_rss_feeds(
    feeds: List[Dict[str, str]],
    timeout: int = 30,
    max_retries: int = 3,
    max_articles_per_feed: Optional[int] = None,
) -> List[Dict]:
    """
    Convenience function để parse multiple RSS feeds (Sequential)

    Args:
        feeds: List of feed dicts with 'url' and 'category' keys
        timeout: Request timeout
        max_retries: Max retries
        max_articles_per_feed: Max articles per feed

    Returns:
        List of all articles

    Example:
        >>> feeds = [
        ...     {"url": "https://example.com/rss", "category": "News"},
        ...     {"url": "https://example.com/tech.rss", "category": "Tech"}
        ... ]
        >>> articles = parse_rss_feeds(feeds)
    """
    parser = RSSFeedParser(timeout=timeout, max_retries=max_retries)
    return parser.parse_multiple_feeds(feeds, max_articles_per_feed)


def parse_rss_feeds_parallel(
    feeds: List[Dict[str, str]],
    timeout: int = 30,
    max_retries: int = 3,
    max_articles_per_feed: Optional[int] = None,
    max_workers: int = 5,
) -> List[Dict]:
    """
    Convenience function để parse multiple RSS feeds (Parallel)

    Uses ThreadPoolExecutor for concurrent parsing

    Args:
        feeds: List of feed dicts with 'url' and 'category' keys
        timeout: Request timeout
        max_retries: Max retries
        max_articles_per_feed: Max articles per feed
        max_workers: Number of parallel workers

    Returns:
        List of all articles

    Example:
        >>> feeds = [
        ...     {"url": "https://example.com/rss", "category": "News"},
        ...     {"url": "https://example.com/tech.rss", "category": "Tech"}
        ... ]
        >>> articles = parse_rss_feeds_parallel(feeds, max_workers=5)
    """
    parser = RSSFeedParser(timeout=timeout, max_retries=max_retries)
    return parser.parse_multiple_feeds_parallel(
        feeds, max_articles_per_feed, max_workers
    )


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Example feeds
    feeds = [
        {"url": "https://vnexpress.net/rss/tin-moi-nhat.rss", "category": "Trang chủ"},
        {"url": "https://vnexpress.net/rss/thoi-su.rss", "category": "Thời sự"},
    ]

    # Parse feeds
    parser = RSSFeedParser(timeout=30, max_retries=3)
    articles = parser.parse_multiple_feeds(feeds, max_articles_per_feed=5)

    # Display results
    print(f"\n📊 Total articles: {len(articles)}")
    print("\nSample articles:")
    for i, article in enumerate(articles[:3]):
        print(f"\n[{i+1}] {article['title']}")
        print(f"    Category: {article['category']}")
        print(f"    Link: {article['link']}")
        print(f"    Published: {article['published_date']}")
