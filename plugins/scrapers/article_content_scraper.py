"""
Article Content Scraper
Fetch full article content and metadata from article URLs
"""

import logging
from typing import Dict, List, Optional, Union
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ArticleContentScraper:
    """
    Scraper để fetch full content và metadata từ article URLs
    """

    def __init__(self, timeout: int = 30, max_retries: int = 3, user_agent: str = None):
        """
        Initialize Article Content Scraper

        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries
            user_agent: Custom user agent string
        """
        self.timeout = timeout
        self.max_retries = max_retries

        # Default user agent
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        # Setup session with retry strategy
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy"""
        session = requests.Session()

        # Retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set headers
        session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
        )

        return session

    def _fetch_html(self, url: str) -> Optional[str]:
        """
        Fetch HTML content from URL

        Args:
            url: Article URL

        Returns:
            HTML content or None if failed
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Check content type
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type:
                logger.warning(f"Non-HTML content type: {content_type}")
                return None

            return response.text

        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching URL: {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching URL {url}: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""

        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text)

        # Strip leading/trailing whitespace
        text = text.strip()

        return text

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract article title from HTML

        Priority:
        1. <meta property="og:title">
        2. <meta name="twitter:title">
        3. <h1> tag
        4. <title> tag
        """
        # Try Open Graph title
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return self._clean_text(og_title["content"])

        # Try Twitter title
        twitter_title = soup.find("meta", attrs={"name": "twitter:title"})
        if twitter_title and twitter_title.get("content"):
            return self._clean_text(twitter_title["content"])

        # Try h1 tag
        h1 = soup.find("h1")
        if h1:
            return self._clean_text(h1.get_text())

        # Fallback to title tag
        title = soup.find("title")
        if title:
            return self._clean_text(title.get_text())

        return None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract article description/summary

        Priority:
        1. <meta property="og:description">
        2. <meta name="description">
        3. <meta name="twitter:description">
        """
        # Try Open Graph description
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            return self._clean_text(og_desc["content"])

        # Try meta description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            return self._clean_text(meta_desc["content"])

        # Try Twitter description
        twitter_desc = soup.find("meta", attrs={"name": "twitter:description"})
        if twitter_desc and twitter_desc.get("content"):
            return self._clean_text(twitter_desc["content"])

        return None

    def _extract_published_date(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract published date

        Priority:
        1. <meta property="article:published_time">
        2. <meta name="pubdate">
        3. <time> tag with datetime attribute
        """
        # Try article:published_time
        article_time = soup.find("meta", property="article:published_time")
        if article_time and article_time.get("content"):
            return article_time["content"]

        # Try pubdate
        pubdate = soup.find("meta", attrs={"name": "pubdate"})
        if pubdate and pubdate.get("content"):
            return pubdate["content"]

        # Try time tag
        time_tag = soup.find("time", attrs={"datetime": True})
        if time_tag:
            return time_tag["datetime"]

        return None

    def _extract_category(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        """
        Extract article category

        Priority:
        1. <meta property="article:section">
        2. <meta name="category">
        3. Extract from URL path
        """
        # Try article:section
        article_section = soup.find("meta", property="article:section")
        if article_section and article_section.get("content"):
            return self._clean_text(article_section["content"])

        # Try meta category
        meta_category = soup.find("meta", attrs={"name": "category"})
        if meta_category and meta_category.get("content"):
            return self._clean_text(meta_category["content"])

        # Try to extract from URL
        parsed_url = urlparse(url)
        path_parts = [p for p in parsed_url.path.split("/") if p]
        if path_parts:
            # Usually first part of path is category
            return path_parts[0]

        return None

    def _extract_content(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract main article content

        Strategy:
        1. Look for common article containers
        2. Remove unwanted elements (ads, scripts, etc.)
        3. Extract text from paragraphs
        """
        # Remove unwanted elements
        for element in soup.find_all(
            ["script", "style", "nav", "header", "footer", "aside", "iframe"]
        ):
            element.decompose()

        # Common article container selectors
        article_selectors = [
            "article",
            '[class*="article-content"]',
            '[class*="post-content"]',
            '[class*="entry-content"]',
            '[id*="article-content"]',
            '[id*="post-content"]',
            ".content",
            "main",
        ]

        content_container = None
        for selector in article_selectors:
            content_container = soup.select_one(selector)
            if content_container:
                break

        # If no container found, use body
        if not content_container:
            content_container = soup.find("body")

        if not content_container:
            return None

        # Extract paragraphs
        paragraphs = content_container.find_all("p")

        if not paragraphs:
            # Fallback: get all text
            return self._clean_text(content_container.get_text())

        # Join paragraphs
        content_parts = []
        for p in paragraphs:
            text = self._clean_text(p.get_text())
            if text and len(text) > 20:  # Skip very short paragraphs
                content_parts.append(text)

        return "\n\n".join(content_parts) if content_parts else None

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article author"""
        # Try article:author
        article_author = soup.find("meta", property="article:author")
        if article_author and article_author.get("content"):
            return self._clean_text(article_author["content"])

        # Try meta author
        meta_author = soup.find("meta", attrs={"name": "author"})
        if meta_author and meta_author.get("content"):
            return self._clean_text(meta_author["content"])

        return None

    def _extract_thumbnail(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article thumbnail/image"""
        # Try Open Graph image
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            return og_image["content"]

        # Try Twitter image
        twitter_image = soup.find("meta", attrs={"name": "twitter:image"})
        if twitter_image and twitter_image.get("content"):
            return twitter_image["content"]

        return None

    def scrape_article(self, url: str, category: str = None) -> Optional[Dict]:
        """
        Scrape full article content and metadata from URL

        Args:
            url: Article URL
            category: Optional category override

        Returns:
            Dict with article data or None if failed
        """
        logger.info(f"Scraping article: {url}")

        # Fetch HTML
        html = self._fetch_html(url)
        if not html:
            logger.error(f"Failed to fetch HTML from: {url}")
            return None

        # Parse HTML
        soup = BeautifulSoup(html, "html.parser")

        # Extract metadata
        title = self._extract_title(soup)
        description = self._extract_description(soup)
        published_date = self._extract_published_date(soup)
        extracted_category = self._extract_category(soup, url)
        content = self._extract_content(soup)
        author = self._extract_author(soup)
        thumbnail = self._extract_thumbnail(soup)

        # Use provided category or extracted category
        final_category = category or extracted_category

        # Build result
        article_data = {
            "title": title,
            "description": description,
            "content": content,
            "category": final_category,
            "published_date": published_date,
            "scraped_at": datetime.now().isoformat(),
        }

        # Log extraction results
        logger.info(f"✅ Scraped article: {title}")
        logger.info(f"   Content length: {len(content) if content else 0} chars")
        logger.info(f"   Category: {final_category}")
        logger.info(f"   Published: {published_date}")

        return article_data

    def scrape_multiple_articles(
        self,
        articles: List[Dict],
        url_field: str = "link",
        category_field: str = "category",
    ) -> List[Dict]:
        """
        Scrape multiple articles

        Args:
            articles: List of article dicts with URLs
            url_field: Field name for URL
            category_field: Field name for category

        Returns:
            List of scraped article data
        """
        logger.info(f"🔄 Scraping {len(articles)} articles...")

        scraped_articles = []

        for idx, article in enumerate(articles):
            url = article.get(url_field)
            category = article.get(category_field)

            if not url:
                logger.warning(f"[{idx+1}/{len(articles)}] No URL found, skipping")
                continue

            logger.info(f"\n[{idx+1}/{len(articles)}] Scraping: {url}")

            try:
                article_data = self.scrape_article(url, category)

                if article_data:
                    # Merge with original article data
                    merged_data = {**article, **article_data}
                    scraped_articles.append(merged_data)
                    logger.info(f"   ✅ Success")
                else:
                    logger.warning(f"   ⚠️  Failed to scrape")

            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                continue

        logger.info(f"\n✅ Scraped {len(scraped_articles)}/{len(articles)} articles")

        return scraped_articles


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def scrape_article_content(
    url: str, category: str = None, timeout: int = 30, max_retries: int = 3
) -> Optional[Dict]:
    """
    Convenience function to scrape a single article

    Args:
        url: Article URL
        category: Optional category
        timeout: Request timeout
        max_retries: Max retries

    Returns:
        Article data dict or None

    Example:
        >>> article = scrape_article_content('https://example.com/news/article-1')
        >>> print(article['title'])
        >>> print(article['content'])
    """
    scraper = ArticleContentScraper(timeout=timeout, max_retries=max_retries)
    return scraper.scrape_article(url, category)


def scrape_multiple_article_contents(
    articles: List[Dict],
    url_field: str = "link",
    category_field: str = "category",
    timeout: int = 30,
    max_retries: int = 3,
) -> List[Dict]:
    """
    Convenience function to scrape multiple articles (Sequential)

    Args:
        articles: List of article dicts
        url_field: Field name for URL
        category_field: Field name for category
        timeout: Request timeout
        max_retries: Max retries

    Returns:
        List of scraped article data

    Example:
        >>> articles = [
        ...     {'link': 'https://example.com/news/1', 'category': 'Tech'},
        ...     {'link': 'https://example.com/news/2', 'category': 'Sports'},
        ... ]
        >>> scraped = scrape_multiple_article_contents(articles)
        >>> print(f"Scraped {len(scraped)} articles")
    """
    scraper = ArticleContentScraper(timeout=timeout, max_retries=max_retries)
    return scraper.scrape_multiple_articles(articles, url_field, category_field)


def scrape_multiple_article_contents_parallel(
    articles: List[Dict],
    url_field: str = "link",
    category_field: str = "category",
    timeout: int = 30,
    max_retries: int = 3,
    max_workers: int = 5,
) -> List[Dict]:
    """
    Convenience function to scrape multiple articles (Parallel)

    Uses ThreadPoolExecutor for concurrent scraping

    Args:
        articles: List of article dicts
        url_field: Field name for URL
        category_field: Field name for category
        timeout: Request timeout
        max_retries: Max retries
        max_workers: Number of parallel workers

    Returns:
        List of scraped article data

    Example:
        >>> articles = [
        ...     {'link': 'https://example.com/news/1', 'category': 'Tech'},
        ...     {'link': 'https://example.com/news/2', 'category': 'Sports'},
        ... ]
        >>> scraped = scrape_multiple_article_contents_parallel(
        ...     articles, max_workers=5
        ... )
        >>> print(f"Scraped {len(scraped)} articles")
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    scraper = ArticleContentScraper(timeout=timeout, max_retries=max_retries)

    logger.info(
        f"🔄 Scraping {len(articles)} articles in parallel (max_workers={max_workers})..."
    )

    scraped_articles = []
    failed_count = 0

    def scrape_single(article):
        """Helper to scrape single article"""
        try:
            url = article.get(url_field)
            category = article.get(category_field)

            if not url:
                return None

            # Scrape article
            article_data = scraper.scrape_article(url, category)

            if article_data:
                # Merge with original data
                return {**article, **article_data}

            return None

        except Exception as e:
            logger.error(f"Error scraping article: {e}")
            return None

    # Parallel scraping
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_article = {
            executor.submit(scrape_single, article): article for article in articles
        }

        # Process completed tasks
        for idx, future in enumerate(as_completed(future_to_article)):
            try:
                result = future.result()

                if result:
                    scraped_articles.append(result)
                    logger.info(
                        f"[{idx+1}/{len(articles)}] ✅ Scraped: {result.get('title', 'Unknown')[:50]}"
                    )
                else:
                    failed_count += 1
                    logger.warning(f"[{idx+1}/{len(articles)}] ⚠️  Failed")

            except Exception as e:
                failed_count += 1
                logger.error(f"[{idx+1}/{len(articles)}] ❌ Error: {e}")

    logger.info(f"\n✅ Parallel scraping complete:")
    logger.info(f"   Total: {len(articles)}")
    logger.info(f"   Scraped: {len(scraped_articles)}")
    logger.info(f"   Failed: {failed_count}")

    return scraped_articles


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Example: Scrape single article
    print("\n" + "=" * 80)
    print("Example 1: Scrape single article")
    print("=" * 80)

    url = "https://vnexpress.net/nhung-chinh-sach-cong-nghe-tac-dong-toi-nguoi-dan-nam-2026-5039525.html"
    article = scrape_article_content(url, category="Science")

    if article:
        print(f"\n✅ Article scraped successfully!")
        print(f"Title: {article['title']}")
        print(f"Category: {article['category']}")
        print(f"Published: {article['published_date']}")
        print(
            f"Content length: {len(article['content']) if article['content'] else 0} chars"
        )
        print(
            f"Description: {article['description'][:100] if article['description'] else 'N/A'}..."
        )
