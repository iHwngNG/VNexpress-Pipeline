"""
VNExpress RSS Scraper Module
Crawl và parse danh sách RSS feeds từ vnexpress.net/rss
"""

import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import json
from datetime import datetime
from pathlib import Path
import logging
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RSSListScraper:
    """Scraper để lấy danh sách RSS feeds từ VNExpress"""

    def __init__(self, timeout: int = 30, max_retries: int = 3):
        """
        Args:
            timeout: Timeout cho HTTP request (seconds)
            max_retries: Số lần retry khi request fail
        """
        self.base_url = "https://vnexpress.net/rss"
        self.timeout = timeout
        self.max_retries = max_retries
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

    def fetch_rss_page(self) -> Optional[str]:
        """
        Fetch HTML content từ trang RSS list

        Returns:
            HTML content string hoặc None nếu fail
        """
        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Fetching RSS page (attempt {attempt + 1}/{self.max_retries})..."
                )

                with httpx.Client(
                    timeout=self.timeout, follow_redirects=True
                ) as client:
                    response = client.get(self.base_url, headers=self.headers)
                    response.raise_for_status()

                    logger.info(
                        f"Successfully fetched RSS page (status: {response.status_code})"
                    )
                    return response.text

            except httpx.TimeoutException:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                if attempt == self.max_retries - 1:
                    logger.error("Max retries reached. Failed to fetch RSS page.")
                    return None

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error {e.response.status_code}: {e}")
                return None

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return None

        return None

    def parse_rss_list(self, html_content: str) -> List[Dict[str, str]]:
        """
        Parse HTML content để extract category và RSS URLs

        Args:
            html_content: HTML string từ vnexpress.net/rss

        Returns:
            List of dict chứa category và rss_url
            [
                {"category": "Thời sự", "rss_url": "https://vnexpress.net/rss/thoi-su.rss"},
                ...
            ]
        """
        try:
            soup = BeautifulSoup(html_content, "lxml")  # lxml nhanh hơn html.parser
            rss_items = []

            # Lấy tất cả các link RSS
            rss_links = soup.find_all("a", href=re.compile(r"\.rss$"))

            for link in rss_links:
                href = link.get("href", "").strip()
                text = link.get_text().strip().replace("RSS", "")

                # Filter chỉ lấy các link .rss
                if href and ".rss" in href:
                    # Đảm bảo URL đầy đủ
                    if not href.startswith("http"):
                        href = f"https://vnexpress.net{href}"

                    rss_items.append({"category": text, "rss_url": href})
                    logger.debug(f"Found RSS: {text} -> {href}")

            logger.info(f"Successfully parsed {len(rss_items)} RSS feeds")
            return rss_items

        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return []

    def scrape_rss_list(self) -> List[Dict[str, str]]:
        """
        Main function: Fetch và parse RSS list

        Returns:
            List of RSS feeds
        """
        logger.info("Starting RSS list scraping...")

        # Step 1: Fetch HTML
        html_content = self.fetch_rss_page()
        if not html_content:
            logger.error("Failed to fetch RSS page")
            return []

        # Step 2: Parse HTML
        rss_list = self.parse_rss_list(html_content)

        logger.info(f"Scraping completed. Found {len(rss_list)} RSS feeds")
        return rss_list

    def save_to_json(
        self, rss_list: List[Dict[str, str]], output_path: str = None
    ) -> bool:
        """
        Lưu RSS list vào JSON file

        Args:
            rss_list: List of RSS feeds
            output_path: Path to output JSON file (default: src/rss_feeds.json)

        Returns:
            True nếu save thành công, False nếu fail
        """
        try:
            # Nếu không có output_path, dùng default path trong folder src
            if output_path is None:
                output_path = "src/rss_feeds.json"

            # Tạo thư mục nếu chưa tồn tại
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            # Chuẩn bị data với metadata
            output_data = {
                "metadata": {
                    "scraped_at": datetime.now().isoformat(),
                    "source_url": self.base_url,
                    "total_feeds": len(rss_list),
                },
                "feeds": rss_list,
            }

            # Save to JSON
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

            logger.info(f"Successfully saved RSS list to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return False


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def scrape_vnexpress_rss_list(
    timeout: int = 30, max_retries: int = 3
) -> List[Dict[str, str]]:
    """
    Convenience function to scrape VNExpress RSS list

    Args:
        timeout: Request timeout in seconds
        max_retries: Maximum number of retries

    Returns:
        List of RSS feeds with category and url

    Example:
        >>> rss_list = scrape_vnexpress_rss_list()
        >>> print(f"Found {len(rss_list)} RSS feeds")
        >>> for feed in rss_list:
        ...     print(f"{feed['category']}: {feed['rss_url']}")
    """
    scraper = RSSListScraper(timeout=timeout, max_retries=max_retries)
    return scraper.scrape_rss_list()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Example 1: Using utility function
    print("\n" + "=" * 80)
    print("Example 1: Using utility function")
    print("=" * 80)

    rss_list = scrape_vnexpress_rss_list()

    print(f"\n✅ Found {len(rss_list)} RSS feeds")
    for i, feed in enumerate(rss_list[:5]):
        print(f"[{i+1}] {feed['category']}: {feed['rss_url']}")

    # Example 2: Using class directly (if needed)
    print("\n" + "=" * 80)
    print("Example 2: Using class directly")
    print("=" * 80)

    scraper = RSSListScraper()
    rss_list = scraper.scrape_rss_list()

    # Save to JSON
    scraper.save_to_json(rss_list)
    print(f"\n✅ Saved {len(rss_list)} RSS feeds to src/rss_feeds.json")
