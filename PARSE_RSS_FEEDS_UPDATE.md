# ✅ Task parse_rss_feeds - Extract Article Information

## 🎯 Objective

Refactor task để:
1. ✅ **Fetch RSS feeds** - Request từng RSS URL
2. ✅ **Parse XML content** - Sử dụng lxml để parse RSS
3. ✅ **Extract article info** - Lấy thông tin chi tiết của từng bài báo

## 🔄 Changes

### ❌ Before: `extract_rss_urls` - Chỉ extract URLs

```python
@task
def extract_rss_urls(metadata: dict) -> dict:
    # Load RSS URLs
    df = load_output(...)
    
    # Extract URLs only
    df_urls = df[["category", "rss_url"]].copy()
    
    # Save URLs
    save_output(data=df_urls)
    
    return {"url_count": len(df_urls)}
```

**Vấn đề:**
- ❌ Chỉ extract URLs, không fetch content
- ❌ Không parse RSS XML
- ❌ Không lấy thông tin bài báo

### ✅ After: `parse_rss_feeds` - Parse & Extract Articles

```python
@task
def parse_rss_feeds(metadata: dict) -> dict:
    from parsers.rss_feed_parser import RSSFeedParser
    
    # Load RSS URLs
    df = load_output(...)
    
    # Prepare feeds list
    feeds = [
        {"url": row["rss_url"], "category": row["category"]}
        for idx, row in df.iterrows()
    ]
    
    # Parse RSS feeds
    parser = RSSFeedParser(timeout=30, max_retries=3)
    articles = parser.parse_multiple_feeds(feeds)
    
    # Convert to DataFrame
    df_articles = pd.DataFrame(articles)
    
    # Save articles
    save_output(data=df_articles)
    
    return {"article_count": len(df_articles)}
```

**Ưu điểm:**
- ✅ Fetch RSS content từ URLs
- ✅ Parse XML với lxml
- ✅ Extract article information
- ✅ Handle errors gracefully

## 📊 Article Information Extracted

Mỗi article chứa:

| Field | Description | Example |
|-------|-------------|---------|
| `title` | Tiêu đề bài báo | "Việt Nam thắng 3-0..." |
| `link` | URL bài báo | "https://vnexpress.net/..." |
| `description` | Mô tả ngắn | "Đội tuyển Việt Nam..." |
| `published_date` | Ngày publish | "Mon, 16 Feb 2026..." |
| `author` | Tác giả | "Nguyễn Văn A" |
| `category` | Chủ đề | "Thể thao" |
| `guid` | Unique ID | "https://vnexpress.net/..." |
| `thumbnail` | Ảnh thumbnail | "https://i1-thethao.vnecdn.net/..." |

## 🔧 RSS Feed Parser

### Features:

**1. Fetch RSS Content**
```python
# HTTP request với retry logic
response = session.get(url, timeout=30)
xml_content = response.text
```

**2. Parse XML với lxml**
```python
# Parse XML
root = etree.fromstring(xml_content.encode('utf-8'))

# Find all items
items = root.xpath('//item')
```

**3. Extract Article Info**
```python
# Extract từng field
title = item.find('title').text
link = item.find('link').text
description = item.find('description').text
pubDate = item.find('pubDate').text
```

**4. Handle Multiple Feeds**
```python
# Parse multiple feeds
articles = parser.parse_multiple_feeds([
    {"url": "https://example.com/rss", "category": "News"},
    {"url": "https://example.com/tech.rss", "category": "Tech"}
])
```

## 📝 Example Flow

### Input (RSS URLs):
```
DataFrame (3 feeds):
     category                                    rss_url
0        News  https://vnexpress.net/rss/tin-moi-nhat.rss
1    Business  https://vnexpress.net/rss/kinh-doanh.rss
2       Tech  https://vnexpress.net/rss/so-hoa.rss
```

### Processing:
```
[1/3] Processing: News - https://vnexpress.net/rss/tin-moi-nhat.rss
   📡 Fetching RSS feed...
   ✅ Fetched successfully: 45,230 bytes
   📰 Found 20 articles
   ✅ Parsed 20 articles successfully
   ✅ Added 20 articles

[2/3] Processing: Business - https://vnexpress.net/rss/kinh-doanh.rss
   📡 Fetching RSS feed...
   ✅ Fetched successfully: 38,120 bytes
   📰 Found 15 articles
   ✅ Parsed 15 articles successfully
   ✅ Added 15 articles

[3/3] Processing: Tech - https://vnexpress.net/rss/so-hoa.rss
   📡 Fetching RSS feed...
   ✅ Fetched successfully: 42,890 bytes
   📰 Found 18 articles
   ✅ Parsed 18 articles successfully
   ✅ Added 18 articles

✅ Total articles parsed: 53
```

### Output (Articles):
```
DataFrame (53 articles):
                                    title  ... thumbnail
0   Việt Nam thắng 3-0 trước Thái Lan  ... https://...
1   Giá vàng tăng mạnh trong tuần qua  ... https://...
2   iPhone 16 ra mắt với nhiều tính năng mới  ... https://...
...

Columns: ['title', 'link', 'description', 'published_date', 
          'author', 'category', 'guid', 'thumbnail']
```

### Saved as Parquet:
```
File: parse_rss_feeds_2024-02-16.parquet
Size: ~15 KB
Format: Parquet (PyArrow Table)
Records: 53 articles
Schema: {
    title: string,
    link: string,
    description: string,
    published_date: string,
    author: string,
    category: string,
    guid: string,
    thumbnail: string
}
```

## 🛡️ Error Handling

### Level 1: Individual Feed Error
```python
# Nếu 1 feed fail → skip và continue
try:
    articles = parser.parse_rss_url(url, category)
except Exception as e:
    logger.error(f"Failed to parse feed: {e}")
    continue  # Skip to next feed
```

### Level 2: No Articles Found
```python
# Nếu không có articles → return empty result
if len(articles) == 0:
    return {
        "article_count": 0,
        "success": False
    }
```

### Level 3: Parser Error
```python
# Nếu parser fail → return error result
except Exception as e:
    logger.error(f"Failed to parse RSS feeds: {e}")
    return {
        "article_count": 0,
        "success": False,
        "error": str(e)
    }
```

## ⚡ Performance

### Retry Logic:
```python
# Automatic retry on failure
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)
```

### Timeout:
```python
# 30 seconds timeout per request
response = session.get(url, timeout=30)
```

### Parallel Processing (Future):
```python
# TODO: Add parallel processing for multiple feeds
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(parser.parse_rss_url, feed) for feed in feeds]
```

## 📊 Metadata

Updated metadata includes article count:

```python
{
    "task_id": "parse_rss_feeds",
    "run_id": "2024-02-16",
    "article_count": 53,
    "feed_count": 3,
    "storage_path": "/tmp/.../parse_rss_feeds_2024-02-16.parquet",
    "success": True
}
```

## 🚀 Next Stage Integration

Downstream tasks có thể process articles:

```python
@task
def process_articles(metadata: dict):
    # Load articles
    df = load_output(task_id="parse_rss_feeds", ...)
    
    # Process articles
    for idx, row in df.iterrows():
        title = row['title']
        link = row['link']
        category = row['category']
        
        # Extract full content, vectorize, etc.
        ...
```

## 📁 Files Created

1. **`plugins/parsers/rss_feed_parser.py`** - RSS parser với lxml
2. **`dags/news-dag.py`** - Updated task

## ✅ Benefits Summary

1. **Complete Article Info**
   - ✅ Title, link, description
   - ✅ Published date, author
   - ✅ Category, thumbnail

2. **Robust Parsing**
   - ✅ lxml for XML parsing
   - ✅ Retry logic
   - ✅ Error handling

3. **Scalable**
   - ✅ Handle multiple feeds
   - ✅ Efficient storage (parquet)
   - ✅ Ready for parallel processing

4. **Production Ready**
   - ✅ Graceful error handling
   - ✅ Detailed logging
   - ✅ Type-safe with PyArrow

---

**Status**: ✅ UPDATED
**Date**: 2026-02-16
**Task**: `extract_rss_urls` → `parse_rss_feeds`
**Result**: Full article extraction with lxml! 🎉
