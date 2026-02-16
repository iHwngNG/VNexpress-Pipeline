"""
RSS Data Processor - PyArrow Version
Xử lý và làm sạch dữ liệu RSS feeds sử dụng PyArrow cho performance cao
"""

import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd
import logging
from typing import Optional, List, Dict, Union

logger = logging.getLogger(__name__)


class RSSDataProcessor:
    """
    Processor để xử lý và validate RSS feed data sử dụng PyArrow
    """

    def __init__(self):
        """Initialize RSS Data Processor"""
        self.required_fields = ["category", "rss_url"]
        self.optional_fields = []

    def _to_arrow_table(
        self, data: Union[pa.Table, pd.DataFrame, List[Dict]]
    ) -> pa.Table:
        """
        Convert input data to PyArrow Table

        Args:
            data: Input data (PyArrow Table, DataFrame, or list of dicts)

        Returns:
            PyArrow Table
        """
        if isinstance(data, pa.Table):
            return data
        elif isinstance(data, pd.DataFrame):
            return pa.Table.from_pandas(data)
        elif isinstance(data, list):
            return pa.Table.from_pylist(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

    def remove_duplicates(
        self, table: pa.Table, subset: Optional[List[str]] = None, keep: str = "first"
    ) -> pa.Table:
        """
        Loại bỏ duplicates, chỉ giữ lại 1 record duy nhất

        Args:
            table: Input PyArrow Table
            subset: Columns để check duplicate (default: ['rss_url'])
            keep: 'first' hoặc 'last'

        Returns:
            PyArrow Table đã loại bỏ duplicates
        """
        if subset is None:
            subset = ["rss_url"]

        original_count = len(table)

        # Convert to pandas để dùng drop_duplicates (PyArrow chưa có built-in)
        df = table.to_pandas()

        # Check duplicates
        duplicates = df[df.duplicated(subset=subset, keep=False)]
        if len(duplicates) > 0:
            logger.warning(f"⚠️  Found {len(duplicates)} duplicate records")
            logger.info(f"   Duplicate URLs: {duplicates['rss_url'].tolist()}")

        # Remove duplicates
        df_clean = df.drop_duplicates(subset=subset, keep=keep)

        removed_count = original_count - len(df_clean)
        if removed_count > 0:
            logger.info(f"🗑️  Removed {removed_count} duplicate(s)")
            logger.info(f"   Kept: {keep}")

        # Convert back to PyArrow
        return pa.Table.from_pandas(df_clean.reset_index(drop=True))

    def handle_missing_fields(
        self, table: pa.Table, required_url_field: str = "rss_url"
    ) -> pa.Table:
        """
        Xử lý missing fields:
        - Nếu thiếu field thường → set None
        - Nếu thiếu URL field → xóa record đó

        Args:
            table: Input PyArrow Table
            required_url_field: Field name cho URL (bắt buộc phải có)

        Returns:
            PyArrow Table đã xử lý missing fields
        """
        original_count = len(table)

        # 1. Check và add missing columns (set None)
        schema = table.schema
        existing_fields = schema.names

        for field in self.required_fields:
            if field not in existing_fields:
                logger.warning(f"⚠️  Missing column '{field}', adding with None values")
                # Add column with None values
                null_array = pa.array([None] * len(table), type=pa.string())
                table = table.append_column(field, null_array)

        # 2. Log null counts cho các fields (except URL)
        for field in self.required_fields:
            if field != required_url_field and field in table.column_names:
                null_count = pc.sum(pc.is_null(table[field])).as_py()
                if null_count > 0:
                    logger.info(
                        f"ℹ️  Field '{field}': {null_count} null values → keeping as None"
                    )

        # 3. Remove records với missing URL
        if required_url_field in table.column_names:
            url_column = table[required_url_field]

            # Filter: keep only non-null URLs
            is_valid = pc.invert(pc.is_null(url_column))

            # Also filter empty strings
            is_not_empty = pc.not_equal(
                pc.utf8_trim(pc.cast(url_column, pa.string())), ""
            )

            # Combine filters
            mask = pc.and_(is_valid, is_not_empty)

            null_count = original_count - pc.sum(mask).as_py()
            if null_count > 0:
                logger.warning(f"⚠️  Found {null_count} records with missing/empty URL")
                logger.info(f"🗑️  Removing records with missing/empty URL...")
                table = table.filter(mask)

        removed_count = original_count - len(table)
        if removed_count > 0:
            logger.info(f"✅ Removed {removed_count} record(s) due to missing URL")

        return table

    def validate_url_format(
        self, table: pa.Table, url_field: str = "rss_url", remove_invalid: bool = True
    ) -> pa.Table:
        """
        Validate URL format sử dụng PyArrow compute

        Args:
            table: Input PyArrow Table
            url_field: Field name cho URL
            remove_invalid: Nếu True, xóa invalid URLs

        Returns:
            PyArrow Table với valid URLs
        """
        if url_field not in table.column_names:
            logger.error(f"❌ URL field '{url_field}' not found in Table")
            return table

        url_column = table[url_field]

        # Check URL format (must start with http:// or https://)
        starts_with_http = pc.starts_with(pc.cast(url_column, pa.string()), "http://")
        starts_with_https = pc.starts_with(pc.cast(url_column, pa.string()), "https://")

        valid_urls = pc.or_(starts_with_http, starts_with_https)
        invalid_count = len(table) - pc.sum(valid_urls).as_py()

        if invalid_count > 0:
            logger.warning(f"⚠️  Found {invalid_count} invalid URL(s)")

            # Get invalid URLs for logging
            invalid_mask = pc.invert(valid_urls)
            invalid_table = table.filter(invalid_mask)
            invalid_urls = invalid_table[url_field].to_pylist()
            logger.info(f"   Invalid URLs: {invalid_urls}")

            if remove_invalid:
                logger.info(f"🗑️  Removing invalid URLs...")
                table = table.filter(valid_urls)
                logger.info(f"✅ Removed {invalid_count} record(s) with invalid URL")

        return table

    def clean_text_fields(
        self, table: pa.Table, text_fields: Optional[List[str]] = None
    ) -> pa.Table:
        """
        Clean text fields: strip whitespace, remove extra spaces

        Args:
            table: Input PyArrow Table
            text_fields: List of fields to clean (default: ['category'])

        Returns:
            PyArrow Table với cleaned text
        """
        if text_fields is None:
            text_fields = ["category"]

        for field in text_fields:
            if field in table.column_names:
                # Get column
                column = table[field]

                # Cast to string and trim
                cleaned = pc.utf8_trim(pc.cast(column, pa.string()))

                # Replace multiple spaces with single space (using pandas for regex)
                # PyArrow doesn't have regex replace yet
                df_temp = pa.table({field: cleaned}).to_pandas()
                df_temp[field] = df_temp[field].str.replace(r"\s+", " ", regex=True)

                # Replace 'nan' string with None
                df_temp.loc[df_temp[field] == "nan", field] = None

                # Update table
                cleaned_array = pa.array(df_temp[field])

                # Replace column in table
                field_index = table.schema.get_field_index(field)
                table = table.set_column(field_index, field, cleaned_array)

                logger.info(f"✨ Cleaned text field: '{field}'")

        return table

    def process(
        self,
        data: Union[pa.Table, pd.DataFrame, List[Dict]],
        remove_duplicates: bool = True,
        validate_urls: bool = True,
        clean_text: bool = True,
    ) -> pa.Table:
        """
        Full processing pipeline với PyArrow

        Args:
            data: Input data (PyArrow Table, DataFrame, or list of dicts)
            remove_duplicates: Có loại bỏ duplicates không
            validate_urls: Có validate URL format không
            clean_text: Có clean text fields không

        Returns:
            Processed PyArrow Table
        """
        logger.info("=" * 80)
        logger.info("🔄 Starting RSS Data Processing (PyArrow)")
        logger.info("=" * 80)

        # Convert to PyArrow Table
        table = self._to_arrow_table(data)
        logger.info(f"📊 Input: {len(table)} records")

        # Step 1: Handle missing fields
        logger.info("\n[Step 1] Handling missing fields...")
        table = self.handle_missing_fields(table)
        logger.info(f"   Result: {len(table)} records")

        # Step 2: Validate URLs
        if validate_urls:
            logger.info("\n[Step 2] Validating URL format...")
            table = self.validate_url_format(table)
            logger.info(f"   Result: {len(table)} records")

        # Step 3: Remove duplicates
        if remove_duplicates:
            logger.info("\n[Step 3] Removing duplicates...")
            table = self.remove_duplicates(table)
            logger.info(f"   Result: {len(table)} records")

        # Step 4: Clean text fields
        if clean_text:
            logger.info("\n[Step 4] Cleaning text fields...")
            table = self.clean_text_fields(table)
            logger.info(f"   Result: {len(table)} records")

        logger.info("\n" + "=" * 80)
        logger.info(f"✅ Processing Complete: {len(table)} records")
        logger.info("=" * 80)

        return table


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def process_rss_data(
    data: Union[pa.Table, pd.DataFrame, List[Dict]],
    remove_duplicates: bool = True,
    validate_urls: bool = True,
    clean_text: bool = True,
    return_pandas: bool = False,
) -> Union[pa.Table, pd.DataFrame]:
    """
    Convenience function để process RSS data với PyArrow

    Args:
        data: Input data (PyArrow Table, DataFrame, or list of dicts)
        remove_duplicates: Loại bỏ duplicates
        validate_urls: Validate URL format
        clean_text: Clean text fields
        return_pandas: Nếu True, return pandas DataFrame; nếu False, return PyArrow Table

    Returns:
        Processed data (PyArrow Table hoặc pandas DataFrame)

    Example:
        >>> data = [
        ...     {"category": "News", "rss_url": "http://example.com/rss"},
        ...     {"category": "News", "rss_url": "http://example.com/rss"},  # duplicate
        ...     {"category": None, "rss_url": "http://example.com/rss2"},
        ...     {"category": "Tech", "rss_url": None},  # missing URL
        ... ]
        >>> table = process_rss_data(data)
        >>> df = process_rss_data(data, return_pandas=True)
    """
    processor = RSSDataProcessor()
    table = processor.process(
        data,
        remove_duplicates=remove_duplicates,
        validate_urls=validate_urls,
        clean_text=clean_text,
    )

    if return_pandas:
        return table.to_pandas()
    return table


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Example data
    data = [
        {
            "category": "Trang chủ ",
            "rss_url": "https://vnexpress.net/rss/tin-moi-nhat.rss",
        },
        {
            "category": "Trang chủ ",
            "rss_url": "https://vnexpress.net/rss/tin-moi-nhat.rss",
        },  # Duplicate
        {"category": "Thế giới", "rss_url": "https://vnexpress.net/rss/the-gioi.rss"},
        {
            "category": None,
            "rss_url": "https://vnexpress.net/rss/thoi-su.rss",
        },  # Missing category
        {"category": "Kinh doanh", "rss_url": None},  # Missing URL - will be removed
        {
            "category": "Giải trí",
            "rss_url": "invalid-url",
        },  # Invalid URL - will be removed
        {
            "category": "  Thể thao  ",
            "rss_url": "https://vnexpress.net/rss/the-thao.rss",
        },  # Extra spaces
    ]

    print("\n📊 Original Data:")
    print(f"Records: {len(data)}")
    for i, item in enumerate(data):
        print(f"  [{i}] {item}")

    # Process with PyArrow
    processor = RSSDataProcessor()
    table_clean = processor.process(data)

    print("\n✅ Cleaned PyArrow Table:")
    print(f"Shape: {table_clean.shape}")
    print(f"Schema: {table_clean.schema}")
    print("\nData:")
    print(table_clean.to_pandas())

    # Also test returning pandas
    df_clean = process_rss_data(data, return_pandas=True)
    print("\n✅ As pandas DataFrame:")
    print(df_clean)
