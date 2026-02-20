import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.ERROR)

# Add plugins to path to import vectorizer
sys.path.append("/opt/airflow/plugins")

try:
    from processors.vectorizer import create_vectorizer
except ImportError as e:
    print(f"Error importing vectorizer: {e}")
    sys.exit(1)


def search_articles(query, limit=5):
    print(f"🔍 Connecting to Database...")
    print(f"   Query: '{query}'")

    try:
        # Connect to ChromaDB Service
        # We use explicit host/port for the script running inside connector
        vectorizer = create_vectorizer(
            model_type="sentence-transformers",
            model_name="all-MiniLM-L6-v2",
            chroma_host="chromadb",
            chroma_port=8000,
            collection_name="news_articles",
        )

        # Perform search
        results = vectorizer.search(query, n_results=limit)

        count = results.get("n_results", 0)
        print(f"\n✅ Found {count} results:")
        print("=" * 60)

        if count == 0:
            print("No matching articles found.")
            return

        docs = results.get("documents", [])
        metadatas = results.get("metadatas", [])
        distances = results.get("distances", [])

        for i in range(len(docs)):
            print(f"📄 Result #{i+1} (Score: {distances[i]:.4f})")
            if i < len(metadatas):
                meta = metadatas[i]
                print(f"   Title: {meta.get('title', 'Unknown')}")
                print(f"   Date:  {meta.get('published_date', 'Unknown')}")
                print(f"   Link:  {meta.get('link', 'Unknown')}")

            content = docs[i]
            # Truncate content for display
            display_content = content[:200].replace("\n", " ") + "..."
            print(f"   Content: {display_content}")
            print("-" * 60)

    except Exception as e:
        print(f"\n❌ Error during search: {e}")
        print("Make sure the pipeline has finished processing at least one batch.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('\nUsage: python query_db.py "<search_query>"')
        print('Example: python query_db.py "xem giá vàng hôm nay"')
        sys.exit(1)

    query = " ".join(sys.argv[1:])
    search_articles(query)
