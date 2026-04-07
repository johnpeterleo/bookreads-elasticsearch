import pandas as pd
from elasticsearch import Elasticsearch, helpers


def main():
    # 1. Connect to the local Elasticsearch container
    print("Connecting to Elasticsearch...")
    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", "YwGNRfez")
    )

    # 2. Define how the index (database table) should understand the data
    index_name = "books"
    mapping = {
        "mappings": {
            "properties": {
                "book_id": {"type": "keyword"},
                "title": {"type": "text"},
                "authors": {"type": "keyword"},
                "description": {"type": "text"},
                "genres": {"type": "keyword"},
                "average_rating": {"type": "float"}
            }
        }
    }

    if es.indices.exists(index=index_name):
        print(f"Deleting existing index '{index_name}'...")
        es.indices.delete(index=index_name)

    print(f"Creating index '{index_name}'...")
    es.indices.create(index=index_name, body=mapping)

    csv_path = "../data/goodreads_clean.csv"
    print(f"Loading first 1000 rows from {csv_path} for testing...")
    df = pd.read_csv(csv_path, nrows=1000)

    def generate_book_data(dataframe):
        for _, row in dataframe.iterrows():
            yield {
                "_index": index_name,
                "_source": {
                    "book_id": str(row.get('book_id', '')),
                    "title": str(row.get('title_x', '')),
                    "authors": str(row.get('authors_x', '')),
                    "description": str(row.get('description', '')),
                    "genres": str(row.get('genres_x', '')),
                    "average_rating": float(row.get('average_rating_x', 0.0)) if pd.notnull(row.get('average_rating_x')) else 0.0
                }
            }

    print("Sending data to Elasticsearch...")
    success, failed = helpers.bulk(es, generate_book_data(df))
    print(f"Indexing complete! Successfully indexed {success} documents.")


if __name__ == "__main__":
    main()
