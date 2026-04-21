import pandas as pd
from elasticsearch import Elasticsearch, helpers
import ast


def main():
    print("Connecting to Elasticsearch...")
    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", "YwGNRfez")
    )

    index_name = "books"
    reviews_index_name = "reviews"

    mapping = {
        "mappings": {
            "properties": {
                "book_id":        {"type": "keyword"},
                "title":          {"type": "text"},
                "authors":        {"type": "keyword"},
                "description":    {"type": "text"},
                "average_rating": {"type": "float"}
            }
        }
    }

    reviews_mapping = {
        "mappings": {
            "properties": {
                "book_id":     {"type": "keyword"},
                "user_id":     {"type": "keyword"},
                "rating":      {"type": "integer"},
                "review_text": {"type": "text"}
            }
        }
    }

    # delete old index if exists
    if es.indices.exists(index=index_name):
        print(f"Deleting existing index '{index_name}'...")
        es.indices.delete(index=index_name)

    print(f"Creating index '{index_name}'...")
    es.indices.create(index=index_name, body=mapping)

    if es.indices.exists(index=reviews_index_name):
        print(f"Deleting existing index '{reviews_index_name}'...")
        es.indices.delete(index=reviews_index_name)

    print(f"Creating index '{reviews_index_name}'...")
    es.indices.create(index=reviews_index_name, body=reviews_mapping)

    csv_path = "../data/goodreads_clean.csv"
    print(f"Loading books data from {csv_path}...")
    df = pd.read_csv(csv_path)

    reviews_csv_path = "../data/goodreads_reviews_clean.csv"
    print(f"Loading reviews data from {reviews_csv_path}...")
    df_reviews = pd.read_csv(reviews_csv_path)

    def generate_book_data(dataframe):
        for _, row in dataframe.iterrows():
            # Parse authors back from string to list (CSV flattens lists to strings)
            authors = row["authors"]
            if isinstance(authors, str):
                try:
                    authors = ast.literal_eval(authors)
                except (ValueError, SyntaxError):
                    authors = [authors]
 
            yield {
                "_index": index_name,
                "_source": {
                    "book_id":        str(row["book_id"]),
                    "title":          str(row["title"]) if pd.notnull(row["title"]) else "",
                    "authors":        authors,
                    "description":    str(row["description"]) if pd.notnull(row["description"]) else "",
                    "average_rating": float(row["average_rating"]) if pd.notnull(row["average_rating"]) else 0.0
                }
            }

    def generate_review_data(dataframe):
        for _, row in dataframe.iterrows():
            yield {
                "_index": reviews_index_name,
                "_source": {
                    "book_id":     str(row["book_id"]),
                    "user_id":     str(row["user_id"]),
                    "rating":      int(row["rating"]) if pd.notnull(row["rating"]) else 0,
                    "review_text": str(row["review_text"]) if pd.notnull(row["review_text"]) else ""
                }
            }

    print("Sending books data to Elasticsearch...")
    success, failed = helpers.bulk(es, generate_book_data(df))

    print("Sending reviews data to Elasticsearch...")
    success_rev, failed_rev = helpers.bulk(es, generate_review_data(df_reviews))

    print(f"Done!")
    print(f"Books Indexed: {success}")
    print(f"Books Failed: {len(failed) if failed else 0}")
    print(f"Reviews Indexed: {success_rev}")
    print(f"Reviews Failed: {len(failed_rev) if failed_rev else 0}")


if __name__ == "__main__":
    main()