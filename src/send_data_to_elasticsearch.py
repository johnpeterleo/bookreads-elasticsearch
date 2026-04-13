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

    # delete old index if exists
    if es.indices.exists(index=index_name):
        print(f"Deleting existing index '{index_name}'...")
        es.indices.delete(index=index_name)

    print(f"Creating index '{index_name}'...")
    es.indices.create(index=index_name, body=mapping)

    csv_path = "../data/goodreads_clean.csv"
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)

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
                    "title":          row["title"],
                    "authors":        authors,
                    "description":    row["description"],
                    "average_rating": float(row["average_rating"]) if pd.notnull(row["average_rating"]) else 0.0
                }
            }


    print("Sending data to Elasticsearch...")
    success, failed = helpers.bulk(es, generate_book_data(df))

    print(f"Done!")
    print(f"Indexed: {success}")
    print(f"Failed: {len(failed) if failed else 0}")


if __name__ == "__main__":
    main()