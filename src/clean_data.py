# clean_data.py
import gzip
import json
import pandas as pd
import os
import ast


# ---------------------------
# LOAD AUTHORS MAP
# ---------------------------
def load_authors(file_path):
    '''
    reads authors file and creates a mapping of author_id -> author_name
    why: because dataset stores IDs
    '''
    author_map = {}

    #open compressed file in text mode 
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line) #convert JSON string to python dict
            author_map[obj["author_id"]] = obj.get("name", "") #store mapping ID -> name

    return author_map


# ---------------------------
# LOAD BOOKS (with limit)
# ---------------------------
def load_books(file_path, limit=1000):
    '''
    Load book metadata from compressed JSON file, with a limit
    Each line in JSON file is a book JSON object
    '''
    books = []

    #open compressed file in text mode 
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            books.append(json.loads(line)) #parse JSON line into dict

    #convert list of dicts into pandas DataFrame
    return pd.DataFrame(books)


# ---------------------------
# CLEAN + TRANSFORM BOOK DATA
# ---------------------------
def clean_books(df, author_map):

    def resolve(authors):
        '''
        Convert list of author dictionaries into readable author names using the author_map
        For example: [{"author_id": 123"}, {author_id": 456}] -> }] -> ["J.K. Rowling", "Stephen King"]'''

        #some rows may not be lists
        if not isinstance(authors, list):
            return []

        names = []
        for a in authors:
            aid = a.get("author_id") #extract author ID
            names.append(author_map.get(aid, "unknown")) #convert ID -> name using map,  default to "unknown" if ID not found
        return names

    #dont modify original dataframe
    df = df.copy()

    df["authors"] = df["authors"].apply(resolve)

    df["description"] = df["description"].fillna("").astype(str)

    df["average_rating"] = pd.to_numeric(df["average_rating"], errors="coerce").fillna(0.0)

    
    if "genres" in df.columns:
        df["genres"] = df["genres"].fillna([])

    #relevant fields for elasticsearch index
    df = df[[
        "book_id",
        "title",
        "authors",
        "description",
        "average_rating"
    ]]

    return df

def load_reviews_for_books(file_path, valid_book_ids, limit=100000):
    """
    Load ONLY reviews that match selected books
    """

    reviews = []
    valid_book_ids = set(map(str, valid_book_ids))

    #open compressed file in text mode
    with gzip.open(file_path, "rt", encoding="utf-8") as f:

        #read all reviews (millions.... but we will filter by book_id)
        for i, line in enumerate(f):

            if i >= limit:
                break
            
            #parse review JSON line into dict
            obj = json.loads(line)

            book_id = obj.get("book_id")
            if book_id is None:
                continue

            #if review's book_id is in our valid set, keep it
            if str(book_id) in valid_book_ids:
                reviews.append({
                    "book_id": str(book_id),
                    "user_id": obj.get("user_id"),
                    "rating": obj.get("rating"),
                    "review_text": obj.get("review_text", "")
                })

    return pd.DataFrame(reviews)

# ---------------------------
# MAIN FUNCTION
# ---------------------------
if __name__ == "__main__":
    #paths to compressed JSON files
    books_path = "../data/goodreads_books.json.gz"
    authors_path = "../data/goodreads_book_authors.json.gz"
    reviews_path = "../data/goodreads_reviews_dedup.json.gz"

    print("Loading authors...")
    author_map = load_authors(authors_path)

    print("Loading books...")
    df_books = load_books(books_path, limit=1000)

    #downloaded books (1000 of full dataset)
    valid_book_ids = set(df_books["book_id"].astype(str))

    #scan for reviews that match the subset of books
    print("Loading filtered reviews...")
    df_reviews = load_reviews_for_books(
        reviews_path,
        valid_book_ids,
        limit=500000  # large scan but filtered output
    )

    print("\n=== ONE RANDOM REVIEW ===")
    print(df_reviews.sample(1))

    # keep relevant columns for Elasticsearch indexing
    print("Cleaning...")
    df_clean = clean_books(df_books, author_map)

    print(df_clean.head())

    df_clean.to_csv("../data/goodreads_clean.csv", index=False)

    print("Saved cleaned dataset.")