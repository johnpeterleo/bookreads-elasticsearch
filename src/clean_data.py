# clean_data.py
import pandas as pd
import os

def clean_goodreads_data(file_path="books_sample_matched.csv"):
    """
    Loads the Goodreads dataset and filters out entries that are missing any essential information.
    Keeps all columns, but drops rows where essential fields are empty or NaN.
    """
    # Load the full dataset
    df = pd.read_csv(file_path)
    print(f"Number of books before cleaning: {df.shape}")

    # Essential columns to check for missing values
    essential_columns = [
        'title_x',         # book title
        'authors_x',       # authors info
        'description',     # text for full-text search
        'genres_x',        # genres for filtering
        'average_rating_x' # overall rating for ranking
    ]

    # Drop rows with NaN in any essential column
    df_clean = df.dropna(subset=essential_columns)

    # Drop rows where any essential column is empty string
    df_clean = df_clean[df_clean[essential_columns].apply(lambda row: all(str(x).strip() != '' for x in row), axis=1)]

    return df_clean

if __name__ == "__main__":
    os.makedirs("../data", exist_ok=True)
    df_clean = clean_goodreads_data(file_path="../data/books_sample_matched.csv")
    df_clean.to_csv("../data/goodreads_clean.csv", index=False)

    # Print summary info
    print(f"Cleaned data shape: {df_clean.shape}")
    print(f"Number of books: {len(df_clean)}")
    print("First 5 records:")
    print(df_clean.head())