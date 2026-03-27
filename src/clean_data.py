# clean_data.py

import pandas as pd
import os

def clean_goodreads_data(file_path="../data/books_sample_matched.csv"):
    """
    Loads the Goodreads sampled dataset from the data folder and filters out entries without titles.
    """
    df = pd.read_csv(file_path)

    # Keep only rows that have a non-empty title
    df_clean = df[df['title_x'].notna() & (df['title_x'] != '')]

    return df_clean

if __name__ == "__main__":
    df = clean_goodreads_data()
    os.makedirs("../data", exist_ok=True)
    df.to_csv("../data/goodreads_clean.csv", index=False)
    print("Loaded data shape:", df.shape)
    print(df.head())