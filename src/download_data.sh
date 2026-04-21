#!/bin/bash

set -e 
mkdir -p ../data

BASE="https://mcauleylab.ucsd.edu/public_datasets/gdrive/goodreads"


FILES=(
goodreads_books.json.gz
goodreads_book_authors.json.gz
goodreads_book_genres_initial.json.gz
goodreads_reviews_dedup.json.gz
)

# Download each file with retry logic
for f in "${FILES[@]}"; do
    echo "Downloading $f ..."

    curl -L \
         --retry 10 \
         --retry-delay 5 \
         -C - \
         "$BASE/$f" \
         -o "../data/$f"

    echo "$f done"
done

echo "ALL DONE"







##MORE COMPLETE DATASET (BUT LARGER) 
# FILES=(
# goodreads_books.json.gz
# goodreads_book_authors.json.gz
# goodreads_book_works.json.gz
# goodreads_book_series.json.gz
# goodreads_book_genres_initial.json.gz
# goodreads_reviews_dedup.json.gz
# goodreads_reviews_spoiler.json.gz
# goodreads_reviews_spoiler_raw.json.gz
# )