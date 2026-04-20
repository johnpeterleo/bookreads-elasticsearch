from elasticsearch import Elasticsearch
"""
get user history, 
get the books that readers with similar tastes read,

"""


class Recommend:
    def __init__(self):
        self.es = Elasticsearch(
            "http://localhost:9200",
            basic_auth=("elastic", "YwGNRfez"))

    def get_user_history(self, user_id, holdout_book_id=None):
        query = {
            "query": {
                "term": {"user_id": user_id}
            },
            "size": 10000
        }
        res = self.es.search(index="reviews", body=query)  # returns a dict
        hits = res['hits']['hits']
        read_books = []
        liked_books = []
        for book in hits:
            book_id = book['_source']['book_id']
            if book_id == holdout_book_id:
                continue # Skip the holdout book
            read_books.append(book_id)
            if (book['_source']['rating'] >= 3):
                liked_books.append(book_id)
        return read_books, liked_books

    def get_books_from_similar_users(self, liked_books, read_books, user_id):
        query = {
            "query": {
                "bool": {
                    "must": [
                        # get users that read similar books
                        {"terms": {"book_id": liked_books}},
                    ],
                    "must_not": [
                        # don't want to double count
                        {"term": {"user_id": user_id}}
                    ]
                }
            },
            "aggs": {
                "similar_users": {
                    "terms": {"field": "user_id", "size": 15}
                }
            },
            "size": 0
        }
        res = self.es.search(index="reviews", body=query)
        buckets = res.get("aggregations", {}).get(
            "similar_users", {}).get("buckets", [])
        similar_users = [b["key"] for b in buckets]

        # now that we have similar users, we want the actual books they read
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"user_id": similar_users}}
                    ],
                    "must_not": [
                        # we don't want the user's read books
                        {"terms": {"book_id": read_books}},
                    ]
                }
            },
            "aggs": {
                "books_from_similar_users": {
                    "terms": {"field": "book_id", "size": 25}
                }
            },
            "size": 0
        }

        res_books = self.es.search(index="reviews", body=query)
        book_buckets = res_books.get("aggregations", {}).get(
            "books_from_similar_users", {}).get("buckets", [])
        candidate_books = [b["key"] for b in book_buckets]
        return candidate_books, similar_users

    def get_liked_book_details(self, liked_books):
        """Fetch authors and descriptions for the books the user liked."""
        if not liked_books:
            return [], []

        query = {
            "query": {
                "terms": {"book_id": liked_books}
            },
            "size": len(liked_books)
        }
        res = self.es.search(index="books", body=query)

        authors = set()
        descriptions = []
        for hit in res["hits"]["hits"]:
            source = hit["_source"]
            if source.get("authors"):
                for author in source["authors"]:
                    authors.add(author)
            if source.get("description"):
                descriptions.append(source["description"])

        return list(authors), descriptions

    def get_book_titles(self, book_ids):
        """Fetch titles for a list of book IDs."""
        if not book_ids:
            return {}

        query = {
            "query": {
                "terms": {"book_id": book_ids}
            },
            "size": len(book_ids),
            "_source": ["book_id", "title"]
        }
        res = self.es.search(index="books", body=query)

        titles = {}
        for hit in res["hits"]["hits"]:
            source = hit["_source"]
            titles[source["book_id"]] = source.get("title", "Unknown Title")
        
        return titles

    def recommend(self, query, user, limit=50, holdout_book_id=None, raw_results=False):
        """
        The recommendation engine. Combines similar user's tastes with the given query. 
        Args:
            query (str): The book the user is looking for. 
            user (str): User id
            limit (int): Number of max books to return
            holdout_book_id (str): A book ID to hold out of the history (for testing).
            raw_results (bool): Return the raw dictionary instead of printing
        """
        # so we can build up the master query
        must = []
        must_not = []
        should = []

        # so we know what they've read so far.
        read_books, liked_books = self.get_user_history(user_id=user, holdout_book_id=holdout_book_id)

        # get similar users and the books thety've read
        candidate_books, similar_users = self.get_books_from_similar_users(
            liked_books, read_books, user)

        # get liked authors and descriptions for weighting purposes
        liked_authors, liked_descriptions = self.get_liked_book_details(
            liked_books)

        must.append({
            "multi_match": {
                "query": query,
                # Title matches are worth 2x description matches
                "fields": ["title^2", "description"]
            }
        })

        # make sure we dont get books that they've read
        if read_books:
            must_not.append({"terms": {"book_id": read_books}})

        # Boost if written by an author they like
        if liked_authors:
            should.append({
                "terms": {
                    "authors": liked_authors,
                    "boost": 2.0
                }
            })

        # Boost if recommended by similar users
        if similar_users:
            should.append({
                "terms": {
                    "book_id": similar_users,
                    "boost": 3.0
                }
            })

        # Boost if the description is similar to their favorably rated books (More Like This)
        if liked_descriptions:
            # Limit to top 10 to avoid massive text payload
            combined_desc = " ".join(liked_descriptions[:10])
            should.append({
                "more_like_this": {
                    "fields": ["description"],
                    "like": combined_desc,
                    "min_term_freq": 1,
                    "max_query_terms": 12,
                    "boost": 1.5
                }
            })

        master_query = {
            "query": {
                "bool": {
                    "must": must,
                    "must_not": must_not,
                    "should": should
                }
            },
            "size": limit
        }

        #Execute and Display
        response = self.es.search(index="books", body=master_query)

        hits = response["hits"]["hits"]
        if raw_results:
            return hits

        if not hits:
            print("\nNo matching books found.\n")
            return

        print(f"\nTop {min(limit, len(hits))} Recommendations:\n")

        for i, hit in enumerate(hits, 1):
            source = hit["_source"]
            title = source.get("title", "Unknown Title")
            authors = ", ".join(source.get("authors", []))
            rating = source.get("average_rating", "N/A")
            desc = source.get("description", "")
            snippet = desc[:150] + "..." if len(desc) > 150 else desc

            print(f"{i}. {title}")
            print(
                f"   Author(s): {authors} | Avg Rating: {rating} | Match Score: {hit['_score']:.2f}")
            print(f"   {snippet}")
            print("-" * 60)


if __name__ == '__main__':
    engine = Recommend()
    user_id = "37b3e60b4e4152c580fd798d405150ff"
    read, liked = engine.get_user_history(user_id)

    read_titles = engine.get_book_titles(read)
    print(f"Read Books ({len(read)}):")
    for r in read[:5]:  # Just show top 5 for brevity
        print(f" - {read_titles.get(r, r)}")
    if len(read) > 5:
        print("   ...")

    liked_titles = engine.get_book_titles(liked)
    print(f"\nLiked Books ({len(liked)}):")
    for l in liked[:5]:
        print(f" - {liked_titles.get(l, l)}")
    if len(liked) > 5:
        print("   ...")

    #debug to be removed
    query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"book_id": liked}},
                    {"range": {"rating": {"gte": 4}}}
                ]
            }
        }
    }
    test = engine.es.search(index="reviews", body=query)
    print(
        f"Reviews matching liked books with rating >= 4: {test['hits']['total']['value']}")
    if test['hits']['hits']:
        print(f"Sample: {test['hits']['hits'][0]}")

    candidate, similar = engine.get_books_from_similar_users(
        liked_books=liked, read_books=read, user_id="37b3e60b4e4152c580fd798d405150ff")
    
    candidate_titles = engine.get_book_titles(candidate)
    print(f"\nSimilar users: {similar}")
    print("\nCandidate books from similar users:")
    for book_id in candidate:
        print(f" - {candidate_titles.get(book_id, book_id)}")
    print("\n" + "="*80)
    print("Running Full Recommendation Engine...")
    print("="*80)
    engine.recommend(query="romance drama", user="37b3e60b4e4152c580fd798d405150ff")
