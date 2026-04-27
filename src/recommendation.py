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

    def get_user_history(self, user_id):
        """Fetch user's read history and their likes.
        Args:
            user_id (str): User id
        Returns: 
            read_books (list): List of book ids.
            liked_books(list): List of liked book ids. 
        """
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
            read_books.append(book_id)
            if (book['_source']['rating'] >= 3):
                liked_books.append(book_id)
        return read_books, liked_books

    def get_books_from_similar_users(self, liked_books, read_books, user_id):
        """Fetch books from similar users. 

        Args:
            liked_books (list): Consists of book ids of the user's liked books.
            read_books (list): Consists of book ids of the user's read books.
            user_id (str): User id.

        Returns:
            candidate_books(list): List of book ids that were read by similar users.
            similar_users(list): List of user ids.
        """
        query = {
            "query": {
                "bool": {
                    "must": [
                        # get users that read similar books (liked books)
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
                    "terms": {"field": "user_id", "size": 50}
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
                    "terms": {"field": "book_id", "size": 500}
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
        """Fetch authors and descriptions for the books the user liked.

        Args:
            liked_books (list): list of book ids.

        Returns:
            authors(list): Ids of the authors. 
            descriptions(list): Descriptions of the books. 
        """
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
        """Fetch titles for a list of book IDs.
        Args: 
            book_ids (list): Book ids whose titles are to be fetched.  
        Returns:
            titles(list): List of titles. 
        """
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

    def recommend(self, query, user, limit=50, raw_results=False, boosts=None):
        """
        The recommendation engine. Combines similar user's tastes with the given query. 
        Args:
            query (str): The book the user is looking for. 
            user (str): User id
            limit (int): Number of max books to return
            raw_results (bool): Return the raw dictionary instead of printing
            boosts (dict): Optional custom boost values
        """

        # default boosts if not provided
        if boosts is None:
            boosts = {
                "title": 2.0,
                "authors": 2.0,
                "similar_users": 3.0,
                "description": 1.5
            }

        # so we can build up the master query
        must = []
        must_not = []
        should = []

        # so we know what they've read so far.
        read_books, liked_books = self.get_user_history(
            user_id=user)

        # get similar users and the books thety've read
        candidate_books, _ = self.get_books_from_similar_users(
            liked_books, read_books, user)

        # get liked authors and descriptions for weighting purposes
        liked_authors, liked_descriptions = self.get_liked_book_details(
            liked_books)

        if query:
            must.append({
                "multi_match": {
                    "query": query,
                    "fields": [f"title^{boosts['title']}", "description"]
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
                    "boost": boosts["authors"]
                }
            })

        # Boost if recommended by similar users
        if candidate_books:
            should.append({
                "terms": {
                    "book_id": candidate_books,
                    "boost": boosts["similar_users"]
                }
            })

        # Boost if the description is similar to their liked books (More Like This)
        if liked_descriptions:
            # Limit to top 10 to avoid massive text payload
            combined_desc = " ".join(liked_descriptions[:10])
            should.append({
                "more_like_this": {
                    "fields": ["description"],
                    "like": combined_desc,
                    "min_term_freq": 1,
                    "max_query_terms": 12,
                    "boost": boosts["description"]
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

        # Execute and Display
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
            print(f"{i}. {title}")
            print(
                f"   Author(s): {authors} | Avg Rating: {rating} | Match Score: {hit['_score']:.2f}")
            print(f"   {desc}")
            print("-" * 60)


if __name__ == '__main__':
    engine = Recommend()
    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", "YwGNRfez"))

    res = es.search(index="reviews", body={
        "query": {
            "bool": {
                "must": {"range": {
                    "rating": {
                        "gte": 1.0,
                        "lte": 5.0
                    }
                }}
            }
        },
        "aggs": {
            "users_with_reviews": {
                "terms": {
                    "field": "user_id",
                    "size": 10
                }
            }
        }
    })
    buckets = res.get("aggregations", {}).get(
        "users_with_reviews", {}).get("buckets", [])
    # print(buckets)
    user_id = "37b3e60b4e4152c580fd798d405150ff" # nerd user
    # user_id = "9003d274774f4c47e62f77600b08ac1d" # not nerd user
    read, liked = engine.get_user_history(user_id)
    print(engine.get_book_titles(liked))
    
    #engine.recommend(query="Love and the city", user=user_id)
