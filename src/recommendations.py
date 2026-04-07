from elasticsearch import Elasticsearch

def connect_es():
    return Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", "YwGNRfez")
    )

def get_book_preferences(es, read_books):
    liked_genres = set()
    liked_authors = set()
    
    for book_title in read_books:
        # Search for the exact or best matching book by title
        response = es.search(
            index="books",
            body={
                "query": {
                    "match": {
                        "title": book_title
                    }
                },
                "size": 1
            }
        )
        
        hits = response['hits']['hits']
        if hits:
            book = hits[0]['_source']
            # We extract strings to build preference profile
            if book.get('genres'):
                liked_genres.add(str(book['genres']))
            if book.get('authors'):
                liked_authors.add(str(book['authors']))
            print(f"Found '{book.get('title')}' (Author(s): {book.get('authors')})")
        else:
            print(f"Could not find '{book_title}' in our database.")
            
    return list(liked_genres), list(liked_authors)

def get_recommendations(es, search_query, read_books, liked_genres, liked_authors):
    """
    Builds an Elasticsearch boolean query. Builds it from the ground up. 
    """
    # 1. Base query structure
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": search_query,
                            "fields": ["title^2", "description", "genres"], # title is weighted 2x heavier
                        }
                    }
                ],
                "must_not": [],
                "should": []
            }
        },
        "size": 5 # Return top 5 recommendations
    }
    
    # 2. Exclude previously read books
    for book in read_books:
        query_body["query"]["bool"]["must_not"].append({
            "match_phrase": {
                "title": book
            }
        })
    
    # 3. Add boosting for liked genres and authors
    if liked_genres:
        query_body["query"]["bool"]["should"].append({
            "match": {
                "genres": {
                    "query": " ".join(liked_genres),
                    "boost": 2.0  # Double points for matching liked genres
                }
            }
        })
        
    if liked_authors: 
        query_body["query"]["bool"]["should"].append({
            "match": {
                "authors": {
                    "query": " ".join(liked_authors),
                    "boost": 1.5  # 1.5x points for matching liked authors
                }
            }
        })

    # Execute search
    response = es.search(index="books", body=query_body)
    return response['hits']['hits']

def main():
    es = connect_es()
    
    print("\n" + "="*40)
    print(" Book Recommendation Engine ")
    print("="*40 + "\n")
    
    # 1. Get user history
    print("Tell me one or two books you've read and liked (comma-separated) [Press Enter to skip]:")
    read_input = input("> ").strip()
    read_books = [b.strip() for b in read_input.split(',')] if read_input else []
    
    # 2. Extract preferences
    liked_genres, liked_authors = get_book_preferences(es, read_books)
    
    # 3. Get new search query
    print("\nWhat kind of book are you looking for today? (e.g., 'adventure', 'dragons and trolls')")
    search_query = input("> ").strip()
    
    if not search_query:
        print("No query provided. Exiting.")
        return
        
    # 4. Fetch recommendations
    print(f"\nSearching for '{search_query}' and applying your preferences...\n")
    recommendations = get_recommendations(es, search_query, read_books, liked_genres, liked_authors)
    
    # 5. Display results
    if not recommendations:
        print("No matching books found! (Try indexing the full dataset if using the 1000-row subset)")
        return
        
    for i, hit in enumerate(recommendations, 1):
        book = hit['_source']
        score = hit['_score']
        # Truncate description for readability
        desc = book.get('description', '')
        desc_snippet = desc[:200] + "..." if len(desc) > 200 else desc
        
        print(f"{i}. {book.get('title')} (Relevance Score: {score:.2f})")
        print(f"   Author(s): {book.get('authors')} | Rating: {book.get('average_rating')}")
        print(f"   Description: {desc_snippet}\n")

if __name__ == "__main__":
    main()