from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", basic_auth=("elastic", "YwGNRfez"))

# A search query looking for "wizard" in the description, sorted by rating
search_query = {
    "query": {
        "match": {
            "author": "J.D Salinger"
        }
    },
    "sort": [
        {"rating": {"order": "desc"}}
    ],
    "size": 3 # return top 3 results
}

response = es.search(index="books", body=search_query)
print(response)

print("Top results for 'wizard':\n")
for hit in response['hits']['hits']:
    book = hit['_source']
    print(f"Title: {book['title']} (Rating: {book['rating']})")
    print(f"Author: {book['authors']}")
    print("-" * 40)