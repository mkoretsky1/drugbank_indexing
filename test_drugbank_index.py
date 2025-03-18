import os
from handlers.elastic import ElasticSearch

if __name__ == '__main__':
    # initialize ES handler
    index_name = 'drugbank-no-tags'
    es = ElasticSearch(index_name=index_name)

    response = es.query_index('conjug consist of human')
    resp = response['hits']['hits']

    for hit in resp:
        print(hit['_id'])
        # print(hit['_source']['content'])