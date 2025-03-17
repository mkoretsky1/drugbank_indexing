import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers, BadRequestError

class ElasticSearch:
    """A handler for managing Elasticsearch operations such as index creation, document uploading, and querying."""

    def __init__(self, index_name, es_url=None, es_api_key=None, index_mappings=None, index_settings=None):
        """Initializes an ElasticSearchHandler instance."""
        self.client = self.initialize_es_client(es_url, es_api_key)
        self.index_name = index_name

    @staticmethod
    def initialize_es_client(es_url=None, es_api_key=None):
        """Initializes the Elasticsearch client with the provided URL and API key or loads them from environment variables."""
        load_dotenv('configs/.env')

        if not es_url:
            es_url = os.getenv("ES_URL")
        if not es_api_key:
            es_api_key = os.getenv("ES_API_KEY")

        if not es_url or not es_api_key:
            raise ValueError("ElasticSearch URL and API key must be in configs/.env or provided.")
        
        es_client = Elasticsearch(
            es_url,
            api_key=es_api_key,
            request_timeout=30
        )
        return es_client
    
    def create_index(self, index_mappings=None, index_settings=None):
        """Creates an index in Elasticsearch with specified mappings and settings."""
        try:
            resp = self.client.indices.create(index=self.index_name, mappings=index_mappings, settings=index_settings)
            print(resp)
        except BadRequestError as e:
            print(f'Error Index already exists: {e}.')
    
    def upload_docs(self, documents, batch_size=1000):
        """Uploads a list of documents to the Elasticsearch index in batches."""
        for start in range(0, len(documents), batch_size):
            end = start+batch_size
            
            # build actions dir 
            actions = [
                {
                    '_index':self.index_name,
                    '_id':doc['id'],
                    '_source':{
                        'content':doc['content']
                    }
                }
                for doc in documents[start:end]
            ]

            # try to upload and catch any errors
            try:
                res = helpers.bulk(self.client, actions)
                print(res)
            except helpers.BulkIndexError as e:
                print(f"Bulk indexing error: {e}")
                for error in e.errors:
                    print(error)

    def query_index(self, query, search_field="content", top_k=5):
        """Queries the Elasticsearch index for documents that match a given query string."""
        response = self.client.search(
            index=self.index_name,
            body={
                "query": {
                    "match": {
                        search_field: query
                    }
                },
                "sort" : [
                    {"_score": "desc"},
                ],
                "size":top_k,
            }
        )
        return response