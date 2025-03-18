import os
from handlers.elastic import ElasticSearch

def create_docs(data_dir):
    """Loop through data dir and create docs to upload to Elastic."""
    docs = []

    for drug in os.listdir(data_dir):
        drug_path = os.path.join(data_dir, drug)
        if drug_path.endswith('.xml'):
            with open(drug_path, 'r') as f:
                text = f.read()
                f.close()
            drug_id = drug.split('.x')[0]
        docs.append({'id':drug_id, 'content':text})

    return docs

if __name__ == '__main__':
    # set some directory info
    data_dir = f'drugbank/clean'

    # initialize ES handler
    index_name = 'drugbank-no-tags-stemmed'
    es = ElasticSearch(index_name=index_name)

    # index specifications
    settings = {
        "analysis":{
            "analyzer":{
                "html_analyzer":{
                    "type":"custom",
                    "tokenizer":"standard",
                    "filter":["lowercase","porter_stem"],
                    "char_filter":["strip_tags"]
                }
            },
            "char_filter":{
                "strip_tags":{
                    "type": "pattern_replace",
                    "pattern":"<[^>]*>",
                    "replacement": ""
                }
            }
        }
    }

    # settings = {
    #     "analysis":{
    #         "analyzer":{
    #             "html_analyzer":{
    #                 "type":"custom",
    #                 "tokenizer":"standard",
    #                 "filter":["lowercase","porter_stem"]
    #             }
    #         }
    #     }
    # }
    
    mappings = {
        "properties":{
            "content":{
                "type":"text",
                "analyzer":"html_analyzer"
            }
        }
    }

    # create index
    es.create_index(index_mappings=mappings, index_settings=settings)

    # create docs
    docs = create_docs(data_dir=data_dir)
    print(len(docs))

    # upload docs
    es.upload_docs(documents=docs)