from elasticsearch import Elasticsearch

es = Elasticsearch("http://34.101.246.247:9200")

def index():

    index_name = "dwh-siloam"

    request_body = {
        "settings" :{
        "number_of_shards": 1, 
        "number_of_replicas": 0}
    }

    res = es.indices.exists(index=index_name)

    if res:
        data = {
            'index': 'index exists'
        }
    else:
        res = es.indices.create(index=index_name, body=request_body)

    return f'index {index_name} created'

index()