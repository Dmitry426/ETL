from elasticsearch import Elasticsearch , ElasticsearchException
from elasticsearch.helpers import  bulk
import  json
import logging

class  Upload_batch:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")
        self.request_body = None
    
    def create_index(self):
        with open('etl_index.json') as json_file:
            self.request_body = json.load(json_file)

    def push_index(self):
        if not self.es.indices.exists(index="movies"):
            try:
                self.create_index()
                self.es.indices.create(index='movies', body=self.request_body)
            except ElasticsearchException as es1:
                logging.error(es1)

    def generate_data(self,data:list):
        for item in data:
            yield {
                "_index": 'movies',
                "_id": item['id'],
                '_source':item
            }

    def es_push_butch(self,data:list):
        try:
            bulk(self.es , self.generate_data(data=data))
        except ElasticsearchException as es2:
            logging.error(es2)
