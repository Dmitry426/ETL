from elasticsearch import Elasticsearch , ElasticsearchException
from elasticsearch.helpers import  bulk
import  json
import logging

class  Upload_batch:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")
        self.request_body = None
    
    def _create_index(self):
        with open('etl_index.json') as json_file:
            self.request_body = json.load(json_file)

    def _push_index(self):
        """Method to keep index automatically updated """
        if not self.es.indices.exists(index="movies"):
            try:
                self._create_index()
                self.es.indices.create(index='movies', body=self.request_body)
            except ElasticsearchException as es1:
                logging.error(es1)

    def _generate_data(self,data:list):
        for item in data:
            yield {
                "_index": 'movies',
                "_id": item['id'],
                '_source':item
            }
    def es_push_butch(self,data:list):
        self._push_index()
        try:
            bulk(self.es , self._generate_data(data=data))
            self.es.transport.close()
        except ElasticsearchException as es2:
            logging.error(es2)
