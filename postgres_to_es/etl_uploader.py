from elasticsearch import Elasticsearch
import  json
class  Upload_batch:
    def __init__(self):
        self.es = Elasticsearch("http://localhost:9200")
        self.request_body = None
    
    def create_index(self):
        with open('etl_index.json') as json_file:
            self.request_body = json.load(json_file)

    def push_index(self):
        existanse = self.es.search(
            index="movies",
            request_timeout=5 )
        print(existanse)
        self.create_index()
        self.es.indices.create(index='movies',body=self.request_body)

    def es_push_butch(self,data:list):
        pass
