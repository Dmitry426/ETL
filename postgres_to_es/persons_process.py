import logging
import backoff
from psycopg2 import OperationalError,DatabaseError

from postgres_loader import Load_data
from transform_data import Data_Merger
from etl_uploader import Upload_batch
from state_operator import State_operator

class Persons_etl_process:
    def __init__(self, config, postgres_connection):
        self.config = config.film_work_pg
        self.loader_process = Load_data(config=config)
        self.config = config.film_work_pg
        self.sql_query_persons = self.config.sql_query_persons
        self.sql_query_person_film_work = self.config.sql_query_person_film_work
        self.sql_query_film_work_by_id = self.config.sql_query_film_work_by_id
        self.conn_postgres = postgres_connection
        self.state_field_name = self.config.persons_state_field
        self.transform_data = Data_Merger()
        self.state = State_operator(config)
        self.logger = logging.getLogger('migrate_etl')

    @backoff.on_exception(backoff.expo, OperationalError,  max_time=60)
    def migrate_film_work(self):
        try:
            with self.conn_postgres.cursor() as cursor:
                for loaded in iter(lambda: self.loader_process.postgres_producer(
                                                                cursor=cursor, query=self.sql_query_persons,
                                                                state_field_name=self.state_field_name
                                                                ), []):
                    person_ids = (res['id'] for res in loaded)
                    fw_ids=  self.loader_process.postgres_enricher(
                                                                    cursor=cursor, ids=person_ids,
                                                                    query=self.sql_query_person_film_work
                                                                  )
                    merged_film_work= self.loader_process.postgres_merger(
                                                                      cursor=cursor,ids=fw_ids,
                                                                      query=self.sql_query_film_work_by_id
                                                                      )
                    parsed_data = self.transform_data.handle_merge_cases(query_data=merged_film_work)
                    es = Upload_batch(config=self.config)
                    es.es_push_butch(data=parsed_data)
                    self.state.validate_save_timestamp(
                                                        state_field_name=self.state_field_name,
                                                        timestamp=loaded[-1]['updated_at']
                                                       )
                self.conn_postgres.commit()
        except (OperationalError, DatabaseError) as e:
            self.logger.exception(e)