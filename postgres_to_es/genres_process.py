import logging

from psycopg2 import OperationalError,DatabaseError

from postgres_loader import Load_data
from transform_data import Data_Merger
from etl_uploader import Upload_batch
from state_operator import State_operator

class Genres_etl_process:
    def __init__(self, config, postgres_connection):
        self.config = config.film_work_pg
        self.loader_process = Load_data(config=config)
        self.config = config.film_work_pg
        self.sql_query_genres = self.config.sql_query_genres
        self.sql_query_genre_film_work = self.config.sql_query_genre_film_work
        self.sql_query_film_work_by_id = self.config.sql_query_film_work_by_id
        self.conn_postgres = postgres_connection
        self.state_field_name = 'genres_updated_at'
        self.transform_data = Data_Merger()
        self.state = State_operator(config)
        self.logger = logging.getLogger('migrate_etl')

    def migrate_film_work(self):
        try:
            with self.conn_postgres.cursor() as cursor:
                for loaded in iter(lambda: self.loader_process.postgres_producer(
                                                                cursor=cursor, query=self.sql_query_genres,
                                                                state_field_name=self.state_field_name
                                                                ), []):
                    person_ids = (res['id'] for res in loaded)
                    fw_ids=  self.loader_process.postgres_enricher(
                                                                    cursor=cursor, ids=person_ids,
                                                                    query=self.sql_query_genre_film_work
                                                                  )
                    merged_film_work= self.loader_process.postgres_merger(
                                                                      cursor=cursor,ids=fw_ids,
                                                                      query=self.sql_query_film_work_by_id
                                                                      )
                    parsed_data = self.transform_data.handle_merge_cases(query_data=merged_film_work)
                    es = Upload_batch()
                    es.es_push_butch(data=parsed_data)
                    self.state.validate_save_timestamp(
                                                        state_field_name=self.state_field_name,
                                                        timestamp=loaded[-1]['updated_at']
                                                       )

        except (OperationalError, DatabaseError) as e:
            self.logger.exception(e)