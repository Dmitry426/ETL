import logging

from psycopg2 import OperationalError, DatabaseError

from postgres_loader import Load_data
from transform_data import Data_Merger
from etl_uploader import Upload_batch
from state_operator import State_operator
import backoff

class Film_work_etl_process:
    def __init__(self, config, postgres_connection):
        self.config = config.film_work_pg
        self.loader_process = Load_data(config=config)
        self.config = config.film_work_pg
        self.sql_query_film_work = self.config.sql_query_film_work_by_updated_at
        self.conn_postgres = postgres_connection
        self.state_field_name = self.config.film_work_state_field
        self.transform_data = Data_Merger()
        self.state = State_operator(config)
        self.logger = logging.getLogger('migrate_etl')

    @backoff.on_exception(backoff.expo, OperationalError ,max_time=60)
    def migrate_film_work(self):
        try:
            with self.conn_postgres.cursor() as cursor:
                for loaded in iter(lambda: self.loader_process.postgres_producer(
                        cursor=cursor, query=self.sql_query_film_work,
                        state_field_name=self.state_field_name), []):

                    parsed_data = self.transform_data.handle_merge_cases(
                        query_data=loaded)
                    es = Upload_batch(config=self.config)
                    es.es_push_butch(data=parsed_data)
                    self.state.validate_save_timestamp(state_field_name='film_work_updated_at',
                                                       timestamp=loaded[-1]['updated_at']
                                                       )
            self.conn_postgres.commit()
        except (OperationalError, DatabaseError)as e:
            self.logger.exception(e)
