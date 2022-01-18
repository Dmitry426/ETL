import logging

import psycopg2
from psycopg2.extras import DictCursor

from postgres_loader import Load_data
from validation_classes import Config
from etl_uploader import Upload_batch
from state_operator import State_operator

config = Config.parse_file('config.json')
dsl = dict(config.film_work_pg.dsl)

def load_loger():
    logging.basicConfig(filename='es.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')

def migrate_to_etl(connection):
    state = State_operator(config)
    postgres_connection = connection
    postgres_loader = Load_data(config=config, connection_postgres=postgres_connection)
    for loaded in iter(lambda: postgres_loader.load_from_postgres(), []):
        parsed_data = postgres_loader.handle_merge_cases(query_data=loaded)
        es = Upload_batch()
        es.es_push_butch(data=parsed_data)
        state.validate_save_timestamp(loaded[-1]['updated_at'])


if __name__ == '__main__':
    load_loger()
    try:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            migrate_to_etl(connection=pg_conn)
    except psycopg2.OperationalError as e:
        logging.error(e)
