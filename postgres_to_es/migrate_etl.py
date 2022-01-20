import logging

import psycopg2
from psycopg2.extras import DictCursor

from validation_classes import Config
from film_work_process import Film_work_etl_process
from persons_process import Persons_etl_process
from genres_process import  Genres_etl_process
config = Config.parse_file('config.json')
dsl = dict(config.film_work_pg.dsl)


def load_loger():
    logging.basicConfig(filename='es.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')


def migrate_to_etl(connection):
    film_work_to_es = Film_work_etl_process(config=config, postgres_connection=connection)
    film_work_to_es.migrate_film_work()
    film_work_to_es_by_person = Persons_etl_process(config=config, postgres_connection=connection)
    film_work_to_es_by_person.migrate_film_work()
    film_work_to_es_by_genre = Genres_etl_process(config=config, postgres_connection=connection)
    film_work_to_es_by_genre.migrate_film_work()

if __name__ == '__main__':
    load_loger()
    try:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            migrate_to_etl(connection=pg_conn)
    except psycopg2.OperationalError as e:
        logging.error(e)
