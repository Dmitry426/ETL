import logging

import backoff
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from psycopg2 import OperationalError

from validation_classes import Config, DSNSettings
from film_work_process import Film_work_etl_process
from persons_process import Persons_etl_process
from genres_process import Genres_etl_process

config = Config.parse_file('config.json')
load_dotenv()
dsl = DSNSettings().dict()


def load_loger():
    logging.basicConfig(filename='es.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')


@backoff.on_exception(backoff.expo, OperationalError, max_time=60)
def migrate_to_etl():
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as connection:
        film_work_to_es = Film_work_etl_process(
            config=config, postgres_connection=connection)
        film_work_to_es.migrate_film_work()
        film_work_to_es_by_person = Persons_etl_process(
            config=config, postgres_connection=connection)
        film_work_to_es_by_person.migrate_film_work()
        film_work_to_es_by_genre = Genres_etl_process(
            config=config, postgres_connection=connection)
        film_work_to_es_by_genre.migrate_film_work()


if __name__ == '__main__':
    load_loger()
    try:
        migrate_to_etl()
    except psycopg2.OperationalError as e:
        logging.error(e)
