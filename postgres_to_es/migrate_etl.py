import psycopg2
from psycopg2.extras import DictCursor
from postgres_loader import  Load_data
from validation_classes import Config
from etl_uploader import Upload_batch

config = Config.parse_file('config.json')
dsl = dict(config.film_work_pg.dsl)
def migrate_to_etl(connection):
    postgres_connection = connection
    postgres_loader = Load_data(config=config, connection_postgres=postgres_connection)
    loaded = postgres_loader.load_from_postgres()
    es = Upload_batch()
    es.push_index()







with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    migrate_to_etl(connection=pg_conn)