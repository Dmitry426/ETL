import psycopg2
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch

from validation_classes import Config
from data_merger import Data_Merger
from state_manager import State, JsonFileStorage



config = Config.parse_file('config.json')
dsl = dict(config.film_work_pg.dsl)
sql_query = config.film_work_pg.sql_query

file_path = config.film_work_pg.state_file_path
json_file_storage = JsonFileStorage(file_path=file_path)
state = State(json_file_storage)
updated_at = state.get_state(key='updated_at')
def  handle_no_date():
    limit = config.film_work_pg.limit
    if  updated_at is None:
        sql_query_params = f""" 
        ORDER BY updated_at
        LIMIT {limit};
    """
    if  updated_at :
        sql_query_params = f""" 
        WHERE updated_at > {updated_at}   
        ORDER BY updated_at
        LIMIT {limit};
    """
    return sql_query_params

sql_query = sql_query.format(handle_no_date())

client = Elasticsearch()
data_merger = Data_Merger()

with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    with pg_conn.cursor() as cursor:
        cursor.execute(sql_query)
        results = []
        for res in iter(lambda: cursor.fetchall(), []):
            film_id_counter = None
            for item in res:
                fw = dict(item)
                if film_id_counter is None:
                    data_merger.combine_tables(obj=fw)
                    film_id_counter = fw['film_id']
                if film_id_counter == fw['film_id']:
                    data_merger.combine_tables(obj=fw)
                if film_id_counter != fw['film_id']:
                    result = data_merger.validate_and_return()
                    results.append(result)
                    film_id_counter = fw['film_id']
                    data_merger.combine_tables(obj=fw)
                    time = data_merger.last_updated(updated_at=fw['updated_at'])
                    state.set_state(time.dict())