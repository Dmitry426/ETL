import psycopg2
from psycopg2.extras import DictCursor

from validation_classes import Config
from data_merger import Data_Merger
sql = """SELECT
    fw.id as film_id, 
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.updated_at, 
    pfw.role, 
    p.id as person_id, 
    p.full_name,
    g.name as genre
FROM content.film_work fw
INNER JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
INNER JOIN content.person p ON p.id = pfw.person_id
INNER JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
INNER JOIN content.genre g ON g.id = gfw.genre_id
LIMIT 100
"""

config = Config.parse_file('config.json')
dsl = dict(config.film_work_pg.dsl)

with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    with pg_conn.cursor() as cursor:
        cursor.execute(sql)
        res = cursor.fetchall()
        results = []
        film_id_counter = None
        data_merger =Data_Merger()
        for item in res:
            fw = dict(item)
            if film_id_counter is None:
                data_merger.combine_tables(obj=fw)
                film_id_counter = fw['film_id']
            if film_id_counter == fw['film_id']:
                data_merger.combine_tables(obj=fw)
            if film_id_counter != fw['film_id']:
                result =data_merger.validate_and_return()
                results.append(result)
                data_merger.clean()
                film_id_counter = fw['film_id']
                data_merger.combine_tables(obj=fw)
    to_load = (results[0].dict())