import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from state_manager import State
from validation import Config

sql = """SELECT
    fw.id as fw_id, 
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.created_at, 
    fw.updated_at, 
    pfw.role, 
    p.id, 
    p.full_name,
    g.name
FROM content.film_work fw
INNER JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
INNER JOIN content.person p ON p.id = pfw.person_id
INNER JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
INNER JOIN content.genre g ON g.id = gfw.genre_id 
LIMIT 10000
"""



config  = Config.parse_file('config.json')
dsl =  dict(config.film_work_pg.dsl)

with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
    with pg_conn.cursor() as cursor :
        cursor.execute(sql)
        res =cursor.fetchall()
        print(res[1000])

