import psycopg2
import json
from psycopg2.extras import DictCursor

from validation import Config, FilmWork

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
        result = []
        film_id_counter = None
        desired_structure = FilmWork
        for item in res:
            fw = dict(item)
            if film_id_counter is None:
                film_id_counter = fw['film_id']
                desired_structure.parse_obj({
                    "fw_id": fw['film_id'],
                })
                print(desired_structure.fw_id)
            if film_id_counter == fw['film_id']:
                film_id_counter = fw['film_id']
                if fw['role'] == 'director':
                    desired_structure.parse_obj({'director':fw['full_name'],
                                                 "genre": [fw['genre']]})
                if fw['role'] == 'actor':
                    desired_structure.parse_obj({'actors': [{'id':fw['person_id'],
                                                            'name':fw['full_name']}],
                                                    'actor_names':[fw['full_name']],
                                                    "genre": [fw['genre']]}
                                                )
                if fw['role'] == 'director':
                    desired_structure.parse_obj({'writers': [{'id':fw['person_id'],
                                                             'name':fw['full_name']}],
                                                    'writers_names':[fw['full_name']],
                                                    "genre": [fw['genre']]}
                                                )
            if film_id_counter != fw['film_id']:
                film_id_counter = fw['film_id']
                result.append(desired_structure)
                desired_structure = FilmWork
                desired_structure.parse_obj({
                    "id": fw['film_id'], "imdb_rating": fw['rating'],
                    "genre": [fw['genre']], "title": fw['title'],
                    "description": fw['description']
                })
