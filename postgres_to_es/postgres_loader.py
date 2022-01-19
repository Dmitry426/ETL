import logging
from typing import Iterator

from state_operator import State_operator
from data_merger import Data_Merger
from psycopg2 import OperationalError


class Load_data:
    def __init__(self, connection_postgres, config):
        self.results = None
        self.logger = logging.getLogger('migrate_etl')
        self.conn_postgres = connection_postgres
        self.state = State_operator(config=config)
        self.config = config.film_work_pg
        self.sql_query_film_work = self.config.sql_query_film_work
        self.sql_query_person = self.config.sql_query_person
        self.sql_query_person_film_work = self.config.sql_query_person_film_work
        self.sql_query_genre = self.config.sql_query_genre
        self.sql_query_genre_film_work = self.config.sql_query_genre_film_work
        self.parsed_state = None
        self.film_work_ids = []

    def _handle_no_date(self):
        """Method to handle the case where we don't have recorded state yet."""
        limit = 1000
        if self.parsed_state['updated_at'] is None:
            sql_query_params = f""" 
            ORDER BY updated_at
            LIMIT {limit};
        """
            return sql_query_params
        if self.parsed_state['updated_at']:
            sql_query_params = f""" 
            WHERE updated_at > ('%s') 
            ORDER BY updated_at
            LIMIT {limit};
        """ % (self.parsed_state['updated_at'])
            return sql_query_params

    def handle_merge_cases(self, query_data: list):
        """Method to merge datatables by id ,since Data is requested using Left Joins  """
        data_merger = Data_Merger()
        for index, element in enumerate(query_data):
            if index + 1 < len(query_data):
                if element['film_id'] == query_data[index + 1]['film_id']:
                    data_merger.combine_tables(element)
                    continue
            data_merger.combine_tables(element)
            result = data_merger.validate_and_return()
            self.results.append(result.dict())

        return self.results

    def _load_updated_ids(self, cursor, query: str):
        """Method to load updated_at and ids from related to film_work fields """
        query_ids = query.format(self._handle_no_date())
        cursor.execute(query_ids)
        result = cursor.fetchall()
        ids = [res['id'] for res in result]
        return ids

    def _load_related_fw_id(self, cursor, ids: str, query: str) -> Iterator[str]:
        """Method to load related  film_work ids   """
        args = cursor.mogrify(query, (tuple(ids),))
        cursor.execute(args)
        fetch_fw_ids = cursor.fetchall()
        for data in dict(fetch_fw_ids):
            if data not in self.film_work_ids:
                self.film_work_ids.append(data)

    def _all_tables_by_id(self, cursor, query: str, ids: list):
        args = cursor.mogrify(query, (tuple(ids),))
        cursor.execute(args)
        final_result = cursor.fetchall()
        return final_result

    def load_from_postgres(self):
        """Method for cursor factory and load methods management """
        try:
            with self.conn_postgres.cursor() as cursor:
                self.parsed_state = self.state.validate_load_timestamp()
                updated_persons_ids = self._load_updated_ids(
                    cursor=cursor, query=self.sql_query_person)
                self._load_related_fw_id(cursor=cursor, ids=updated_persons_ids,
                                         query=self.sql_query_person_film_work)
                updated_genre_ids = self._load_updated_ids(
                    cursor=cursor, query=self.sql_query_genre)
                self._load_related_fw_id(cursor=cursor, ids=updated_genre_ids,
                                         query=self.sql_query_genre_film_work)
                query_all_by_id = self._all_tables_by_id(cursor=cursor,
                                                         query=self.sql_query_film_work,
                                                         ids=self.film_work_ids)
                return query_all_by_id
        except OperationalError as e:
            self.logger.exception(e)
