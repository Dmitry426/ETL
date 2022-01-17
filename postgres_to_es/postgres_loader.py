from state_operator import State_operator
from data_merger import Data_Merger
import logging
class Load_data:
    def __init__(self, connection_postgres, config):
        self.conn_postgres = connection_postgres
        self.config = config
        self.sql_query = self.config.film_work_pg.sql_query
        self.state = State_operator(config=self.config)
        self.parsed_state = None
        self.results = []

    def _handle_no_date(self):
        """Method to handle the case where we don't have recorded state yet."""
        limit = self.config.film_work_pg.limit
        if self.parsed_state['updated_at'] is None:
            sql_query_params = f""" 
            ORDER BY updated_at
            LIMIT {limit};
        """
            return sql_query_params
        if self.parsed_state['updated_at']:
            sql_query_params = f""" 
            WHERE fw.updated_at > ('%s') 
            ORDER BY updated_at
            LIMIT {limit};
        """ % (self.parsed_state['updated_at'])
            return sql_query_params

    def handle_merge_cases(self, query_data: list):
        """Method to merge datatables by id ,since Data is requested using Left Joins  """
        data_merger = Data_Merger()
        for index,element in enumerate(query_data):
            if index + 1 < len(query_data):
                if element['film_id'] == query_data[index+1]['film_id']:
                    data_merger.combine_tables(element)
                    continue
            data_merger.combine_tables(element)
            result = data_merger.validate_and_return()
            self.results.append(result.dict())

        return self.results
    def load_from_postgres(self):
        """Simple postgres loader"""
        try:
            with self.conn_postgres.cursor() as cursor:
                self.parsed_state = self.state.validate_load_timestamp()
                query = self.sql_query.format(self._handle_no_date())
                cursor.execute(query)
                response = cursor.fetchall()
        except  Exception as e :
            logging.exception(e)
        return response
