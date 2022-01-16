from state_operator import State_operator
from data_merger import Data_Merger


class Load_data:
    def __init__(self, connection_postgres, config):
        self.conn_postgres = connection_postgres
        self.config = config
        self.sql_query = self.config.film_work_pg.sql_query
        self.state = State_operator(config=self.config)
        self.parsed_state = None
        self.results = []
        self.film_id_counter = None

    def handle_no_date(self):
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


    def load_from_postgres(self):
        self.film_id_counter = None
        with self.conn_postgres.cursor() as cursor:
            self.parsed_state= self.state.validate_load_timestamp()
            query = self.sql_query.format(self.handle_no_date())
            cursor.execute(query)
            response = cursor.fetchall()
            data_merger = Data_Merger()
            for obj in iter(response):
                film = dict(obj)
                if self.film_id_counter is None:
                    data_merger.combine_tables(obj=film)
                    self.film_id_counter = film['film_id']
                if self.film_id_counter == film['film_id']:
                    data_merger.combine_tables(obj=film)
                if self.film_id_counter != film['film_id']:
                    result = data_merger.validate_and_return()
                    self.results.append(result.dict())
                    self.film_id_counter = film['film_id']
                    data_merger.combine_tables(obj=film)
        return self.results
