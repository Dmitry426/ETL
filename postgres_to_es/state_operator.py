from state_manager import State, JsonFileStorage
from validation_classes import  Datetime_serialization

class State_operator:
    def __init__(self,config):
        self.config = config
        self.file_path = self.config.film_work_pg.state_file_path
        self.json_file_storage = JsonFileStorage(file_path=self.file_path)
        self.state = State(self.json_file_storage)
        self.updated_at = None
        self.parsed_time = None

    def validate_load_timestamp(self):
        self.updated_at = self.state.get_state(key='updated_at')
        parsed_time = Datetime_serialization.parse_obj({'updated_at': self.updated_at}).dict()
        return parsed_time

    def validate_save_timestamp(self,timestamp:object):
        self.updated_at = timestamp
        parsed_time = Datetime_serialization.parse_obj({'updated_at': self.updated_at}).dict()
        self.state.set_state(key='updated_at', value=str(parsed_time['updated_at']))
