import abc
import json
from typing import Any, Optional
from datetime import datetime


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as fp:
            json.dump(state, fp)

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        with open(self.file_path, "r") as fp:
            data = json.load(fp )
        return data


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        load = self.storage.retrieve_state()
        load[key] = value
        self.storage.save_state(load)

    def get_state(self, key: str) -> Any:
        result = self.storage.retrieve_state()
        if result:
            return result[key]
        else:
            return None
