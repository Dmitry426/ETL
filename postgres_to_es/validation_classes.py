from typing import Optional, List
from pydantic import BaseModel, validator
from pydantic.validators import UUID
from datetime import  datetime


class DSNSettings(BaseModel):
    host: str
    port: int
    dbname: str
    password: str
    user: str


class PostgresSettings(BaseModel):
    dsl: DSNSettings
    limit: Optional[int]
    order_field: List[str]
    state_field: List[str]
    fetch_delay: Optional[float]
    state_file_path: Optional[str]
    sql_query: str


class Config(BaseModel):
    film_work_pg: PostgresSettings


class Person(BaseModel):
    id:UUID
    name:str

class Datetime_serialization(BaseModel):
        updated_at: datetime = None


class FilmWork(BaseModel):
    id:UUID
    imdb_rating:float = None
    genre: List[str]
    title:str =None
    description:str = None
    director:str =None
    actors_names:Optional[List[str]]
    writers_names:Optional[List[str]]
    actors:Optional[List[Person]]
    writers:Optional[List[Person]]

    @validator('director', 'description', 'title')
    def handle_empty_str(cls, variable: str) -> str:
        if not variable:
            return None
        return variable