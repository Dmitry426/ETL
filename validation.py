from typing import Optional, List
from pydantic import BaseModel
from pydantic.validators import UUID


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

class FilmWork(BaseModel):
    fw_id:UUID
    imdb_rating:Optional[float]
    genre: Optional[List[str]]
    title:Optional[str]
    description:Optional[str]
    director:Optional[str]
    actors_names: Optional[List[str]]
    writers_names:Optional[List[str]]
    actors:Optional[List[Person]]
    writers:Optional[List[Person]]
