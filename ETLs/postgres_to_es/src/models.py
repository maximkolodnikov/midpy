import datetime as dt
import uuid
from enum import Enum
from typing import List

from pydantic import BaseModel

    
class Roles(str, Enum):
    director = 'DIRECTOR'
    actor = 'ACTOR'
    writer = 'WRITER'


class EntryName(str, Enum):
    genre = 'genre'
    person = 'person'
    filmwork = 'filmwork'


class Person(BaseModel):
    id: uuid.UUID
    modified: dt.datetime


class Genre(BaseModel):
    id: uuid.UUID
    modified: dt.datetime


class Person(BaseModel):
    id: uuid.UUID
    modified: dt.datetime


class FilmworkId(BaseModel):
    id: uuid.UUID
    modified: dt.datetime


class FilmworkRow(BaseModel):
    fw_id: uuid.UUID
    title: str
    description: str
    imdb_rating: float
    type: str
    created: dt.datetime
    modified: dt.datetime
    role: str
    person_id: uuid.UUID
    full_name: str
    genre: str


class ESFilmwork(BaseModel):
    id: str
    title: str
    description: str
    imdb_rating: float
    genre: List[str] = []
    writers: List[str] = []
    actors: List[str] = []
    director: List[str] = []
    actors_names: List[str] = []
    writers_names: List[str] = []


class PersonData(BaseModel):
    id: str
    name: str
