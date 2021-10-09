import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List


class MovieRole(Enum):
    ACTOR = 'actor'
    DIRECTOR = 'director'
    WRITER = 'writer'


class RatingAgency(Enum):
    IMDB = 'IMDB'


class RussianContentRatingSystem(Enum):
    ALL_AGES = '0+'
    OVER_6 = '6+'
    OVER_12 = '12+'
    OVER_16 = '16+'
    OVER_18 = '18+'


@dataclass
class MovieRating:
    agency: RatingAgency
    rating: float


@dataclass
class MovieUnit:
    id: uuid.UUID
    full_name: str
    role: MovieRole


@dataclass
class MovieGenre:
    id: uuid.UUID
    title: str


@dataclass
class Movie:
    id: uuid.UUID
    title: str
    plot: str
    genres: List[MovieGenre]
    ratings: List[MovieRating]
    units: List[MovieUnit]
    certificate: Optional[RussianContentRatingSystem]
    poster_url: str


@dataclass
class EsIndexPerson:
    """Elastichsearch movie index nested person schema representation."""
    id: str
    full_name: str


@dataclass
class EsIndexGenre:
    """Elastichsearch movie index nested genre schema representation."""
    id: str
    title: str


@dataclass
class EsIndexMovie:
    """Elastichsearch index schema representation."""
    id: str
    imdb_rating: float
    genre: str
    title: str
    description: str
    directors_names: str
    actors_names: str
    writers_names: str
    directors: List[EsIndexPerson]
    actors: List[EsIndexPerson]
    writers: List[EsIndexPerson]
    genres: List[EsIndexGenre]
    certificate: str
    poster_url: str
