from datetime import datetime
from typing import Union

from pydantic import BaseModel


class PGFilmwork(BaseModel):
    id: str
    rating: float
    genre: list
    title: str
    description: Union[str, None]
    updated_at: datetime
    persons: list


class ESFilmwork(BaseModel):
    id: str
    imdb_rating: Union[float, None]
    genre: list
    title: str
    description: Union[str, None]
    director: Union[list, None]
    actors_names: Union[list, None]
    writers_names: Union[list, None]
    actors: list
    writers: list
