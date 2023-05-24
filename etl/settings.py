from pydantic import BaseSettings, Field


class PGSettings(BaseSettings):
    DB_NAME: str = 'movies_database'
    DB_USER: str = 'app'
    DB_PASSWORD: str = '123qwe'
    DB_HOST: str = '0.0.0.0'
    DB_PORT: int = 5432


class ESSettings(BaseSettings):
    INDEX: str = 'movies'
    ADDRESS: str = 'http://0.0.0.0:9200'


class ETLSettings(BaseSettings):
    BATCH_SIZE: int = 100
    STORAGE_PATH: str = 'storage.json'
    SCHEMA_PATH: str = 'movies_index_schema.json'

