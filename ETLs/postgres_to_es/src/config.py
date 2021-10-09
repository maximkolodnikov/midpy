from pydantic import BaseModel, BaseSettings

import datetime as dt
import logging


class PgDsn(BaseModel):
    dbname: str = 'dbname'
    user: str = 'postgres'
    password: str = 'password'
    host: str = 'localhost'
    port: int = 5432


class Settings(BaseSettings):
    # Data storage
    pg_dsn: PgDsn = PgDsn(
        dbname='',
        user='',
        password='',
        host='127.0.0.1',
        port=5432
    )
    state_json_filepath: str = 'src/state.json'

    # Constants
    default_updated_at: dt.datetime = dt.datetime(1970, 1, 1, 0, 0, 0)
    data_sql_limit: int = 100

    # Logging
    loglevel: int = logging.DEBUG
    logformat: str = '[%(asctime)s]%(name)s::%(levelname)s - %(message)s'

    # ElasticSearch
    es_url: str = 'http://127.0.0.1:9200'
    es_index: str = 'movies'

    # Backoff
    backoff_maxtime = 10


settings = Settings()

logging.basicConfig(
    level=settings.loglevel, 
    format=settings.logformat
)
