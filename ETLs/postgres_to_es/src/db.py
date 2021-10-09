import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.errors import DatabaseError, ConnectionException
from .config import settings
import backoff

psycopg2.extras.register_uuid()


class DBHanlder:
    def __init__(self, dsn: dict = None):
        self.dsn = dsn
        if dsn is None:
            self.dsn = settings.pg_dsn.dict()
        
        self.conn = self._get_conn()
        self.cur = self.conn.cursor()

    @backoff.on_exception(backoff.expo, DatabaseError, max_time=settings.backoff_maxtime)
    def execute_query(self, query: str, params: tuple) -> dict:
        self.cur.execute(query, params)
        
        return self.cur.fetchall()

    @backoff.on_exception(backoff.expo, ConnectionException, max_time=settings.backoff_maxtime)
    def _get_conn(self):
        return psycopg2.connect(**self.dsn, cursor_factory=RealDictCursor)