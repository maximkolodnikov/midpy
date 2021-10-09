from datetime import datetime
from typing import Tuple

from new_etl.es_loader import ESLoader
from psycopg2.extensions import connection as pg_connection
from utils.state import State
from utils.utils import coroutine


class BaseETL:
    """ Базовый класс для ETL процессов. """

    def __init__(self, conn: pg_connection, es_loader: ESLoader, state: State):
        self.es_loader = es_loader
        self.conn = conn
        self.state = state

    def _get_filter_period(self, name: str) -> Tuple:
        """ Возвращает время послелнего процесса elt и текущее время. """
        state_time = self.state.get_state(name)
        if not state_time:
            state_time = datetime(year=2000, month=1, day=1)

        # зафиксируем время начала процесса,
        # если изменения произойдут во время работы etl мы их обработает в следующий раз
        start_time = datetime.now()
        return state_time, start_time

    @staticmethod
    def transform(data: dict) -> dict:
        raise NotImplementedError

    @coroutine
    def load(self, index_name: str):
        """ Обрабатывает полученную пачку данных методом transform и загружает в ElasticSearch. """
        while True:
            data = (yield)
            records = self.transform(data)
            self.es_loader.load_to_es(records, index_name)
