import backoff
import psycopg2
from new_etl.base_elt import BaseETL
from new_etl.config import dsl, es_url, max_time, max_tries, storage_path
from new_etl.es_loader import ESLoader
from psycopg2.extras import DictCursor
from utils.logger import logger
from utils.state import JsonFileStorage, State
from utils.utils import coroutine


class PersonETL(BaseETL):
    """ ETL обработки изменений в персонах. """

    @coroutine
    @backoff.on_exception(backoff.expo, psycopg2.Error, max_tries=max_tries, max_time=max_time, logger=logger)
    def extract_persons(self, target):
        """ Корутина выгружает пачки персон, которые изменились с момента сохраненного в state. """
        sql = '''
            WITH params (time_from, time_to, ids) as (values (%s, %s, %s))
            SELECT id
            FROM cinema.person, params
            WHERE CASE
                WHEN ids=''
                    THEN updated_at BETWEEN time_from::timestamp  AND time_to::timestamp
                    ELSE updated_at BETWEEN time_from::timestamp  AND time_to::timestamp and id > ids::uuid
                END
            ORDER BY id, updated_at
            LIMIT 100
        '''
        state_time, start_time = self._get_filter_period('person_elt_time')
        ids = ''
        logger.info('start extract_persons state time %s start time %s', state_time, start_time)
        while True:
            person_ids = []
            cur = self.conn.cursor()
            cur.execute(sql, (state_time, start_time, ids))

            last_id = ''
            for person in cur:
                last_id = person['id']
                person_ids.append(last_id)

            ids = last_id  # сохраним последний id для фильтрации

            if person_ids:
                logger.info('extract persons send %s', len(person_ids))
                target.send(person_ids)
            else:
                # данные закончились, сохраним время и выйдем из корутины
                self.state.set_state('person_elt_time', start_time)
                logger.info('stop extract persons  %s  %s', state_time, ids)
                raise GeneratorExit

    @coroutine
    @backoff.on_exception(backoff.expo, psycopg2.Error, max_tries=max_tries, max_time=max_time, logger=logger)
    def extract(self, target):
        """ Корутина получения полных данных по персоне из Postgres. """
        sql = '''
            SELECT
                p.id as person_id,
                p.full_name as full_name
            FROM cinema.person p
            WHERE p.id IN %s;
        '''
        while True:
            person_ids = (yield)
            cur = self.conn.cursor()
            cur.execute(sql, (tuple(person_ids),))
            data = cur.fetchall()
            logger.info('extract send %s ', len(person_ids))
            target.send(data)

    @staticmethod
    def transform(data: dict) -> dict:
        """ Обрабатывает сырые данные и преобразовывает в формат, пригодный для ElasticSearch. """
        records = {}
        for row in data:
            person_id = row['person_id']
            if person_id not in records:
                records[person_id] = {
                    'id': person_id,
                    'full_name': row['full_name'],
                }

        return records


if __name__ == "__main__":
    """ Запускает ETL Process обработки изменений персон. """

    loader = ESLoader(url=es_url)
    storage = JsonFileStorage(storage_path)

    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        etl = PersonETL(conn=pg_conn, es_loader=loader, state=State(storage))
        try:
            load_data = etl.load('persons')
            all_data = etl.extract(load_data)
            etl.extract_persons(all_data)

        except GeneratorExit:
            logger.info('exit person ETL')
