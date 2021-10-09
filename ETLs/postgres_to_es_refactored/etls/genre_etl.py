import backoff
import psycopg2
from new_etl.base_elt import BaseETL
from new_etl.config import dsl, es_url, max_time, max_tries, storage_path
from new_etl.es_loader import ESLoader
from psycopg2.extras import DictCursor
from utils.logger import logger
from utils.state import JsonFileStorage, State
from utils.utils import coroutine


class GenreETL(BaseETL):
    """ ETL обработки изменений в жанрах. """

    @coroutine
    @backoff.on_exception(backoff.expo, psycopg2.Error, max_tries=max_tries, max_time=max_time, logger=logger)
    def extract_genres(self, target):
        """ Корутина выгружает пачки жанров, которые изменились с момента сохраненного в state. """
        sql = '''
            WITH params (time_from, time_to, ids) as (values (%s, %s, %s))
            SELECT id
            FROM cinema.genre, params
            WHERE CASE
                WHEN ids=''
                    THEN updated_at BETWEEN time_from::timestamp  AND time_to::timestamp
                    ELSE updated_at BETWEEN time_from::timestamp  AND time_to::timestamp and id > ids::uuid
                END
            ORDER BY id, updated_at
            LIMIT 10
        '''
        state_time, start_time = self._get_filter_period('genre_elt_time')
        ids = ''
        logger.info('start extract_genres state time %s start time %s', state_time, start_time)
        while True:
            genre_ids = []
            cur = self.conn.cursor()
            cur.execute(sql, (state_time, start_time, ids))

            last_id = ''
            for genre in cur:
                last_id = genre['id']
                genre_ids.append(last_id)

            ids = last_id  # сохраним последний id для фильтрации

            if genre_ids:
                logger.info('extract_genres send %s', len(genre_ids))
                target.send(genre_ids)
            else:
                # данные закончились, сохраним время и выйдем из корутины
                self.state.set_state('genre_elt_time', start_time)
                logger.info('stop extract_genres  %s  %s', state_time, ids)
                raise GeneratorExit

    @coroutine
    @backoff.on_exception(backoff.expo, psycopg2.Error, max_tries=max_tries, max_time=max_time, logger=logger)
    def extract(self, target):
        """ Корутина получения полных данных по жанру из Postgres. """
        sql = '''
            SELECT
                g.id as genre_id,
                g.title as title,
                g.description as description
            FROM cinema.genre g
            WHERE g.id IN %s;
           '''
        while True:
            genre_ids = (yield)
            cur = self.conn.cursor()
            cur.execute(sql, (tuple(genre_ids),))
            data = cur.fetchall()
            logger.info('extract send %s ', len(genre_ids))
            target.send(data)

    @staticmethod
    def transform(data: dict) -> dict:
        """ Обрабатывает сырые данные и преобразовывает в формат, пригодный для ElasticSearch. """
        records = {}
        for row in data:
            genre_id = row['genre_id']
            if genre_id not in records:
                records[genre_id] = {
                    'id': genre_id,
                    'title': row['title'],
                    'description': row['description'],
                }

        return records


if __name__ == "__main__":
    """ Запускает ETL Process обработки изменений жанров. """

    loader = ESLoader(url=es_url)
    storage = JsonFileStorage(storage_path)

    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        etl = GenreETL(conn=pg_conn, es_loader=loader, state=State(storage))
        try:
            load_data = etl.load('genres')
            all_data = etl.extract(load_data)
            etl.extract_genres(all_data)

        except GeneratorExit:
            logger.info('exit genre ETL')
