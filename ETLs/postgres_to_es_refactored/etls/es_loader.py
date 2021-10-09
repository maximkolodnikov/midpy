import json
from typing import Dict, List
from urllib.parse import urljoin

import backoff
import requests
from utils.logger import logger
from utils.utils import default_json_encoder

from .config import max_time, max_tries


class ESLoader:
    """ Класс для загрузки данных в ElasticSearch. """
    def __init__(self, url: str):
        self.url = url

    @staticmethod
    def _get_es_bulk_query(rows: Dict, index_name: str) -> List[str]:
        """ Подготавливает bulk-запрос в ElasticSearch. """
        prepared_query = []
        for row in rows.values():
            prepared_query.extend([
                json.dumps(
                    {'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row, default=default_json_encoder)
            ])
        return prepared_query

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                          max_tries=max_tries, max_time=max_time, logger=logger)
    def load_to_es(self, records: Dict, index_name: str):
        """ Отправка запроса в ES и разбор ошибок сохранения данных. """
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )
        logger.info('bulk %s objects with status %s', len(records), response.status_code)

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)
