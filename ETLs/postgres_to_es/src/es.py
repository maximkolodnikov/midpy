import json
import logging
from typing import List, Optional
from urllib.parse import urljoin

import backoff
import requests

from .config import settings

logger = logging.getLogger(__name__)


class ESHandler:
    def __init__(
        self, 
        root_url: Optional[str] = None, 
        index_name: Optional[str] = None
    ):
        self.es_root_url = root_url or settings.es_url
        self.index_name = index_name or settings.es_index
    
    def _get_es_bulk_query(self, rows: List[dict]) -> List[str]:
        """
        Подготавливает bulk-запрос в Elasticsearch
        """
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': self.index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return '\n'.join(prepared_query) + '\n'
    
    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=settings.backoff_maxtime)
    def bulk_request(self, query_data):
        """
        Выполняет bulk запрос в Elasticsearch
        """
        es_post_response = requests.post(
            urljoin(self.es_root_url, '_bulk'),
            data=query_data,
            headers={
                'Content-type': 'application/json'
            }
        ).content.decode()
        response_json = json.loads(es_post_response)
        
        return response_json

    def upload_data(self, data):
        """
        Загружает данные в Elasticsearch
        """
        logger.debug(f'Loading {len(data)} items to ES')
        
        prepared_data = self._get_es_bulk_query(data)
        json_response = self.bulk_request(prepared_data)

        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(f'{error_message}')
