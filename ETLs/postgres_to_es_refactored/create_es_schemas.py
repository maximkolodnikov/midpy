import json
import os
from pathlib import Path
from urllib.parse import urljoin

import requests
from utils.logger import logger


def init_schema(url, name: str, schema_json: json):
    """ Создает ES индекс согласно переданной json схеме. """

    response = requests.put(
        urljoin(url, name),
        json=schema_json,
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code == 200:
        logger.info('create es schema %s', response.text)
    elif response.status_code == 400:
        logger.info('es schema %s already exists', response.text)
    else:
        logger.error('error create es schema %s', response.text)


if __name__ == '__main__':
    """ Процесс создания ES индексов загруженных в папку es_schemas. """

    es_url = os.getenv('ES_URL', 'http://127.0.0.1:9200/')
    BASE_DIR = Path(__file__).resolve(strict=True).parent
    schemas = list(BASE_DIR.joinpath('schemas').glob('**/*.json'))

    for schema in schemas:
        with open(str(schema), 'r') as f:
            schema_json = json.load(f)
            init_schema(url=es_url, name=schema.stem, schema_json=schema_json)
