import os


# postgres
dsl = {
    'dbname': os.getenv('PG_DB'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASS'),
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT'),
}

# ElasticSearch
es_url = os.getenv('ES_URL')

storage_path = os.getenv('STORAGE', '/storage/state.json')

# back_off
max_tries = 5
max_time = 300
