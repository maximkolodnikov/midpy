import logging

from src.etl import ETLBase

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    logger.info('ETL started.')
    genre_etl = ETLBase('genre')
    person_etl = ETLBase('person')
    filmwork_etl = ETLBase('filmwork')

    logger.info('ETL on genres changed started.')
    genre_etl.producer(
        genre_etl.enricher(
            genre_etl.merger(
                genre_etl.transformer(
                    genre_etl.loader()
                )
            )
        )
    )

    logger.info('ETL on persons changed started.')
    person_etl.producer(
        person_etl.enricher(
            person_etl.merger(
                person_etl.transformer(
                    person_etl.loader()
                )
            )
        )
    )

    logger.info('ETL on filmworks changed started.')
    filmwork_etl.producer(
        filmwork_etl.merger(
            filmwork_etl.transformer(
                filmwork_etl.loader()
            )
        )
    )
