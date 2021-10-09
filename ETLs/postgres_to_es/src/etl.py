import datetime as dt
import logging
import uuid
from typing import Any, Coroutine, List

import backoff

from .config import settings
from .db import DBHanlder
from .es import ESHandler
from .models import (
    EntryName, 
    ESFilmwork, 
    FilmworkId, 
    FilmworkRow, 
    Genre,
    Person, 
    PersonData, 
    Roles
)
from .state import JsonFileStorage, State
from .utils import coroutine

logger = logging.getLogger(__name__)


class ETLBase:
    def __init__(self, entry_name: EntryName):
        self.entry_name = entry_name

        self.producer_table_props = {
            EntryName.genre: {'props': 'id, modified', 'dataclass': Genre },
            EntryName.person: {'props': 'id, modified', 'dataclass': Person },
            EntryName.filmwork: {'props': 'id, modified', 'dataclass': FilmworkId }
        }

        self.fw_m2m_tables = {
            EntryName.genre: 'filmworks_genres',
            EntryName.person: 'filmworks_persons',
        }

        self.es_handler = ESHandler()
        self.db_handler = DBHanlder()
        self.state_handler = State(
            JsonFileStorage(
                file_path=settings.state_json_filepath
            )
        )

        self._person_role_dispatch = {
            Roles.director: self._handle_director,
            Roles.writer: self._handle_writer,
            Roles.actor: self._handle_actor,
        }

    def _update_unique_list(self, lst: List[Any], value: Any) -> List[Any]:
        if value in lst:
            return lst
        
        return [*lst, value]

    def get_or_create_esfilmwork(self, fw_row: FilmworkRow) -> ESFilmwork:
        id = str(fw_row.fw_id)
        
        if id in self.filmworks:
            return self.filmworks[id]
        
        filmwork = ESFilmwork(
            id=id,
            title=fw_row.title,
            description=fw_row.description,
            imdb_rating=fw_row.imdb_rating
        )
        self.filmworks[id] = filmwork

        return filmwork
    
    def _handle_director(self, es_fw: ESFilmwork, fw_row: FilmworkRow) -> ESFilmwork:
        es_fw.director = fw_row.full_name
        
        return es_fw

    def _handle_actor(self, es_fw: ESFilmwork, fw_row: FilmworkRow) -> ESFilmwork:
        actor_data = PersonData(id=str(fw_row.fw_id), name=fw_row.full_name)
        
        es_fw.actors = self._update_unique_list(es_fw.actors, actor_data.dict())
        es_fw.actors_names = self._update_unique_list(es_fw.actors_names, actor_data.name)
        
        return es_fw
    
    def _handle_writer(self, es_fw: ESFilmwork, fw_row: FilmworkRow) -> ESFilmwork:
        writer_data = PersonData(id=str(fw_row.fw_id), name=fw_row.full_name)
        
        es_fw.writers = self._update_unique_list(es_fw.writers, writer_data.dict())
        es_fw.writers_names = self._update_unique_list(es_fw.writers_names, writer_data.name)
        
        return es_fw

    def update_esfilmwork_info(self, es_fw: ESFilmwork, fw_row: FilmworkRow) -> ESFilmwork:
        es_fw.genre = self._update_unique_list(es_fw.genre, fw_row.genre)
        updated_person_es_fw = self._person_role_dispatch[fw_row.role](es_fw, fw_row)

        return updated_person_es_fw

    def get_last_updated_at(self, entry_name: EntryName) -> dt.datetime:
        updated_at = self.state_handler.get_state(f'{entry_name}_updated_at')
        
        if not updated_at:
            return settings.default_updated_at
        
        return updated_at

    def set_last_updated_at(self, entry_name: EntryName, value: dt.datetime):
        self.state_handler.set_state(f'{entry_name}_updated_at', value)
    
    def producer(self, target: Coroutine[None, None, None]):
        updated_at = self.get_last_updated_at(self.entry_name)
        table_name = self.entry_name
        props = self.producer_table_props[self.entry_name]['props']
        DClass = self.producer_table_props[self.entry_name]['dataclass']
        
        while True:
            query = f'''
                SELECT {props}
                FROM content.{table_name}
                WHERE modified > %s
                ORDER BY modified
                LIMIT {settings.data_sql_limit};
            '''
            params = (updated_at,)
            result = [
                DClass(**row) for row in self.db_handler.execute_query(query, params)
            ]
            
            modified_data_ids = [data.id for data in result]
            logger.debug(
                f'Fetched %s modified {self.entry_name}', len(modified_data_ids)
            )
            
            if not modified_data_ids:
                logger.info(f'No updated %s found', self.entry_name)
                break
            
            target.send(modified_data_ids)

            updated_at = result[-1].modified.isoformat()
            self.set_last_updated_at(self.entry_name, updated_at)

    @coroutine
    def enricher(self, target: Coroutine[None, List[uuid.UUID], None]) -> Coroutine:
        while modified_data_ids := (yield):
            data_ids_placeholder = ', '.join(['%s']*len(modified_data_ids))
            updated_at = self.get_last_updated_at(EntryName.filmwork.value)
            
            fw_m2m_predicate = ''
            if self.entry_name != EntryName.filmwork.value:
                fw_m2m_predicate = f'AND mtm.{self.entry_name}_id IN ({data_ids_placeholder})'
            
            m2m_table_name = self.fw_m2m_tables[self.entry_name]
            
            while True:
                query = f'''
                    SELECT fw.id, fw.modified
                    FROM content.filmwork fw
                    LEFT JOIN content.{m2m_table_name} mtm ON mtm.filmwork_id = fw.id
                    WHERE fw.modified > %s {fw_m2m_predicate}
                    ORDER BY fw.modified
                    LIMIT {settings.data_sql_limit};
                '''
                params = (updated_at, *modified_data_ids)
                result = [
                    FilmworkId(**row) for row in self.db_handler.execute_query(query, params)
                ]

                modified_fw_ids = [fw_id.id for fw_id in result]

                if not modified_fw_ids:
                    break
                
                updated_at = result[-1].modified
                target.send(modified_fw_ids)
    
    
    @coroutine
    def merger(self, target: Coroutine[None, List[uuid.UUID], None]) -> Coroutine:
        while modified_fw_ids := (yield):
            data_ids_placeholder = ', '.join(['%s']*len(modified_fw_ids))
            query = f'''
                SELECT
                fw.id as fw_id, 
                fw.title, 
                fw.description, 
                fw.rating as imdb_rating, 
                fw.type, 
                fw.created, 
                fw.modified, 
                fwp.role, 
                p.id as person_id, 
                p.first_name as full_name,
                g.name as genre
                FROM content.filmwork fw
                LEFT JOIN content.filmworks_persons fwp ON fwp.filmwork_id = fw.id
                LEFT JOIN content.person p ON p.id = fwp.person_id
                LEFT JOIN content.filmworks_genres fwg ON fwg.filmwork_id = fw.id
                LEFT JOIN content.genre g ON g.id = fwg.genre_id
                WHERE fw.id IN ({data_ids_placeholder});
                '''
            params = tuple(modified_fw_ids)
            result = [
                FilmworkRow(**row) for row in self.db_handler.execute_query(query, params)
            ]

            target.send(result)
            
    @coroutine
    def transformer(self, target: Coroutine[None, List[uuid.UUID], None]) -> Coroutine:
        while fw_rows := (yield):
            self.filmworks = {}
            
            for fw_row in fw_rows:
                esfilmwork = self.get_or_create_esfilmwork(fw_row)
                updated_filmwork = self.update_esfilmwork_info(esfilmwork, fw_row)
                
                self.filmworks[fw_row.fw_id] = updated_filmwork
            
            es_entries = [es_fw.dict() for es_fw in self.filmworks.values()]

            target.send(es_entries)

    @coroutine
    def loader(self) -> Coroutine:
        while data := (yield):
            self.es_handler.upload_data(data)
            

class ETLOnGenreChanged(ETLBase):
    def producer(self, target: Coroutine[None, List[uuid.UUID], None]):
        genres_updated_at = self.get_last_updated_at(EntryName.genre.value)

        while True:
            query = f'''
                SELECT id, modified
                FROM content.genre
                WHERE modified > %s
                ORDER BY modified
                LIMIT {settings.data_sql_limit};
            '''
            params = (genres_updated_at,)
            result = [
                Genre(**row) for row in self.db_handler.execute_query(query, params)
            ]
            
            modified_genres_ids = [genre_id.id for genre_id in result]
            logger.debug(f'Fetched %s modified genres', len(modified_genres_ids))
            
            if not modified_genres_ids:
                logger.info('No updated genres found')
                break
            
            target.send(modified_genres_ids)

            genres_updated_at = result[-1].modified.isoformat()
            self.set_last_updated_at(EntryName.genre.value, genres_updated_at)

    @coroutine
    def enricher(self, target: Coroutine[None, List[uuid.UUID], None]) -> Coroutine:
        while modified_data_ids := (yield):
            data_ids_placeholder = ', '.join(['%s']*len(modified_data_ids))
            updated_at = self.get_last_updated_at(EntryName.filmwork.value)
            
            while True:
                query = f'''
                    SELECT fw.id, fw.modified
                    FROM content.filmwork fw
                    LEFT JOIN content.filmworks_genres fwg ON fwg.filmwork_id = fw.id
                    WHERE fw.modified > %s AND fwg.genre_id IN ({data_ids_placeholder})
                    ORDER BY fw.modified
                    LIMIT {settings.data_sql_limit};
                '''
                params = (updated_at, *modified_data_ids)
                result = [
                    FilmworkId(**row) for row in self.db_handler.execute_query(query, params)
                ]

                modified_fw_ids = [fw_id.id for fw_id in result]

                if not modified_fw_ids:
                    break
                
                updated_at = result[-1].modified
                target.send(modified_fw_ids)


class ETLOnPersonChanged(ETLBase):
    def producer(self, target: Coroutine[None, List[uuid.UUID], None]):
        persons_updated_at = self.get_last_updated_at(EntryName.person.value)

        while True:
            query = f'''
                SELECT id, modified
                FROM content.person
                WHERE modified > %s
                ORDER BY modified
                LIMIT {settings.data_sql_limit};
            '''
            params = (persons_updated_at,)
            result = [
                Person(**row) for row in self.db_handler.execute_query(query, params)
            ]
            
            modified_persons_ids = [genre_id.id for genre_id in result]
            logger.debug(f'Fetched %s modified persons', len(modified_persons_ids))
            
            if not modified_persons_ids:
                logger.info('No updated persons found')
                break
            
            target.send(modified_persons_ids)

            persons_updated_at = result[-1].modified.isoformat()
            self.set_last_updated_at(EntryName.person.value, persons_updated_at)

    @coroutine
    def enricher(self, target: Coroutine[None, List[uuid.UUID], None]) -> Coroutine:
        while modified_data_ids := (yield):
            data_ids_placeholder = ', '.join(['%s']*len(modified_data_ids))
            updated_at = self.get_last_updated_at(EntryName.filmwork.value)
            
            while True:
                query = f'''
                    SELECT fw.id, fw.modified
                    FROM content.filmwork fw
                    LEFT JOIN content.filmworks_persons fwp ON fwp.filmwork_id = fw.id
                    WHERE fw.modified > %s AND fwp.person_id IN ({data_ids_placeholder})
                    ORDER BY fw.modified
                    LIMIT {settings.data_sql_limit};
                '''
                params = (updated_at, *modified_data_ids)
                result = [
                    FilmworkId(**row) for row in self.db_handler.execute_query(query, params)
                ]

                modified_fw_ids = [fw_id.id for fw_id in result]

                if not modified_fw_ids:
                    break
                
                updated_at = result[-1].modified
                target.send(modified_fw_ids)


class ETLOnFilmworkChanged(ETLBase):
    def producer(self, target: Coroutine[None, List[uuid.UUID], None]):
        target.send([])

    @backoff.on_exception(backoff.expo, Exception, max_time=settings.backoff_maxtime)
    @coroutine
    def enricher(self, target: Coroutine[None, List[uuid.UUID], None]) -> Coroutine:
        while modified_data_ids := (yield):
            fw_updated_at = self.get_last_updated_at(EntryName.filmwork.value)
            
            while True:
                query = f'''
                    SELECT fw.id, fw.modified
                    FROM content.filmwork fw
                    WHERE fw.modifies > %s
                    ORDER BY fw.modified
                    LIMIT {settings.data_sql_limit};
                '''
                params = (fw_updated_at, *modified_data_ids)
                result = [
                    FilmworkId(**row) for row in self.db_handler.execute_query(query, params)
                ]

                modified_fw_ids = [fw_id.id for fw_id in result]

                if not modified_fw_ids:
                    logger.info('No updated filmworks found')
                    break
                
                target.send(modified_fw_ids)
                fw_updated_at = result[-1].modified.isoformat()
                self.set_last_updated_at(EntryName.filmwork.value, fw_updated_at)
