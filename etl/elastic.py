import json
import logging

from elasticsearch import Elasticsearch, NotFoundError

from decorator import backoff
from settings import ESSettings, ETLSettings
from schemas import ESFilmwork


class Elastic:
    def __init__(self):
        es_settings = ESSettings()
        etl_settings = ETLSettings()
        self.schema = etl_settings.SCHEMA_PATH
        self.index = es_settings.INDEX
        self.es = Elasticsearch(es_settings.ADDRESS)


    def create_index(self):
        try:
            is_exist_index = self.es.indices.get(index=self.index)       # Не совсем понятно замечание :(
        except NotFoundError:                                            # что именно нужно вызвать повторно?
            with open(self.schema, 'r') as json_data:
                index_schema = json.load(json_data)
            self.es.indices.create(index=self.index, body=index_schema)

    def transform(self, batch: list):
        logging.info('Приведение данных из ПГ в ЕС формат')
        list_for_bulk = []
        for row in batch:
            es_record = ESFilmwork(id=row['id'],
                       imdb_rating=row['rating'],
                       genre=row['genre'],
                       title=row['title'],
                       description=row['description'],
                       director=[person['person_name'] for person in row['persons'] if person['person_role'] == 'director'],
                       actors_names=[person['person_name'] for person in row['persons'] if person['person_role'] == 'actor'],
                       writers_names=[person['person_name'] for person in row['persons'] if person['person_role'] == 'writer'],
                       actors=[{'id': person['person_id'],
                                'name': person['person_name']} for person in row['persons'] if person['person_role'] == 'actor'],
                       writers=[{'id': person['person_id'],
                                 'name': person['person_name']} for person in row['persons'] if person['person_role'] == 'writer']
                       )
            list_for_bulk.append(es_record)
        return list_for_bulk

    @backoff(start_sleep_time=0.5)
    def bulk_update(self, docs: list[ESFilmwork]):
        logging.info('Обновление индекса')
        if not docs:
            return None
        body = ''
        for doc in docs:
            index = {'index': {'_index': self.index, '_id': doc.id}}
            body += json.dumps(index) + '\n' + json.dumps(doc.dict(exclude={'id'})) + '\n'
        self.es.bulk(body=body)
        return True
