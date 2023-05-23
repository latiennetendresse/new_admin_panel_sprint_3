import json
import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch import TransportError
from dotenv import load_dotenv

from decorator import backoff
from schemas import ESFilmwork

load_dotenv()


class Elastic:
    def __init__(self):
        self.host = os.environ.get('LOCALHOST')
        self.port = os.environ.get('ELASTIC_PORT')
        self.index = os.environ.get('ELASTIC_INDEX')
        self.es = Elasticsearch(f'http://{self.host}:{self.port}')

    @backoff(start_sleep_time=0.5)
    def create_index(self):
        try:
            is_exist_index = self.es.indices.get(index=f'{self.index}')
            if not is_exist_index:
                with open(os.environ.get('SCHEMA_PATH'), 'r') as json_data:
                    index_schema = json.load(json_data)
                self.es.indices.create(index=self.index, body=index_schema)

        except TransportError as warning:
            logging.warning(warning)

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
        try:
            if not docs:
                return None
            body = ''
            for doc in docs:
                index = {'index': {'_index': self.index, '_id': doc.id}}
                body += json.dumps(index) + '\n' + json.dumps(doc.dict(exclude={'id'})) + '\n'
            self.es.bulk(body=body)
        except self.es.ElasticsearchException() as error:
            logging.error('Ошибка при обновлении индекса')
            return False
        return True
