import logging
from time import sleep

from elastic import Elastic
from settings import ETLSettings, PGSettings
from extractor import PGExtractor
from state import JsonFileStorage


class ETLProcess:
    def __init__(self):
        etl_settings = ETLSettings()
        pg_settings = PGSettings()
        self.storage_path = etl_settings.STORAGE_PATH
        self.limit = etl_settings.BATCH_SIZE
        self.pg_extractor = PGExtractor()
        self.json_storage = JsonFileStorage(self.storage_path)
        self.elastic = Elastic()

    def start(self):
        self.elastic.create_index()

        offset = 0
        while True:
            logging.info('Старт сервиса')
            is_exist_state = self.json_storage.retrieve_state()
            if not is_exist_state:
                logging.info('Состояние не найдено')
                batch = self.pg_extractor.get_all_films(self.limit, offset)
                transformed_batch = self.elastic.transform(batch)
                self.elastic.bulk_update(transformed_batch)
                if batch:
                    offset += self.limit
                    last_update_date = batch[0]['modified'].isoformat()
                    updated_index = False
                else:
                    updated_index = True
            else:
                logging.info('Состояние найдено')
                batch = self.pg_extractor.get_updated_films(is_exist_state['last_update'], self.limit, offset)
                if not batch:
                    logging.info('Все данные синхронизированы. Повторный запуск через 1 минуту')
                    offset = 0
                    sleep(60)
                    continue
                transformed_batch = self.elastic.transform(batch)
                updated_index = self.elastic.bulk_update(transformed_batch)

            if updated_index:
                update_date = batch[0]['modified'].isoformat() if batch else last_update_date
                logging.info('Сохранение состояния в хранилище')
                self.json_storage.save_state({'last_update': update_date})
                offset += self.limit


if __name__ == '__main__':
    ETLProcess().start()
