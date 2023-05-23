import abc
import logging
from typing import Any
import json
from typing import Dict
import os.path


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        logging.info('Получение состояния из хранилища')
        if not os.path.exists(self.file_path):
            with open('storage.json', 'w') as new_file:
                json.dump({}, new_file)

        with open(self.file_path, 'r') as file:
            state_from_storage = json.load(file)
        logging.info('Состояние получено')
        return state_from_storage


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state_from_storage = self.storage.retrieve_state()
        state_from_storage[key] = value
        self.storage.save_state(state_from_storage)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return self.storage.retrieve_state().get(key)
