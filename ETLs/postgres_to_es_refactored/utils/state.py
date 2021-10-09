import abc
import json
from typing import Any

from utils.utils import default_json_encoder


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.retrieve_state()

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as f:
            json.dump(state, f, default=default_json_encoder)

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = {}

    def set_state(self, key: str, value: Any) -> None:
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str, default: Any = None) -> Any:
        if key not in self.state:
            self.state = self.storage.retrieve_state()
        return self.state.get(key, default)
