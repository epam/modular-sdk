from typing import Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from modular_sdk.commons.helpers import replace_keys_in_dict


class MongoDBConnection:

    def __init__(self, mongo_uri: str,
                 default_db_name: Optional[str] = None) -> None:
        self._mongo_uri = mongo_uri
        self._default_db_name = default_db_name

        self._client: Optional[MongoClient] = None
        self._db_cache, self._collection_cache = {}, {}

    def _or_default(self, db_name: Optional[str]) -> str:
        return db_name or self._default_db_name

    @property
    def client(self) -> MongoClient:
        if not self._client:
            self._client = MongoClient(self._mongo_uri)
        return self._client

    def database(self, db_name: Optional[str] = None) -> Database:
        db_name = self._or_default(db_name)
        if db_name not in self._db_cache:
            self._db_cache[db_name] = self.client.get_database(name=db_name)
        return self._db_cache[db_name]

    def collection(self, collection_name: str,
                   db_name: Optional[str] = None) -> Collection:
        db_name = self._or_default(db_name)
        database = self.database(db_name)
        _key = (db_name, collection_name)
        if _key not in self._collection_cache:
            self._collection_cache[_key] = database.get_collection(
                collection_name)
        return self._collection_cache[_key]

    @staticmethod
    def encode_keys(dictionary: dict) -> dict:
        return replace_keys_in_dict(dictionary, '.', '|#|')

    @staticmethod
    def decode_keys(dictionary: dict) -> dict:
        return replace_keys_in_dict(dictionary, '|#|', '.')
