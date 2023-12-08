from .client_adapter import MongoClientAdapter, Operations
from pydantic import BaseModel
from typing import List, Tuple, Dict, Any, Union, Optional, Type, TypeVar, Generic


class AsyncPydanticQueryBuilder:
    def __init__(self, adapter, db_name, collection_name, model: Type[T]):
        self.adapter = adapter
        self.db_name = db_name
        self.collection_name = collection_name
        self.model = model

    async def __aenter__(self):
        self.consumer = self.adapter.create_consumer(self.db_name)
        self.collection = self.adapter.get_collection(self.consumer, self.collection_name)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.adapter.close_consumer(self.consumer)

        # New method to select fields
    

    # TODO: Improve and fix this method
    

    async def insert_one(self, document: ) -> str:
        result = await self.collection.insert_one(document.dict())
        return str(result.inserted_id)

    async def find_one(self, query=None, projection=None) -> Optional[T]:
        if query is None:
            query = {}

        if projection is None:
            projection = {}
        result = await self.collection.find_one(query, projection)
        return self.model.parse_obj(result) if result else None

    async def update_one(self, query, update, upsert=False) -> int:
        result = await self.collection.update_one(query, update, upsert=upsert)
        return result.modified_count

    async def delete_one(self, query) -> int:
        result = await self.collection.delete_one(query)
        return result.deleted_count

    async def insert_many(self, documents: List[T]) -> List[str]:
        result = await self.collection.insert_many([doc.dict() for doc in documents])
        return [str(obj_id) for obj_id in result.inserted_ids]

    async def find(self, query=None, projection=None) -> List[T]:
        if query is None:
            query = {}

        if projection is None:
            projection = {}

        cursor = await self.collection.find(query, projection)
        results = await cursor.to_list(length=None)
        return [self.model.parse_obj(result) for result in results]

    async def update_many(self, query, update, upsert=False) -> int:
        result = await self.collection.update_many(query, update, upsert=upsert)
        return result.modified_count

    async def delete_many(self, query) -> int:
        result = await self.collection.delete_many(query)
        return result.deleted_count

    async def count_documents(self, query) -> int:
        return await self.collection.count_documents(query)

    

    async def create_multi_indexes(self, indexes: List[Tuple[Union[str, List[Tuple[str, int]]], Dict[str, Any]]]) -> List[str]:
        index_models = []
        for keys, options in indexes:
            if isinstance(keys, str):
                field_name = self.model.__fields__[keys].alias
                keys = [(field_name, 1)]
            index_model = pymongo.IndexModel(keys, **options)
            index_models.append(index_model)

        return await self.collection.create_indexes(index_models)
