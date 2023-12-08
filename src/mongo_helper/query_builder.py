from .client_adapter import MongoClientAdapter
from typing import List, Tuple, Dict, Any, Union, Optional
from pymongo import IndexModel
class AsyncQueryBuilder:
    def __init__(self, adapter: MongoClientAdapter, database_name, collection_name):
        self.adapter = adapter
        self.database_name = database_name
        self.collection_name = collection_name
        self.consumer = self.adapter.create_consumer(self.database_name)
        self.collection = self.adapter.get_collection(self.consumer, self.collection_name)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.adapter.close_consumer(self.consumer)
        #if exc_type is not None:
        #    # An exception occurred, handle it here
        #    logger.info(f"An exception of type {exc_type} occurred: {exc}")

    # New method to select fields
    @staticmethod
    def select_fields(include=None, exclude=None):
        if include and exclude:
            raise ValueError("Cannot include and exclude fields at the same time.")

        projection = {}

        if include:
            for field in include:
                projection[field] = 1
        elif exclude:
            for field in exclude:
                projection[field] = 0

        return projection

    # TODO: Improve and fix this method
    @staticmethod
    def select_with_regex(search_field, pattern):
        """
        seach_field: The field to search for for the pattern --> 'sitemap.SITEMAP_BRAND_LINKS'
        """
        projection ={
            f'{search_field}': {
                '$filter': {
                    'input': f'${search_field}',
                    'cond': {
                        '$regexMatch': {
                            'input': '$$this',
                            'regex': pattern
                }}}}}
        return projection

    # Asynchronous methods
    async def find(self, query=None, projection=None):
        if query is None:
            query = {}

        if projection is None:
            projection = {}


        cursor = await self.collection.find(query, projection)
        results = await cursor.to_list(length=None)
        return results

    async def find_one(self, query=None, projection=None):
        if query is None:
            query = {}

        if projection is None:
            projection = {}
        result = await self.collection.find_one(query, projection)
        return result

    async def insert_one(self, document):

        result = await self.collection.insert_one(document)
        return result

    async def insert_many(self, documents):
        result = await self.collection.insert_many(documents)
        return result

    async def update_one(self, query, update, upsert=False):
        result = await self.collection.update_one(query, update, upsert=upsert)
        return result

    async def update_many(self, query, update, upsert=False):
        result = await self.collection.update_many(query, update, upsert=upsert)
        return result

    async def delete_one(self, query):
        result = await self.collection.delete_one(query)
        return result

    async def delete_many(self, query):
        result = await self.collection.delete_many(query)
        return result

    async def count_documents(self, query):
        result = await self.collection.count_documents(query)
        return result

    async def create_index(self, keys: Union[str, List[Tuple[str, int]]], options: Optional[Dict[str, Any]] = None) -> str:
        if isinstance(keys, str):
            keys = [(keys, 1)]

        if options is None:
            options = {}

        return await self.collection.create_index(keys, **options)

    

