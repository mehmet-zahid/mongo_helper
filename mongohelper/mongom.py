from loguru import logger
from os import getenv
from motor.motor_asyncio import (
    AsyncIOMotorClient, 
    AsyncIOMotorDatabase, 
    AsyncIOMotorCollection,
    AsyncIOMotorCursor
)
from motor.core import AgnosticCursor
from functools import wraps
from typing import Dict

def singleton(cls):
    instance = [None]
    def wrapper(*args, **kwargs):
        if instance[0] is None:
            instance[0] = cls(*args, **kwargs)
        return instance[0]
    return wrapper

def select_fields(include: list[str]=None, exclude: list[str]=None):
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

def without_init_protection(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if getattr(self, 'initialized', False):
            # Bypass initialization protection
            return func(self, *args, **kwargs)
        else:
            logger.warning("Attempted to call a method without initializing the MongoDB connection.")
            # You can choose to raise an exception, log a warning, or handle it in any other way
            raise Exception("Attempted to call a method without initializing the MongoDB connection.")
    
    return wrapper

@singleton
class Mongoom:
    POSSIBLE_CONN_STRINGS = ['MONGO_CONNECTION_STRING', 'MONGO_CONN_STR', 'MONGODB_URI', 'MONGO_URI', 'MONGO_URL']
    def __init__(self, connection_string: str = None) -> None:
        if not connection_string:
            logger.info("MongoDB connection string is not provided while initializing the class !")
        logger.info('init function block, lazy initializing method')
        self.initialized = None
        self.connection_string = connection_string
        logger.info(f"self.initialized :{self.initialized}")
        logger.info(f"self.connection_string :{self.connection_string}")
        
    
    async def test_connection(self, dbclient: AsyncIOMotorClient):
        try:
            
            if isinstance(dbclient, AsyncIOMotorClient):
                await dbclient.server_info()
                return True
        except Exception as e:
            logger.error(f"Failed to connect to the database with {str(e)}")
            return False
    
    async def init_mongoo(self, connection_string: str = None):
        if hasattr(self, "mongo_client"):
            logger.warning("There is already an instance of motor client object !")
            return False
        
        if connection_string:
            self.connection_string = connection_string
        elif not self.connection_string:
            logger.info("Searching for connection string in environment variables ...")
            for conn_str in self.POSSIBLE_CONN_STRINGS:
                if getenv(conn_str):
                    self.connection_string = getenv(conn_str)
                    logger.info(f"Found connection string in environment variables: {conn_str}")
                    break

        if not self.connection_string:
            logger.info("Could not found connection string in environment variables !")
            raise Exception("Please provide Connection String via 'init_mongo()' method or env varibles !")
            
        dbclient = AsyncIOMotorClient(self.connection_string)
        conn_verif = await self.test_connection(dbclient)
        if conn_verif:   
            logger.success("Connection to MongoDB established successfully!")
            self.initialized = True
            self.motor_client = dbclient
            logger.success(f"self.initialized :{self.initialized}")
            return True
        raise Exception("Failed to connect to the database!")  
    
    @without_init_protection
    def get_db(self, db_name: str) -> AsyncIOMotorDatabase:
        return self.motor_client[db_name]
    
    @without_init_protection
    def get_client(self) -> AsyncIOMotorClient:
        return self.motor_client
    
    @without_init_protection
    def get_collection(self, db_name: str, collection_name: str) -> AsyncIOMotorCollection:
        return self.motor_client[db_name][collection_name]
    
    def close_connection(self):
        if hasattr(self, "motor_client"):
            self.motor_client.close()
            logger.info("Async connection closed successfully!")
            return True
        logger.info("No client object")
        return False
    
    @staticmethod
    async def is_collection_exists(db: AsyncIOMotorDatabase, collection_name: str):
        collection_names = await db.list_collection_names()
        if collection_name in collection_names:
            return True
        return False

    @staticmethod
    async def get_all_collections(db: AsyncIOMotorDatabase) -> list[AsyncIOMotorCollection]:
        collection_cursor = await db.list_collections()
        collections = []
        for collection in collection_cursor:
            logger.info(f"Collection Name: {collection['name']}")
            collections.append(collection)
        return collections
    
    @staticmethod
    async def search_for_field_in_collection(
        self,
        collection: AsyncIOMotorCollection, 
        fields: list[str], 
        limit=0,
        as_list: bool = True
        ) -> list | AgnosticCursor:
        
        query = {field: {'$exists': True} for field in fields}
        field_dict = {field: 1 for field in fields}
        projection = {'_id': 1, **field_dict}
        cursor = collection.find(query, projection=projection).limit(limit)
        if as_list:
            return [document async for document in cursor]
        return cursor
    
    @staticmethod
    async def query_with_projection(
        collection: AsyncIOMotorCollection,
        query: Dict,
        include_fields: list[str] = None,
        exclude_fields: list[str] = None,
        limit: int = 0,
        as_list: bool = True
        ) -> list | AgnosticCursor:

        if include_fields and exclude_fields:
            raise ValueError("Cannot include and exclude fields at the same time.")
        if include_fields:
            projection = select_fields(include=include_fields)
        elif exclude_fields:
            projection = select_fields(exclude=exclude_fields)
        else:
            projection = None
        cursor = collection.find(query, projection=projection).limit(limit)
        if as_list:
            return [document async for document in cursor]
        return cursor
    
    @staticmethod
    async def filter_by_properties(
        collection: AsyncIOMotorCollection,
        properties: dict,
        limit: int = 0,
        as_list: bool = True
        ) -> list | AgnosticCursor:
        query = {key: value for key, value in properties.items()}
        cursor = collection.find(query).limit(limit)
        if as_list:
            return [document async for document in cursor]
        return cursor
    
    @staticmethod
    async def sort_documents(
        cursor: AsyncIOMotorCursor,
        sort_field: str,
        ascending: bool = True,
        as_list: bool = True
        ) -> list | AsyncIOMotorCursor:

        sort_order = 1 if ascending else -1
        cursor.sort(sort_field, sort_order)
        if as_list:
            return [document async for document in cursor]
        return cursor
    
    @staticmethod
    async def update_documents(
        collection: AsyncIOMotorCollection,
        filter_query: dict,
        update_query: dict
        ) -> int:

        result = await collection.update_many(filter_query, {'$set': update_query})
        return result.modified_count
    
    @staticmethod
    async def delete_documents(
        collection: AsyncIOMotorCollection,
        filter_query: dict
        ) -> int:

        result = await collection.delete_many(filter_query)
        return result.deleted_count
    
    @staticmethod
    async def get_dublicates(collection: AsyncIOMotorCollection, field: str):
        pipeline = [
            { 
                "$group": { 
                    "_id": f"${field}",
                    "count": { "$sum": 1 }
                }
            },
            {    
                "$match": { 
                    "count": { "$gt": 1 } 
                    }
            },
            {
                "$project": {
                    f"{field}": "$_id",
                    "_id": 0
                }
            }
                ]
        dups = []
        async for doc in collection.aggregate(pipeline):
            dups.append(doc)
        return dups
    
    @staticmethod
    async def remove_dublicates(collection: AsyncIOMotorCollection, field: str):
        pipeline = [
            { 
                "$group": { 
                    "_id": {f"{field}": f"${field}"},
                    "count": { "$sum": 1 },
                    "dups": { "$push": "$_id" }
                }
            },
            {    
                "$match": { 
                    "count": { "$gt": 1 } 
                    }
            }]
        del_count = 0
        #distinct_count = len(await collection.distinct('productId'))
        async for doc in collection.aggregate(pipeline):
            doc['dups'].pop(0)  # keep the first document, delete the rest
            dr = await collection.delete_many({ '_id': { '$in': doc['dups'] } }) 
            del_count += dr.deleted_count
        return del_count


mongoo_instance = Mongoom()
        