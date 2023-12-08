from __future__ import annotations
from .client_spawner import MongoClientSpawner, MongoClientConsumer
from enum import Enum
import os
from motor.motor_asyncio import AsyncIOMotorCollection
import uuid
from .helper import singleton
from loguru import logger



class Operations(Enum):
    INSERT_ONE = 1
    UPDATE_ONE = 2
    DELETE_ONE = 3
    INSERT_MANY = 4
    UPDATE_MANY = 5
    DELETE_MANY = 6
    FIND_ONE = 7
    FIND_MANY = 8

@singleton
class MongoClientAdapter:
    class Collection:
        def __init__(self, collection: AsyncIOMotorCollection):
            self.collection = collection

        async def update_one(self, filter=None, update=None, upsert=False, session=None):
            return await self.collection.update_one(filter, update, upsert=upsert, session=session)

        async def insert_one(self, document, session=None):
            return await self.collection.insert_one(document, session=session)

        async def delete_one(self, filter, session=None):
            return await self.collection.delete_one(filter, session=session)

        async def insert_many(self, documents, session=None):
            return await self.collection.insert_many(documents, session=session)

        async def find_one(self, query, projection=None, session=None):
            return await self.collection.find_one(query, projection, session=session)

        async def find(self, query, projection=None, session=None):
            return self.collection.find(query, projection, session=session)

        # Add the update_many method
        async def update_many(self, query, update, upsert=False, session=None):
            return await self.collection.update_many(query, update, upsert=upsert, session=session)

        # Add the delete_many method
        async def delete_many(self, query, session=None):
            return await self.collection.delete_many(query, session=session)

        # Add the count_documents method
        async def count_documents(self, query, session=None):
            return await self.collection.count_documents(query, session=session)

        # Add the create_index method
        async def create_index(self, keys, options=None, session=None):
            if options is None:
                options = {}
            return await self.collection.create_index(keys, **options, session=session)

    class TransactionResult:
        def __init__(self, operation, result, id):
            self.operation = operation
            self.result = result
            self.id = id

    class TransactionObject:
        def __init__(self,
                     collection_name: str,
                     operation,
                     document,
                     collection,
                     query=None,
                     update=None,
                     documents=None,
                     projection=None):

            self.collection_name = collection_name
            self.collection = collection
            self.operation = operation
            self.query = query
            self.update = update
            self.documents = documents
            self.projection = projection
            self.id = uuid.uuid4().hex

        def get_operation_type(self):
            return self.operation

        async def commit(self, session=None):
            if session is None:
                if self.operation == Operations.INSERT_ONE:
                    return await self.collection.insert_one(self.documents)
                elif self.operation == Operations.UPDATE_ONE:
                    return await self.collection.update_one(self.query, self.update)
                elif self.operation == Operations.DELETE_ONE:
                    return await self.collection.delete_one(self.query)
                elif self.operation == Operations.INSERT_MANY:
                    return await self.collection.insert_many(self.documents)
                elif self.operation == Operations.UPDATE_MANY:
                    return await self.collection.update_many(self.query, self.update)
                elif self.operation == Operations.DELETE_MANY:
                    return await self.collection.delete_many(self.query)
                elif self.operation == Operations.FIND_ONE:
                    return await self.collection.find_one(self.query, self.projection)
                elif self.operation == Operations.FIND_MANY:
                    cursor = await self.collection.find(self.query, self.projection)
                    return await cursor.to_list(length=None)
                else:
                    raise ValueError('Invalid operation.')
            else:
                if self.operation == Operations.INSERT_ONE:
                    return await self.collection.insert_one(self.documents, session=session)
                elif self.operation == Operations.UPDATE_ONE:
                    return await self.collection.update_one(self.query, self.update, session=session)
                elif self.operation == Operations.DELETE_ONE:
                    return await self.collection.delete_one(self.query, session=session)
                elif self.operation == Operations.INSERT_MANY:
                    return await self.collection.insert_many(self.documents, session=session)
                elif self.operation == Operations.UPDATE_MANY:
                    return await self.collection.update_many(self.query, self.update, session=session)
                elif self.operation == Operations.DELETE_MANY:
                    return await self.collection.delete_many(self.query, session=session)
                elif self.operation == Operations.FIND_ONE:
                    return await self.collection.find_one(self.query, self.projection, session=session)
                elif self.operation == Operations.FIND_MANY:
                    cursor = await self.collection.find(self.query, self.projection, session=session)
                    return await cursor.to_list(length=None)
                else:
                    raise ValueError('Invalid operation.')
            if session is None:
                if self.operation == Operations.INSERT_ONE:
                    return await self.collection.insert_one(self.document)
                elif self.operation == Operations.UPDATE_ONE:
                    return await self.collection.update_one(self.document)
                elif self.operation == Operations.DELETE_ONE:
                    return await self.collection.delete_one(self.document)
                elif self.operation == Operations.INSERT_MANY:
                    return await self.collection.insert_many(self.document)
                elif self.operation == Operations.UPDATE_MANY:
                    return await self.collection.update_many(self.document)
                elif self.operation == Operations.DELETE_MANY:
                    return await self.collection.delete_many(self.document)
                elif self.operation == Operations.FIND_ONE:
                    return await self.collection.find_one(self.document)
                elif self.operation == Operations.FIND_MANY:
                    return await self.collection.find(self.document)
                else:
                    raise ValueError('Invalid operation.')
            else:
                if self.operation == Operations.INSERT_ONE:
                    return await self.collection.insert_one(self.document, session=session)
                elif self.operation == Operations.UPDATE_ONE:
                    return await self.collection.update_one(self.document, session=session)
                elif self.operation == Operations.DELETE_ONE:
                    return await self.collection.delete_one(self.document, session=session)
                elif self.operation == Operations.INSERT_MANY:
                    return await self.collection.insert_many(self.document, session=session)
                elif self.operation == Operations.UPDATE_MANY:
                    return await self.collection.update_many(self.document, session=session)
                elif self.operation == Operations.DELETE_MANY:
                    return await self.collection.delete_many(self.document, session=session)
                elif self.operation == Operations.FIND_ONE:
                    return await self.collection.find_one(self.document, session=session)
                elif self.operation == Operations.FIND_MANY:
                    return await self.collection.find(self.document, session=session)
                else:
                    raise ValueError('Invalid operation.')


    def __init__(self, *args, **kwargs):
        logger.info('Initializing MongoClientSpawner.')
        self.client_spawner = MongoClientSpawner(*args, **kwargs)
        self.collection_cache = {}

    def __call__(self, *args, **kwargs):
        raise TypeError('Singletons must be accessed through `get_instance()`.')

    def create_consumer(self, db_name=None) -> MongoClientConsumer:
        selected_db_name = None

        if db_name:
            selected_db_name = db_name
        else:
            default_db_name = os.getenv('MONGO_DB_NAME')
            selected_db_name = default_db_name

        return self.client_spawner.spawn_consumer(selected_db_name)

    def close_consumer(self, consumer):
        self.client_spawner.remove_spawned_client(consumer)

    def get_collection(self, consumer, collection_name):
        db = consumer.consume()
        if collection_name not in self.collection_cache:
            self.collection_cache[collection_name] = self.Collection(db[collection_name])
        return self.collection_cache[collection_name]

    async def begin_transaction(self, consumer, collection_name, operation_list) -> list[TransactionResult]:
        is_replica_set = os.getenv('MONGO_REPLICA_SET')

        results = []
        if is_replica_set is None or is_replica_set.lower() == 'false':
            for operation in operation_list:
                collection = self.get_collection(consumer, operation.collection_name)
                result = await operation.commit(collection.collection)
                results.append(self.TransactionResult(operation, result, operation.id))
        else:
            async_client = consumer.get_async_motor_object()
            async with await async_client.start_session() as session:
                async with session.start_transaction():
                    for operation in operation_list:
                        collection = self.get_collection(consumer, operation.collection_name)
                        result = await operation.commit(collection.collection, session)
                        results.append(self.TransactionResult(operation, result, operation.id))
                # Commit the transaction
                await session.commit_transaction()

        return results

    def get_transaction_object(self, consumer, collection_name, operation, query=None, update=None, documents=None, projection=None):
        return MongoClientAdapter.TransactionObject(
            collection_name,
            operation,
            self.get_collection(consumer, collection_name),
            query=query,
            update=update,
            documents=documents,
            projection=projection
        )




