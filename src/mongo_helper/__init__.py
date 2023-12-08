from __future__ import annotations

__version__ = "0.0.2"

from .client_adapter import MongoClientAdapter, Operations
from .query_builder import AsyncQueryBuilder
from .query_builder_pydantic import AsyncPydanticQueryBuilder

__all__ = [
    "MongoClientAdapter",
    "Operations",
    "AsyncQueryBuilder",
]



"""
from client_adapter import MongoClientAdapter, Operations
import asyncio
from utils import metric
from query_builder import AsyncQueryBuilder

adapter = MongoClientAdapter.get_instance()
database_name = "trendyol"
collection_name = "test"

# @metric
async def query_builder():

    async with AsyncQueryBuilder(adapter, database_name, collection_name) as query_builder:
        # Insert a document
        document = {
            "name": "John",
            "age": 30,
            "city": "New York"
        }
        insert_result = await query_builder.insert_one(document)
        print(f"Inserted document with id: {insert_result.inserted_id}")
        await asyncio.sleep(10)

        # Insert multiple documents
        documents = [
            {
                "name": "Jane",
                "age": 25,
                "city": "Boston"
            },
            {
                "name": "Jack",
                "age": 40,
                "city": "Miami"
            },
            { 'links': [
            'https://www.trendyol.com/mavi/erkek-ayakkabi-ve-ayakkab-canta/',
            'https://www.trendyol.com/lc-waikiki/erkek-ayakkabi'
            ]
            }
        ]
        insert_result = await query_builder.insert_many(documents)
        print(f"Inserted {len(insert_result.inserted_ids)} documents")
        await asyncio.sleep(5)

        # Find a document
        query = query_builder.eq("name", "John")
        found_document = await query_builder.find_one(query)
        print(f"Found document: {found_document}")
        await asyncio.sleep(5)

        # Update a document
        update_query = query_builder.eq("name", "John")
        update_data = {"$set": {"age": 31}}
        update_result = await query_builder.update_one(update_query, update_data)
        print(f"Updated {update_result.modified_count} documents")
        await asyncio.sleep(5)

        # Delete a document
        delete_query = query_builder.eq("name", "John")
        delete_result = await query_builder.delete_one(delete_query)
        print(f"Deleted {delete_result.deleted_count} documents")
        await asyncio.sleep(5)

        # Include only the "name" and "age" fields
        projection = query_builder.select_fields(include=["name", "age"])
        documents = await query_builder.find(projection=projection)
        print(documents)
        await asyncio.sleep(5)

        # Exclude the "_id" field
        projection = query_builder.select_fields(exclude=["_id"])
        documents = await query_builder.find(projection=projection)
        print(documents)
        await asyncio.sleep(5)
        projection = query_builder.select_with_regex("links", "mavi")
        documents = await query_builder.find(projection=projection)
        print(documents)

async def operation_list():
    # Bir MongoDB istemcisi oluşturun
    consumer = adapter.create_consumer(db_name="test_db")

    # Koleksiyonları seçin
    users = adapter.get_collection(consumer, "users")

    # Yeni bir kullanıcı ekleme işlemi oluşturun
    new_user = {
        "name": "John Doe",
        "email": "john.doe@example.com",
        "age": 30,
    }
    insert_user_operation = adapter.get_transaction_object(
        consumer, "users", Operations.INSERT_ONE, documents=new_user
    )

    # Belirli bir kullanıcının yaşını güncellemek için işlem oluşturun
    update_user_operation = adapter.get_transaction_object(
        consumer,
        "users",
        Operations.UPDATE_ONE,
        query={"email": "john.doe@example.com"},
        update={"$set": {"age": 31}},
    )

    # İşlemleri gerçekleştirmek için begin_transaction metodunu kullanın
    operation_list = [insert_user_operation, update_user_operation]
    results = await adapter.begin_transaction(consumer, "users", operation_list)

    # İşlem sonuçlarını yazdırın
    for result in results:
        print(f"Operation {result.id} ({result.operation}): {result.result}")

    # Kullanıcıyı bulmak için işlem oluşturun ve gerçekleştirin
    find_user_operation = adapter.get_transaction_object(
        consumer,
        "users",
        Operations.FIND_ONE,
        query={"email": "john.doe@example.com"},
    )
    user = await find_user_operation.commit()
    print(f"Found user: {user}")

    # İstemciyi kapatın
    adapter.close_consumer(consumer)


if __name__ == '__main__':
    asyncio.run(query_builder())

"""

""" Pydantic version of QueryBuilder
from pydantic import BaseModel
from typing import Type, TypeVar, List, Optional
from client_adapter import MongoClientAdapter, Operations
import asyncio
from query_builder_pydantic import AsyncPydanticQueryBuilder
from operators import Operators

class User(BaseModel):
    id: Optional[str]
    name: str
    age: int
    email: str

adapter = MongoClientAdapter.get_instance()
database_name = "trendyol"
collection_name = "test"

async def pydantic_query_builder():
    async with AsyncPydanticQueryBuilder(adapter, database_name, "users", User) as query_builder:
        new_user = User(name="John Doe", age=30, email="john.doe@example.com")
        inserted_id = await query_builder.insert_one(new_user)
        print(f"Inserted user with ID: {inserted_id}")

        found_user = await query_builder.find_one(Operators.eq("name", "John Doe"))
        if found_user:
            print(f"Found user: {found_user.name}")

        modified_count = await query_builder.update_one(Operators.eq("name", "John Doe"), Operators.set("age", 31))
        print(f"Updated {modified_count} user(s)")

        deleted_count = await query_builder.delete_one(Operators.eq("name", "John Doe"))
        print(f"Deleted {deleted_count} user(s)")

if __name__ == '__main__':
    asyncio.run(pydantic_query_builder())
"""

""" Operator examples

from client_adapter import MongoClientAdapter, Operations
import asyncio
from query_builder import AsyncQueryBuilder
from operators import Operators

class User(BaseModel):
    id: Optional[str]
    name: str
    age: int
    email: str

adapter = MongoClientAdapter.get_instance()
database_name = "trendyol"
collection_name = "test"

async def query_builder():
    async with AsyncQueryBuilder(adapter, database_name, collection_name) as query_builder:
        # Insert a document
        new_user = User(name="John Doe", age=30)
        inserted_id = await query_builder.insert_one(new_user)
        print(f"Inserted user with ID: {inserted_id}")
        await asyncio.sleep(5)

        # Find a document
        query = Operators.eq("name", "John Doe")
        found_user = await query_builder.find_one(query)
        if found_user:
            print(f"Found user: {found_user.name}")

        # Update a document
        update_query = Operators.eq("name", "John Doe")
        update_data = Operators.set("age", 31)
        update_result = await query_builder.update_one(update_query, update_data)
        print(f"Updated {update_result.modified_count} documents")

        # Delete a document
        delete_query = Operators.eq("name", "John Doe")
        delete_result = await query_builder.delete_one(delete_query)
        print(f"Deleted {delete_result.deleted_count} documents")


"""

""" QueryBuilder index creation

async with AsyncPydanticQueryBuilder(adapter, db_name, "users", User) as query_builder:
    # Tek bir alan için indeks oluşturma
    index_name1 = await query_builder.create_index("name")

    # Birden fazla alan için indeks oluşturma
    index_name2 = await query_builder.create_index([("age", 1), ("role", -1)])

    # Çoklu indeks oluşturma
    index_names = await query_builder.create_multi_indexes([
        (("name", 1), {}),
        (([("age", 1), ("role", -1)], {}),
    ])
"""


""" Pydantic QueryBuilder index creation

async with AsyncPydanticQueryBuilder(adapter, db_name, "users", User) as query_builder:
    # Tek bir alan için indeks oluşturma
    index_name1 = await query_builder.create_index("name")

    # Birden fazla alan için indeks oluşturma
    index_name2 = await query_builder.create_index([("age", 1), ("role", -1)])

    # Çoklu indeks oluşturma
    index_names = await query_builder.create_multi_indexes([
        (("name", 1), {}),
        (([("age", 1), ("role", -1)], {}),
    ])
"""