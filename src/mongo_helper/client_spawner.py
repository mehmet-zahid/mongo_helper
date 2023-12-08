from motor.motor_asyncio import AsyncIOMotorClient
import uuid
from .helper import singleton
from loguru import logger


# Abstract class for mongo client consumers
class MongoClientConsumer:
    def __init__(self, id, async_motor_object, selected_database):
        self.id = id
        self.async_motor_object = async_motor_object
        self.metadata = {}
        self.selected_database = selected_database

    def get_id(self):
        return self.id

    def get_async_motor_object(self):
        return self.async_motor_object

    def consume(self):
        return self.async_motor_object.get_database(self.selected_database)


# Create async mongo clients and manage them
@singleton
class MongoClientSpawner:

    def __init__(self, is_remote_mode=False, mongo_uri=None):
        if not is_remote_mode:
            self.mongo_uri = 'mongodb://localhost:27017'
        else:
            if mongo_uri is None:
                raise ValueError('Mongo uri.')
            logger.info("Using remote mongo uri.")
            self.mongo_uri = mongo_uri

        self.clients = {}

    def create_id(self):
        return uuid.uuid4().hex

    def spawn_consumer(self, db_name) -> MongoClientConsumer:
        logger.info("Connecting to mongo...")
        try:
            client = AsyncIOMotorClient(self.mongo_uri)
            consumer = MongoClientConsumer(len(self.clients), client, db_name)
        except Exception as e:
            logger.info(e)
            raise ValueError('Could not connect to mongo.')
        logger.info("Connected to mongo.")
        self.clients[consumer.get_id()] = consumer
        return consumer

    def remove_spawned_client(self, consumer):
        consumer.get_async_motor_object().close
        self.clients.pop(consumer.get_id())

    def remove_all_spawned_clients(self):
        for consumer in self.clients.values():
            consumer.get_async_motor_object().close
        self.clients.clear()

    def client_count(self):
        return len(self.clients)




