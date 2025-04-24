import os
from dataclasses import dataclass
from typing import Dict
from dotenv import load_dotenv


@dataclass
class Database_config:
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"--------Missing config for {key}--------")



@dataclass
class MongoDBConfig(Database_config):
    uri : str
    db_name : str

@dataclass
class MySQLConfig(Database_config):
    host : str
    port : int
    user : str
    password : str
    # database : str

@dataclass
class RedisConfig(Database_config):
    host : str
    port : int
    user : str
    password : str
    database : str


def get_database_config()-> Dict[str,Database_config]:
    load_dotenv()
    config = {
        "mongodb": MongoDBConfig(
            uri = os.getenv("MONGO_URI"),
            db_name = os.getenv("MONGO_DB_NAME")
    ),
        "mysql": MySQLConfig(
            host = os.getenv("MYSQL_HOST"),
            port = int(os.getenv("MYSQL_PORT")),
            user = os.getenv("MYSQL_USER"),
            password = os.getenv("MYSQL_PASSWORD"),
            #database = os.getenv("MYSQL_DATABASE")
    ),
        "redis": RedisConfig(
            host = os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            user = os.getenv("REDIS_USER"),
            password = os.getenv("REDIS_PASSWORD"),
            database = os.getenv("REDIS_DB")
        )
    }

    for db , setting in config.items():
        setting.validate()
    return config

test = get_database_config()
# print(test)

