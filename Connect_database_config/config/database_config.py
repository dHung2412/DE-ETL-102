import os
from dataclasses import dataclass
from typing import Dict, Optional
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
    jar_path : Optional[str] = None
    collection : str = "Users"

@dataclass
class MySQLConfig(Database_config):
    host : str
    port : int
    user : str
    password : str
    database : str
    jar_path : Optional[str] = None
    table : str = "Users"

@dataclass
class RedisConfig(Database_config):
    host : str
    port : int
    user : str
    password : str
    database : str
    jar_path : Optional[str] = None
    key_column : str = "id"

def get_database_config()-> Dict[str,Database_config]:
    load_dotenv()
    config = {
        "mongodb": MongoDBConfig(
            uri = os.getenv("MONGO_URI"),
            db_name = os.getenv("MONGO_DB_NAME"),
            jar_path = os.getenv("MONGO_PACKAGE_PATH")
    ),
        "mysql": MySQLConfig(
            host = os.getenv("MYSQL_HOST"),
            port = int(os.getenv("MYSQL_PORT")),
            user = os.getenv("MYSQL_USER"),
            password = os.getenv("MYSQL_PASSWORD"),
            jar_path = os.getenv("MYSQL_JAR_PATH"),
            database = os.getenv("MYSQL_DATABASE")
        ),
        "redis": RedisConfig(
            host = os.getenv("REDIS_HOST"),
            port=int(os.getenv("REDIS_PORT")),
            user = os.getenv("REDIS_USER"),
            password = os.getenv("REDIS_PASSWORD"),
            database = os.getenv("REDIS_DB"),
            jar_path = os.getenv("REDIS_JAR_PATH")
        )
    }

    for db , setting in config.items():
        setting.validate()
    return config

test = get_database_config()
# print(test)

