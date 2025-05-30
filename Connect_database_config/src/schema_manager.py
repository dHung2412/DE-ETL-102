from enum import unique
from pathlib import Path
from mysql.connector import Error

def create_mongodb_schema(db):
    db.drop_collection("Users")
    db.create_collection("Users", validator = {
        "$jsonSchema":{
            "bsonType":"object",
            "required":["user_id","login"],
            "properties":{
                "user_id": {
                    "bsonType":"int",
                },
                "login":{
                    "bsonType":"string"
                },
                "gravatar": {
                    "bsonType": ["string","null"]
                },
                "avatar_url": {
                    "bsonType": ["string","null"]
                },
                "url": {
                    "bsonType": ["string","null"]
                },
            }
        }
    })

    # db.Users.create_index("user_id", unique = True)

def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    # print(collections)
    if "Users" not in collections:
        raise ValueError(f"---------->>>Mins    ing collections in MongoDB")
    user = db.Users.find_one({"user_id":1})
    if not user:
        raise ValueError(f"---------->>>User id not found in MongoDB")
    print("---------->>>Validated Schema in MongoDB")


SQL_FILE_PATH = Path("D:/Project File/Pycharm/DE-ETL-102/Connect_database_config/sql/schema.sql")
def create_mySQL_schema(connection, cursor):
    database = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE `{database}`")
    print(f"---------->>>CREATED DATABASE  {database} IN MYSQL")
    connection.database = database
    try:
        with open(SQL_FILE_PATH, "r") as file:
            sql_script = file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"---------->>>Executed Mysql commands")
            connection.commit()
            print(f"---------->>>Created Mysql Schema")
    except Error as e:
        connection.rollback()
        raise Exception(f"---------->>>Failed to create Mysql Schema: {e}") from e

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    # print(cursor.fetchall())
    tables = [row[0] for row in cursor.fetchall()]
    if "Users" not in tables or "Repositories" not in tables:
        raise ValueError(f"---------->>>Table doesn't exist")
    cursor.execute("SELECT * FROM Users WHERE user_id = 1")
    # print(cursor.fetchone())
    user = cursor.fetchone()
    if not user:
        raise ValueError(f"---------->>>User not found")

    print("---------->>>Validated schema in MySQL")

def create_redis_schema(client):
    try:
        # client.flushdb() # drop database
        client.set("user:1:login","GoogleCodeExporter")
        client.set("user:1:gravatar_id","")
        client.set("user:1:url", "https://api.github.com/users/GoogleCodeExporter")
        client.set("user:1:avatar_url", "https://avatars.githubusercontent.com/u/9614759?")
        client.sadd("user_id","user:1")
        print("---------->>>Add data to Redis successfully")
    except Exception as e:
        raise Exception(f"---------->>>Failed to add data to Redis: {e}") from e

def validated_redis_schema(client):
    if not client.get("user:1:login") == "GoogleCodeExporter":
        raise ValueError("---------->>>Value login not found in Redis")

    if not client.sismember("user_id", "user:1"):
        raise ValueError("---------->>>User not set in Redis")

    print("---------->>>Validated schema in Redis")