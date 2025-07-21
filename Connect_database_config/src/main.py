from Connect_database_config.database.mongodb_connect import MongoDBConnect
from Connect_database_config.database.mysql_connect import MySQLConnect
from Connect_database_config.config.database_config import get_database_config
from Connect_database_config.src.schema_manager import create_mongodb_schema, validate_mongodb_schema
from Connect_database_config.src.schema_manager import create_mySQL_schema, validate_mysql_schema


def main(config):
    # MongoDB
    with MongoDBConnect(config["mongodb"].uri,
                        config["mongodb"].db_name) as mongo_client:
        create_mongodb_schema(mongo_client.connect())
        print("---------->>>Inserted to MongoDB")
        mongo_client.db.Users.insert_one({
            "user_id" : 1,
            "login": "GoogleCodeExporter",
            "gravatar_id" :"",
            "url": "https://api.github.com/users/GoogleCodeExporter",
            "avatar_url": "https://avatars.githubusercontent.com/u/9614759?"
        })
        validate_mongodb_schema(mongo_client.connect())

        #MYSQL
    with MySQLConnect(config["mysql"].host,
                      config["mysql"].port,
                      config["mysql"].user,
                      config["mysql"].password) as mySql_client:
        connection, cursor = mySql_client.connection, mySql_client.cursor
        create_mySQL_schema(connection, cursor)
        cursor.execute("INSERT INTO Users (user_id,""logiN,""gravatar_id,""avatar_url,""url)"
                                           " VALUES (%s, %s, %s, %s, %s)",
                                          (1,"GoogleCodeExporter","","https://api.github.com/users/GoogleCodeExporter","https://avatars.githubusercontent.com/u/9614759?" ))
        connection.commit()
        print("---------->>>Inserted data to Mysql")
        validate_mysql_schema(cursor)

    # #REDIS
    # with RedisConnect(config["redis"].host,
    #                   config["redis"].port,
    #                   config["redis"].user,
    #                   config["redis"].password,
    #                   config["redis"].database) as redis_client:
    #     create_redis_schema(redis_client.connect())
    #     validated_redis_schema(redis_client.connect())
    #     print("---------->>>Inserted data to Redis")

if __name__ == "__main__":
    config = get_database_config()
    main(config)
