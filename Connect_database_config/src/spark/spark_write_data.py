from typing import Dict
from pyspark.sql.functions import lit, col
from pyspark.sql import SparkSession, DataFrame
from Connect_database_config.database.mysql_connect import MySQLConnect
from Connect_database_config.config.spark_config import get_spark_config

class SparkWriteDatabase:
    def __init__(self,spark:SparkSession, db_config:Dict):
        self.spark = spark
        self.db_config = db_config

    def spark_write_mysql(self, df_write: DataFrame, table_name:str, jdbc_url:str, config:Dict, mode:str = "append"):
        try:
            with MySQLConnect(config["host"], config["port"], config["user"], config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                print("---------->>Add column spark_temp to mysql")
                mysql_client.close()
        except Exception as e:
            raise Exception (f"---------->>>Fail to connect Mysql: {e}")

        df_write.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()
        print(f"---------->>>Wrote data to mysql in table: {table_name}")

    def validate_spark_mysql(self,df_write : DataFrame, table_name:str, jdbc_url:str, config:Dict, mode : str = "append"):

        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'sparkwrite') AS subq") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        # df_read.show()
        # print(f"----------- {df_read.count()}")
        def subtract_df(df_read: DataFrame, df_write: DataFrame):
            result = df_write.exceptAll(df_read)
            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .mode(mode) \
                    .save()


        if df_write.count() == df_read.count():
                print(f"---------->>Validate {df_read.count()} record success")
                subtract_df(df_read,df_write)
                print(f"---------->>Validated data of record success")
        else:
                subtract_df(df_read,df_write)
                print(f"---------->>Insert missing record by using spark")
            # Drop column spark_temp in Mysql
        try:
            with MySQLConnect(config["host"], config["port"], config["user"], config["password"]) as mysql_client:
                    connection, cursor = mysql_client.connection, mysql_client.cursor
                    database = "github_data"
                    connection.database = database
                    cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp")
                    connection.commit()
                    print("---------->>Drop column spark_temp in mysql")
                    mysql_client.close()
        except Exception as e:
            raise Exception (f"---------->>>Fail to connect Mysql: {e}")


    def spark_write_mongodb(self, df: DataFrame, uri: str, database: str, collection: str, mode: str = "append"):
        df.write \
            .format("mongo") \
            .option("uri", uri) \
            .option("database",database) \
            .option("collection",collection) \
            .mode(mode) \
            .save()

        # print(f"---------->>>Wrote data to MongoDB in collection: {collection} (database: {database})")

    def validate_spark_mongodb(self,df_write : DataFrame,uri: str, database: str, collection: str, mode: str = "append"):
        query = {"spark_temp":"sparkwrite"}

        df_read = self.spark.read \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .option("pipeline", str([{"$match": query}])) \
            .load()

        df_read = df_read.select(
            col('user_id')
            ,col('login')
            ,col('gravatar_id')
            ,col('avatar_url')
            ,col('url')
            ,col('spark_temp')
    )

        def subtract_df(df_spark_write: DataFrame, df_spark_read: DataFrame):
            result = df_write.exceptAll(df_read)
            # print(f"---------->>>Result record: {result.count()}")
            result.show()
            if not result.isEmpty():
                result.write \
                    .format("mongo") \
                    .option("uri", uri) \
                    .option("database", database) \
                    .option("collection", collection) \
                    .mode(mode) \
                    .save()

        # print(f"df_write: {df_write.count()} ---------- df_read: {df_read.count()}")

        if df_write.count() == df_read.count():
                print(f"---------->>Validate {df_read.count()} record success")
                subtract_df(df_read,df_write)
                print(f"---------->>Validated data of record success")
        else:
                subtract_df(df_read,df_write)
                print(f"---------->>Insert missing record by using spark")

        # Drop spark_temp in mongodb
        from Connect_database_config.database.mongodb_connect import MongoDBConnect
        from Connect_database_config.config.database_config import get_database_config
        # config = get_database_config()
        with MongoDBConnect(uri,database) as mongo_client:
            mongo_client.db.Users.update_many({}, {"$unset" : {"spark_temp": ""}})


    def write_all_database(self, df : DataFrame, mode : str = "append"):
        self.spark_write_mysql(
            df,
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        )

        self.spark_write_mongodb(
            df,
            self.db_config["mongodb"]["uri"],
            self.db_config["mongodb"]["database"],
            self.db_config["mongodb"]["collection"],
            mode
        )

        print(f"---------->>>Spark write success to all database")

    def validate_spark(self, df : DataFrame, mode : str = "append"):
        self.validate_spark_mysql(
            df,
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        )
        self.validate_spark_mongodb(
            df,
            self.db_config["mongodb"]["uri"],
            self.db_config["mongodb"]["database"],
            self.db_config["mongodb"]["collection"],
            mode
        )

        print(f"---------->>>Validate spark write success to all database")
