from pyspark.sql import SparkSession
from typing import Optional, List, Dict
import os
from Connect_database_config.config.database_config import get_database_config

class SparkConnect:
    def __init__(
            self,
            app_name:str,
            master_url: str = "local[*]",
            executor_memory: Optional[str] = "4g",
            executor_cores: Optional[int] = 2,
            driver_memory: Optional[str] = "2g",
            num_executor: Optional[int] = 3,
            jar_packages: Optional[List[str]] = None,
            spark_conf: Optional[Dict[str, str]] = None,
            log_level: str = "WARN"
    ):
        self.app_name = app_name
        self.spark = self.create_spark_session(master_url, executor_memory, executor_cores, driver_memory, num_executor, jar_packages, spark_conf)


    def create_spark_session(
            self,
            # app_name : str,
            master_url : str  = "local[*]",
            executor_memory : Optional[str] = "4g",
            executor_cores : Optional[int] = 2,
            driver_memory : Optional[str] = "2g",
            num_executor : Optional[int] = 3,
            jar_packages : Optional[List[str]] = None,
            spark_conf : Optional[Dict[str,str]] = None,
            log_level : str = "WARN"

    ) -> SparkSession:

        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)

        if executor_memory :
            builder.config("spark.executor.memory", executor_memory)
        if executor_cores :
            builder.config("spark.executor.cores",executor_cores)
        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)
        if num_executor:
            builder.config("spark.executor.instances", num_executor)

        # if jars:
        #     jars_path = ",".join([os.path.abspath(jar) for jar in jars])
        #     builder.config("spark.jars", jars_path)
        if jar_packages:
            jar_packages_url = ",".join([jar_package for jar_package in jar_packages])
            builder.config("spark.jars.packages", jar_packages_url)
        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)

        spark = builder.getOrCreate()

        # Phải chạy spark trước khi ghi log
        spark.sparkContext.setLogLevel(log_level)
        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("---------->>>Stop SparkSession")


def get_spark_config() -> Dict :
    db_config = get_database_config()

    return {
        "mysql" : {
            "table" : db_config["mysql"].table,
            "jdbc_url": "jdbc:mysql://{}:{}/{}".format(
                db_config["mysql"].host,db_config["mysql"].port,db_config["mysql"].database
            ),
            "config" : {
                "host" : db_config["mysql"].host,
                "port" : db_config["mysql"].port,
                "user": db_config["mysql"].user,
                "password": db_config["mysql"].password,
                "database": db_config["mysql"].database,
            }
        },
        "mongodb" : {
            "uri": db_config["mongodb"].uri,
            "database" : db_config["mongodb"].db_name,
            "collection" : db_config["mongodb"].collection

        },
        "redis" : {
        }
    }

# if __name__ == "__main__":
#     db_config = get_spark_config()
#     print(db_config)

