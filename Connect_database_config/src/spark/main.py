from Connect_database_config.config.database_config import get_database_config
from Connect_database_config.config.spark_config import SparkConnect
from pyspark.sql.types import *
from pyspark.sql.functions import col
from Connect_database_config.src.spark.spark_write_data import SparkWriteDatabase


file_jars = [
    r"D:\Project File\Pycharm\DE-ETL-102\Connect_database_config\lib\mysql-connector-j-9.2.0.jar"
]

spark_conf ={
    "spark.jar.package":(
        "mysql:mysql-connector-java-9.2.0"
    )
}


def main():
    db_config = get_database_config()

    spark_write_database = SparkConnect(
        app_name = 'hungbo',
        master_url = "local[*]",
        executor_memory = "2g",
        executor_cores = 1,
        driver_memory = "1g",
        num_executor = 1,
        jars = file_jars,
        spark_conf = spark_conf,
        log_level = "INFO"
    ).spark

    schema =StructType([
        StructField( 'actor', StructType([
            StructField( 'id', LongType(), False),
            StructField('login', StringType(),  True),
            StructField('gravatar_id', StringType(), True),
            StructField( 'url', StringType(), True),
            StructField( 'avatar_url', StringType(), True),
        ]), True),
        StructField( 'repo', StructType([
            StructField('id', LongType(), False),
            StructField( 'name', StringType(), True),
            StructField('url', StringType(), True),
        ]),True)
    ])


    df = spark_write_database.read.schema(schema).json(r"D:\Project File\Pycharm\DE-ETL-102\Connect_database_config\data\2015-03-01-17.json")
    # df.show()

    df_write_table_Users = df.select(
        col('actor.id').alias('user_id')
            ,col('actor.login').alias('login')
            ,col('actor.gravatar_id').alias('gravatar_id')
            ,col('actor.avatar_url').alias('avatar_url')
            ,col('actor.url').alias('url')
    )
    df_write_table_Repositories = df.select(
        col('repo.id').alias('repo_id')
            ,col('repo.name').alias('name')
            ,col('repo.url').alias('url')
    )

    df_write= SparkWriteDatabase(spark_write_database, db_config)
    df_write.spark_write_mysql(df_write_table_Users,"Users",mode="append")

if __name__ == "__main__":
    main()

