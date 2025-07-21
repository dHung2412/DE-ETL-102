from Connect_database_config.config.spark_config import SparkConnect
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit
from Connect_database_config.src.spark.spark_write_data import SparkWriteDatabase
from Connect_database_config.config.spark_config import get_spark_config


def main():

    package = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    ]

    spark_connect = SparkConnect(
        app_name = 'hungbo',
        master_url = "local[*]",
        executor_memory = "2g",
        executor_cores = 2,
        driver_memory = "2g",
        num_executor = 2,
        jar_packages = package,
        # spark_conf = spark_conf,
        log_level = "INFO"
    ).spark

    schema = StructType([
        StructField( 'actor', StructType([
            StructField( 'id', IntegerType(), False),
            StructField('login', StringType(),  True),
            StructField('gravatar_id', StringType(), True),
            StructField( 'url', StringType(), True),
            StructField( 'avatar_url', StringType(), True),
        ]), True),
        StructField( 'repo', StructType([
            StructField('id', LongType(), False),
            StructField( 'name', StringType(), True),
            StructField('url', StringType(), True)
        ]),True)
    ])


    df = spark_connect.read.schema(schema).json(r"D:\Project File\Pycharm\DE-ETL-102\Connect_database_config\data\2015-03-01-17.json")

    df_write_table = df.withColumn("spark_temp", lit("sparkwrite")).select(
        col('actor.id').alias('user_id')
            ,col('actor.login').alias('login')
            ,col('actor.gravatar_id').alias('gravatar_id')
            ,col('actor.avatar_url').alias('avatar_url')
            ,col('actor.url').alias('url')
            ,col('spark_temp').alias("spark_temp")
    )
    # df_write_table.show()

    # df_write_table_Repositories = df.select(
    #     col('repo.id').alias('repo_id')
    #         ,col('repo.name').alias('name')
    #         ,col('repo.url').alias('url')
    # )
    # df_write_table.printSchema()

    spark_config = get_spark_config()
    df_write = SparkWriteDatabase(spark_connect, spark_config)
    df_write.write_all_database(df_write_table, mode="append")

    df_validate = SparkWriteDatabase(spark_connect, spark_config)
    # df_validate.validate_spark_mysql(df_write_table,db_configs["mysql"]["table"], db_configs["mysql"]["jdbc_url"],db_configs["mysql"]["config"])
    # df_validate.validate_spark_mongodb(df_write_table, db_configs["mongodb"]["uri"],db_configs["mongodb"]["database"],db_configs["mongodb"]["collection"])
    df_validate.validate_spark(df_write_table, mode="append")

    print("---------->>>Read data success")

    spark_connect.stop()

if __name__ == "__main__":
    main()

