from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")
sc = SparkContext(conf=conf)

fileRDD = sc.textFile("D:\Project File\Pycharm\PythonProject4\Data\da1")

print(fileRDD.collect())