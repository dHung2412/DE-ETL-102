from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("D:\Project File\Pycharm\PythonProject4\Data\da1")

wordRDD = fileRdd.flatMap(lambda line : line.split(" "))
print(wordRDD.collect())
for line in wordRDD.collect():
    print(line)