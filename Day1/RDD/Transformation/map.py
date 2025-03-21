from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("D:\Project File\Pycharm\PythonProject4\Data\da1")

allCapRdd = fileRdd.map(lambda line : line.upper())

for line in allCapRdd.collect():
    print(line)

#print(allCapRdd.collect())
