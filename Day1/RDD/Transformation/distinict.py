from multiprocessing.reduction import duplicate

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

data = sc.parallelize(["one",2,1,24,521,1,"two","one"])

duplicateRdd = data.distinct()
print(duplicateRdd.collect())