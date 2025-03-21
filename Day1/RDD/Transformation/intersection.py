from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([1,2,3,4,5, 8,6,67])
rdd2 = sc.parallelize([1,8,32,212,5,2,4,1])
rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())