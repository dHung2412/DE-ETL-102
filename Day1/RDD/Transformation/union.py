from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([
    {"id": 1, "name": "Hieu"},
    {"id": 2, "name": "Hung"},
    {"id": 3, "name": "Dat"}
])

rdd2 = sc.parallelize([1,2,4,5,65,6])
rdd3 = rdd1.union(rdd2)
print(rdd3.collect())