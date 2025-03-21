from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf = conf)
data = sc.parallelize([(10,24.5),(20,54.6),(30,57.1),(40,63.1),(10,51.5),(20,04.6)])
# print(data.lookup(200 ))
print(dict(data.countByKey()))