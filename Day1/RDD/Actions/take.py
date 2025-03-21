from pyspark import SparkContext

sc = SparkContext("local[*]", appName="DE-ETL-02")

data = [ 1,2,3,4,5,6,7,8]

rdd = sc.parallelize(data)

print(rdd.take(10))