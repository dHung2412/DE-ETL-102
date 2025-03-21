from pyspark import SparkContext

sc = SparkContext("local[*]", appName="DE-ETL-02")

data = [
    {"id": 1, "name": "Hieu"},
    {"id": 2, "name": "Hung"},
    {"id": 3, "name": "Dat"}

]

rdd = sc.parallelize(data)

print(rdd.count())