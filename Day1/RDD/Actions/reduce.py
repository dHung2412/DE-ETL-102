from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,19], 6)

def sum(v1:int, v2: int) -> int:
    print(f"v1: {v1}, v2: {v2} => ({v1 + v2})")
    return v1 + v2
print(numbers.glom().collect())
print(numbers.getNumPartitions())
print(numbers.reduce(sum))