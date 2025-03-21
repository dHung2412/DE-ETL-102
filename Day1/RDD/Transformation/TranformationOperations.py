from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

number = [1,2,4,24,5,124,5,6,342]

rdd = sc.parallelize(number)

#Biến đổi thành dữ liệu mới
squareRdd = rdd.map(lambda x: x*x)

filterRdd = rdd.filter(lambda x: x > 25)

flatmapRdd = rdd.flatMap(lambda x: [[x, x*2]])
print(flatmapRdd.collect())