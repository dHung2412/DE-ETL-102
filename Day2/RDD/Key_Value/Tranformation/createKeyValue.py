from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
sc = SparkContext(conf = conf)

data = sc.parallelize([(10,24.5),(20,54.6),(30,57.1),(40,63.1),(10,51.5),(20,04.6)])
data1 = sc.parallelize([(10,"a"),(20,"b"),(30,"c"),(40,"a"),(10,"d")])

# JOIN giữa 2 RDD vs nhau, dạng key-value
join = data.join(data1).sortByKey(ascending=False) # False: Giảm dần, True: Tăng dần
for x in join.collect():
    print(x)
