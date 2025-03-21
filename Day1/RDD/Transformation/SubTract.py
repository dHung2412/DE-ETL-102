from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-02").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

word = sc.parallelize(["Lam trai cho dang nen Treai pHu XuaN dD tDai Dong nai da tung"]) \
    .flatMap(lambda w : w.split(" ")) \
    .map(lambda x : x.lower())
stopWords = sc.parallelize(["lam trai cho dang"]) \
    .flatMap(lambda w: w.split(" "))

finalWord = word.subtract(stopWords)
print(finalWord.collect())
