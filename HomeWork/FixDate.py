from pyspark.pandas import spark

data = [("11/12/2025",),("27/02.2014",),("2023.01.09",),("28-12-2005",)]
df = spark.createDataFrame(data , ["date"])
df_rdd = df.rdd.flatMap(lambda row: row)

def split_data(x):
    sep = ["/", ".", "-"]
    for i in sep:
        x = x.replace(i, "-")
    return x.split("-")

def final():
    parsed_data = df_rdd.map(split_data)
    def swap_if_need(x):
        if int(x[0]) > 31:
            return [x[2],x[1],x[0]]
        return x
    true_data = parsed_data.map(swap_if_need)
    return true_data.collect()

rdd_result = final()
df_result = spark.createDataFrame(rdd_result,
                                  ["day","month","year"])
df_result.show()