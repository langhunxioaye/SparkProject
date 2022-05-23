from pyspark import SparkContext

sc = SparkContext()

rdd = sc.parallelize([1,2,3,4])

# 使用分区的计算方法

# 定义一个迭代器
def func(data):  # data接受的是一个分区的数据  [1,2]
    for i in data:  # 遍历分区数据进行计算
        yield i+1      # yield每次返回一个计算数据

# mapPartitions 需要接受一个迭代器  在迭代器内进行计算
# mapPartitions是一个转化方法
rdd_mapPartitions = rdd.mapPartitions(func)

# 调用执行方法
print(rdd_mapPartitions.collect())