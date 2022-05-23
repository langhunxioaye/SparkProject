
from pyspark import SparkContext

sc = SparkContext()

rdd = sc.parallelize([1,2,3,4])

# 使用分区的计算方法

# 定义一个迭代器
def func(data):  # data接受的是一个分区的数据  [1,2]
    for i in data:  # 遍历分区数据进行计算
        yield i+1      # yield每次返回一个计算数据

# foreachPartition  遍历每个分区的数据传入data参数
rdd.foreachPartition(lambda data:print(func(data)))  # 这个和mapPartition的方法作用一样
