# 模块导入
from pyspark import SparkContext,SparkConf
conf=SparkConf().set('spark.default.parallelism','4')
# sc对象
sc = SparkContext(master='yarn',appName='spark1111',conf=conf)

# 读取文件数据转化为rdd
rdd = sc.parallelize(['a','b','c','d','a'])
# rdd_flotMap = rdd.flatMap(lambda x:x.split())
# # map方法 转化为key-v的初始值
rdd_map = rdd.map(lambda x:(x,1))

# 对转化后的k-v数据进行聚合计算
rdd_reduceBykey=  rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
# print(rdd_reduceBykey.collect())
print(rdd_reduceBykey.getNumPartitions())
