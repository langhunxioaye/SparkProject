# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置

# 3 创建sparkcontext对象
sc = SparkContext()

# 4 获取rdd数据
rdd1 = sc.parallelize([('a',1),('b',1),('c',1),('a',1)])
# 5 是Trabsformation方法进行计算
# 接受两个参数,可以指定初始值，在计算时先按照分区分别进行计算，然后再讲不同分区内的数据按照key合并一起计算
rdd_reduceByKey = rdd1.foldByKey(1,lambda x,y:x+y)
# 等价与下面的方法
rdd_aggregateByKey = rdd1.aggregateByKey(1,lambda x,y:x+y,lambda x,y:x+y)  # 第一个lambda方法计算的是分区的数据，第二个是计算分组后的数据

# 6 触发执行业务，获取展示结果 使用action方法
print(rdd1.glom().collect())
print(rdd_reduceByKey.collect())
print(rdd_aggregateByKey.collect())



