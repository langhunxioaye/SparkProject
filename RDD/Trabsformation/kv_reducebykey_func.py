# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置

# 3 创建sparkcontext对象
sc = SparkContext()

# 4 获取rdd数据
rdd1 = sc.parallelize([('a',1),('b',1),('c',1),('a',1)])
rdd2 = sc.parallelize([('a','x'),('b','y'),('c','x'),('a','r')])
# 5 是Trabsformation方法进行计算
# 按照key进行分组,将同一分组下的数据进行计算，接受两个参数
rdd_reduceByKey = rdd2.reduceByKey(lambda x,y:x+y)

# 6 触发执行业务，获取展示结果 使用action方法
print(rdd_reduceByKey.collect())
