# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置
conf = SparkConf().set('deploy_mode','cluster')

# 3 创建sparkcontext对象
sc = SparkContext()

# 4 获取rdd数据
rdd = sc.parallelize([1,2,3,4,5])

# 5 是Trabsformation方法进行计算
# lambda x:x  冒号前十接受参数   冒号后是计算逻辑
# groupBy根据编写分组条件，将条件计算结果相同的数据分成一组
rdd_group = rdd.groupBy(lambda x: x%3) # x%2  根据余数进行分组，余数相同分成一组
# 在spark 中 (key,value)  就是一个k-v数
# mapValues可以获取value值部分
rdd_mapvalue  = rdd_group.mapValues(list) # 取出value值部分后，将数据转化为list类型

# 6 触发执行业务，获取展示结果 使用action方法
print(rdd_mapvalue.collect())
