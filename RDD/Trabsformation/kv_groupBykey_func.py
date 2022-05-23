# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置

# 3 创建sparkcontext对象
sc = SparkContext()

# 4 获取rdd数据
rdd1 = sc.parallelize([('a',1),('b',1),('c',1),('a',1)])
# 5 是Trabsformation方法进行计算
# 按照key进行分组，key相同会放在一起
rdd_groupbykey = rdd1.groupByKey()

# 使用mapvalue转化value值为list
rdd_mapvalue = rdd_groupbykey.mapValues(list).sortByKey() # sortByKey()按照key值排序

# 6 触发执行业务，获取展示结果 使用action方法
print(rdd_mapvalue.collect())
