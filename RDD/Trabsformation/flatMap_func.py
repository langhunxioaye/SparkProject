# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置
conf = SparkConf().set('deploy_mode','cluster')

# 3 创建sparkcontext对象
sc = SparkContext(master='yarn',appName='flatmap_func',conf=conf)

# 4 获取rdd数据
rdd = sc.parallelize([1,2,3,4,5])

# 5 是Trabsformation方法进行计算
# lambda x:x  冒号前十接受参数   冒号后是计算逻辑
# flatMap 接受rdd中的每个元素数据，对么每个元素数据进行计算，要求将计算的结果放入一个列表中返回，会将返回的列表数据统一进行合并
rdd_map = rdd.flatMap(lambda x:[[x+1]]) # [[2],[3],[4],[5],[6]]


# 6 触发执行业务，获取展示结果 使用action方法
print(rdd_map.collect())
