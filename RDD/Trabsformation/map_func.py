# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置
conf = SparkConf().set('deploy_mode','cluster')

# 3 创建sparkcontext对象
sc = SparkContext(master='yarn',appName='map_func',conf=conf)

# 4 获取rdd数据
rdd = sc.parallelize([1,2,3,4,5])

# 5 是Trabsformation方法进行计算
# lambda x:x  冒号前十接受参数   冒号后是计算逻辑
# map会读取rdd中的每个元素数据 ，将每个元素数据传递到lambda的接受参数中，map要求lambda中的接受参数是一个
# map 返回结果可以根据自身计算情况进行指定
# rdd_map = rdd.map(lambda x:x+1) # [2,3,4,5,6]
rdd_map = rdd.map(lambda x:{x+1:'itcast'})
# 等价如下方法使用
def func(x):
    return {x+1:'itcast'}
rdd_map2 = rdd.map(func)


# 6 触发执行业务，获取展示结果 使用action方法
# print(rdd_map.collect())
data = rdd_map2.collect()
print(type(data[0]))