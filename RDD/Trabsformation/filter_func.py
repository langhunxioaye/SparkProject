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
# filter方法类似于sql的where  可以对传递rdd中的的数据进行判断  符合条件的数据会进行返回
# rdd_filter= rdd.filter(lambda x: x > 3)
# 进行链式调用 对上一个rdd的结果进行过滤
rdd_filter = rdd.map(lambda x:x+2).filter(lambda x: x%2 == 0)  # 取出双数数据


# 6 触发执行业务，获取展示结果 使用action方法
print(rdd_filter.collect())
