# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置
# conf = SparkConf().set('deploy_mode','cluster')

# 3 创建sparkcontext对象
sc = SparkContext(master='yarn',appName='flatmap_func')

# 4 获取rdd数据
rdd = sc.parallelize([1,2,3,4,5,3,4])

# 5 是Trabsformation方法进行计算
# lambda x:x  冒号前十接受参数   冒号后是计算逻辑
# distinct 会将rdd中重复数据去重返回
rdd_distinct = rdd.distinct()

# 6 触发执行业务，获取展示结果 使用action方法
print(rdd_distinct.collect())
