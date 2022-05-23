# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置
# conf = SparkConf().set('deploy_mode','cluster')

# 3 创建sparkcontext对象
sc = SparkContext()

# 4 获取rdd数据
rdd = sc.parallelize([1,2,3,4,5])

# 5 是Trabsformation方法进行计算


# 6 触发执行业务，获取展示结果 使用action方法
# 统计rdd中的元素的值进行计算
data =rdd.reduce(lambda x,y:x+y)
print(data)
