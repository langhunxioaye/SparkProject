# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置
# conf = SparkConf().set('deploy_mode','cluster')

# 3 创建sparkcontext对象
sc = SparkContext()

# 4 获取rdd数据
rdd = sc.parallelize([2,3,6,7,9])

# 5 是Trabsformation方法进行计算


# 6 触发执行业务，获取展示结果 使用action方法
# 给定初始值，进行计算，先进行各分区计算，然后在将分区数取合在一起计算
data =rdd.fold(0,lambda x,y:x+y)  # 初始为0 等价与reduce
print(rdd.glom().collect())
print(data)

# [[2, 3], [4, 5, 7]] 1+2+3 6  1+4+5+7 17   1+6+17
# [[2, 3], [6, 7, 9]]  1+2+3 6    1+6+7+9 23   1+6+23 30