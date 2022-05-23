# 1导入模块
from pyspark import SparkContext,SparkConf

# 2 配置 根据需求进行配置
# conf = SparkConf().set('deploy_mode','cluster')

# 3 创建sparkcontext对象
sc = SparkContext()

# 4 获取rdd数据 在spark中可以使用('a',1)表示一个k-v类型的数据
rdd1 = sc.parallelize([('a',1),('b',1),('c',1)])
rdd2 = sc.parallelize([('b',2),('c',2),('d',2)])

# 5 是Trabsformation方法进行计算
# lambda x:x  冒号前十接受参数   冒号后是计算逻辑
# 对应k-v结构的形式的数据，可以key值关联的形式，关联多个rdd数据
# 使用join方法进行key值关联  join找key值相同的数据
rdd_join = rdd1.join(rdd2) # 类似sql的内关联

rdd_leftOuterJoin= rdd1.leftOuterJoin(rdd2).sortByKey()  # 类似sql中左关联   sortByKey() 按照key排序
rdd_rightOuterJoin= rdd1.rightOuterJoin(rdd2) # 类似sql中的有关联

# 6 触发执行业务，获取展示结果 使用action方法
print(rdd_join.collect())
print(rdd_leftOuterJoin.collect())
print(rdd_rightOuterJoin.collect())
