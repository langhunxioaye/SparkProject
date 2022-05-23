from pyspark import SparkContext,SparkConf

# 配置

# 生成sc对象
sc = SparkContext() # master 指定使用那种资源调度，通过master指定主服务 appName 指定计算任务名称  conf 指定配置

# 获取数据生成rdd
rdd = sc.parallelize([6,5,3,7,2,4,5,8,1])

# 计算rdd数据，使用转化方法进行计算
rdd_map = rdd.map(lambda x:x+1)  # [7,6,4,8,3,5,6,9,2])

# rdd执行算子，触发计算
# take取值 take取值时，不会进行排序，可以指定取值的数量，单独take方法时取前几值
print(rdd_map.take(num=5))  # num 指定取值的数量
# 随机取值
print(rdd_map.takeSample(True,5,1)) # True 取值时允许出现重复值，5 是取值个数，1 随机数种子（会根据种子数据，根据算法计算出一个随机数）
print(rdd_map.takeSample(False,5,1))# True 取值时不允许出现重复值

# 排序取值
print(rdd_map.takeOrdered(num=5)) # 默认排序是从小到大进行排序，然后将排序后的数据去除前5个值

print(rdd_map.takeOrdered(num=5,key=lambda x:-x)) # key 可以指定排序方法  x是接受rdd的元素数据，-x是返回结果按照降序排序