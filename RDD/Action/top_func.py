from pyspark import SparkContext,SparkConf

# 配置

# 生成sc对象
sc = SparkContext() # master 指定使用那种资源调度，通过master指定主服务 appName 指定计算任务名称  conf 指定配置

# 获取数据生成rdd
rdd = sc.parallelize([6,5,3,7])

# 计算rdd数据，使用转化方法进行计算
rdd_map = rdd.map(lambda x:x+1)  # [7,6,4,8]

# rdd执行算子，触发计算
# top 方法，可以对rdd数据进行排序，顺序是从大到小进行排序 [8,7,6,4]，取出指定数据量的数据
print(rdd_map.top(num=10))  # num 指定取值的数量