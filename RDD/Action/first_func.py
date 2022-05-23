from pyspark import SparkContext,SparkConf

# 配置

# 生成sc对象
sc = SparkContext() # master 指定使用那种资源调度，通过master指定主服务 appName 指定计算任务名称  conf 指定配置

# 获取数据生成rdd
rdd = sc.parallelize([1,2,3,4])

# 计算rdd数据，使用转化方法进行计算
rdd_map = rdd.map(lambda x:x+1)  # [2,3,4,5]

# rdd执行算子，触发计算
# first获取rdd中的第一个值   类似sql中 limit 1
print(rdd_map.first())