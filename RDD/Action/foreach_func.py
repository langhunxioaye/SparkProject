from pyspark import SparkContext,SparkConf

# 配置

# 生成sc对象
sc = SparkContext() # master 指定使用那种资源调度，通过master指定主服务 appName 指定计算任务名称  conf 指定配置

# 获取数据生成rdd
rdd = sc.parallelize([6,5,3,7])

# 计算rdd数据，使用转化方法进行计算
rdd_map = rdd.map(lambda x:x+1)  # [7,6,4,8]

# rdd执行算子，触发计算
# 遍历元素数据,对元素数据进行计算处理
rdd_map.foreach(lambda x:print(x))  # local本地模式会打印输出,集群模式下不在输出，集群模式可以写入到hdfs中
