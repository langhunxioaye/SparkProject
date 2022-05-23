from pyspark import SparkContext,SparkConf

# 配置

# 生成sc对象
sc = SparkContext(master='yarn') # master 指定使用那种资源调度，通过master指定主服务 appName 指定计算任务名称  conf 指定配置

# 获取数据生成rdd
rdd = sc.parallelize([6,5,3,7])

# 计算rdd数据，使用转化方法进行计算
rdd_map = rdd.map(lambda x:x+1)  # [7,6,4,8]

# rdd执行算子，触发计算
# 将计算结果输出到hdfs上，默认数据是两个分区，所以输出时写入两个文件
# repartition 重分区方法的使用，可以对计算后的数据重新进行分区，
# repartition可以指定任意的分区数量
rdd_repartition =rdd_map.repartition(3)
print(rdd_repartition.getNumPartitions())
rdd_repartition.saveAsTextFile('hdfs://192.168.88.100:8020/output3') # hdfs://node1:8020 hdfs的主服务ip信息