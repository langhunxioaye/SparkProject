from pyspark import SparkContext, StorageLevel

# 生成rdd数据，在日志可以看到缓存形式，使用集群
sc = SparkContext(master='yarn',appName='cache_func1')

# 指定checkponti的在hdfs上的存储目录
sc.setCheckpointDir('hdfs://192.168.88.100:8020/checkpoint_data')

# 生成rdd数据
rdd=sc.parallelize([1,2,3,4])

# 计算
rdd_map = rdd.map(lambda x:x+1)

# 进行保存数据到hdfs上
rdd_map.checkpoint()
rdd_map.cache()
# 通过执行方法触发，不触发是不会保存
rdd_map.collect()

rdd_map.filter(lambda x:x>2) # 有缓存有checkpoint，优先获取缓存

