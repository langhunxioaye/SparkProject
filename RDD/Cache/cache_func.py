from pyspark import SparkContext, StorageLevel

# 生成rdd数据，在日志可以看到缓存形式，使用集群
sc = SparkContext(master='yarn',appName='cache_func1')

# 生成rdd数据
rdd=sc.parallelize([1,2,3,4])

# 计算
rdd_map = rdd.map(lambda x:x+1)

# 将计算结果进行缓存
rdd_map_persist = rdd_map.persist() # storageLevel=StorageLevel.MEMORY_ONLY 默认将缓存数据存储到内存
# rdd_map_cacher = rdd.cache() # cache内部还是调用persist

# 使用执行函数触发缓存,会会将map计算后的数据进行缓存操作
rdd_map.collect()

# 使用缓存后的数据进行计算
rdd_filter = rdd_map.filter(lambda x:x>3)
# 展示结果
print(rdd_filter.collect())

# 默认情况下程序计算完成就自动释放
# 也可以手动释放unpersist释放缓存
rdd_map.unpersist()
rdd_map.collect()

# 释放缓存后再次进行数据的计算
rdd_filter2 = rdd_map.filter(lambda x:x>4)
# 展示结果
print(rdd_filter2.collect())
