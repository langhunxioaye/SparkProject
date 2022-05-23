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
# coalesce指定分区数量 ，但是指定的分区数量指定小于等于原来的分区数量,不能大于原来的分区数量
data_num = rdd_map.getNumPartitions()
print(f'原始分区分区数据：{data_num}')
# 修改分区数据为1
data_num1 = rdd_map.coalesce(1).getNumPartitions()
print(f'修改分区数据为1：{data_num1}')
# 修改分区数据为4
data_num2 = rdd_map.coalesce(4).getNumPartitions()
print(f'修改分区数据为4：{data_num2}')
