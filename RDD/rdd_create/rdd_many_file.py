from pyspark import SparkContext

sc = SparkContext(master='yarn')

# textFile方式读取多个文件
# textFile在同一目录下读取多个小文件时，会将每个文件的数据单独存放一个分区
rdd = sc.textFile('/sparklog') # 读取hdfs上/sparklog下的多个文件
# 每个分区都会有一个task进行执行，小文件过多，就会产生过多分区数据，就间接需要占用大量task线程

# 使用wholeTextFiles会将小文件数据合并一起，进行数据的整体分区划分
# 把所有的小文件合并之后当成一个文件进行分区操作
rdd2 = sc.wholeTextFiles('/sparklog')

# 查看读取多个文件后，rdd中产生分区数据量
# getNumPartitions 可以获取rdd中的分区数量
print(rdd.getNumPartitions())
print(rdd2.getNumPartitions())