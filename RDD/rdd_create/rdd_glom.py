# rdd 是以分区的形式管理计算数据的
# 会将读取到数据按照分区存储到内存上，对每个分区的分别进行计算
from pyspark import SparkContext

# 1、生成sc对象
sc  = SparkContext()
# 2、读取数据转化为rdd数据
# minPartitions 可以指定将读取到的数据分成多少个分区，默认是两个
# 分区指定后rdd内部会自动掉用方法将数据按照分区进行划分
rdd = sc.textFile('/datas/input',minPartitions=6)


# 3、对rdd数据进行计算
# 4、获取计算结果
# 计算结果展示时，数据都是放在一起的，实际内部是按照分区保存数据
# glom()以分区形式查看数据
# [['spark python spark hive spark hive', 'python spark hive spark python'], ['mapreduce spark hadoop hdfs hadoop spark', 'hive mapreduce']]
# 在展示的数据中，将数据有放到两个列表中展示。说明数据被分成两个分区，一个列表就对应一个分区数据
# rdd默认就是将数据分成两个分区

# 指定后分区展示结果
# [['spark python spark hive spark hive'], ['python spark hive spark python'], ['mapreduce spark hadoop hdfs hadoop spark'], ['hive mapreduce']]
print(rdd.glom().collect())
