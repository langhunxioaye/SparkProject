from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# 使用sparksession下的read方法读取 数据
# 集群模式读不了本地文件 ，需要读取hdfs文件  hdfs://192.168.88.100:8020/路径
spark.read.text('file:///export/server/spark/examples/src/main/resources/people.txt').show()
# 读json文件
# spark.read.json('file:///export/server/spark/examples/src/main/resources/people.json').show()
# 读取csv文件  比较特殊  csv 有分割符   表头
# sep 可以指定csv文件中的数据分割方式,
# header 如果csv中有表头数据(字段数据)  需要header=True
# spark.read.csv('file:///export/server/spark/examples/src/main/resources/people.csv',header=True,sep=';').show()
# orc文件数据
# spark.read.orc('file:///export/server/spark/examples/src/main/resources/users.orc').show()
# parquet
# spark.read.parquet('file:///export/server/spark/examples/src/main/resources/users.parquet').show()

