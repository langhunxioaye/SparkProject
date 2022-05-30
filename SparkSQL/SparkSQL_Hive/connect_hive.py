from pyspark.sql import SparkSession

# 在创建SparkSession是指定连接hive的配置信息，就可以连接到hive的metastore服务
# park.sql.warehouse.dir 配置表和库在hdfs上默认创建路径
# hive.metastore.uris  指定metastore的连接信息
# enableHiveSupport  指定后可以将datafram数据转化为hive表，也可以把hive数据转化dataframe数据
spark = SparkSession.builder.\
    config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse").\
    config("hive.metastore.uris", "thrift://node1:9083").\
    enableHiveSupport().\
    getOrCreate()

# 使用sql语句操作
# sql查询后的结果是转化的datafram类型
spark.sql('show databases;').show()

# 创建
# spark.sql('create database itcast')


