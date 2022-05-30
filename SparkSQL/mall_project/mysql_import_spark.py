from pyspark.sql import SparkSession

spark = SparkSession.builder.\
    config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse").\
    config("hive.metastore.uris", "thrift://node1:9083").\
    enableHiveSupport().\
    getOrCreate()

# 读取mysql数据导入ods层表中
df = spark.read.jdbc('jdbc:mysql://192.168.88.100:3306/db_company?useSSL=false',table='dept',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})

# df.show() format 指定写入数据的格式  mode 是覆盖吸入
df.write.saveAsTable('mall_ods.dept',format='orc',mode='overwrite')