from pyspark.sql import SparkSession

spark = SparkSession.builder.\
    config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse").\
    config("hive.metastore.uris", "thrift://node1:9083").\
    enableHiveSupport().\
    getOrCreate()

# sql查询后是dataframe数据
df = spark.sql('select * from mall_app.tb_country_quantity')
# df.show()
df.write.jdbc('jdbc:mysql://192.168.88.100:3306/mall_app?useSSL=false',mode='overwrite',table='max_country_quantity_sql',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})