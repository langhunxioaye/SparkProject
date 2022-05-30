from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
spark = SparkSession.builder.getOrCreate()


# 读取mysql数据 指定Mysql连接信息(ip:端口/库名)  table=emp 是指定表名
# 读取到的数据是一个datafram数据
df = spark.read.jdbc('jdbc:mysql://192.168.88.100:3306/db_company?useSSL=false',table='salgrade',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})
df.show()

