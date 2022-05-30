from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
spark = SparkSession.builder.getOrCreate()
import pandas


# 创建df进行写入
pd_df = pandas.DataFrame(
    {
        'grade':[1,2,3],
        'losal':[4,5,6],
        'hisal':[10,11,16]
    }
)

df = spark.createDataFrame(pd_df)
# 写入mysql数据  mode指定写方式  table 指定写入的表


df.write.jdbc('jdbc:mysql://192.168.88.100:3306/db_company?useSSL=false',mode='overwrite',table='salgrade',properties={'user':'root','password':'123456','driver':'com.mysql.jdbc.Driver'})
