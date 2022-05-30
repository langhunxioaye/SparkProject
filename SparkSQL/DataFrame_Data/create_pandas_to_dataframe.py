# 导入使用的模块
from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row
import pandas as pd

# pandas中的数据是在本地内存上存储 ,将pandas中的数据转成spark的方式进行分布式存储，在多台机器上进行计算，提升计算效率

# 生成SparkSession对象
spark = SparkSession.builder.getOrCreate()
# 创建pandas的dataframe数据
pd_df = pd.DataFrame({
    'id':[1,2],
    'name':['张三','李四'],
    'age':[18,20],
    'gender':['男','男']
})

# 指定表字段信息
schema_type = StructType().\
    add('id',IntegerType(),False).\
    add('username',StringType()).\
    add('age',IntegerType()).\
    add('gender',StringType())

# 将pandas的df转化为spark的df
# 使用createDataFrame方法进行转化,也可将schem信息传递进去，没有指定则自动判断
spark_df = spark.createDataFrame(pd_df,schema_type)
# 查看信息
spark_df.show()
spark_df.printSchema()