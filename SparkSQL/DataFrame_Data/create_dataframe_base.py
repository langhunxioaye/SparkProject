# 导入表结构类型模块
from pyspark.sql.types import *
# 导入Row类
from pyspark.sql import Row,SparkSession

# 创建schema表结构
schema_type = StructType().\
    add('id',IntegerType(),False).\
    add('name',StringType()).\
    add('age',IntegerType()).\
    add('gender',StringType())
# 创建row行对象
# 第一行数据
r1 = Row(1,'张三',18,'男')
# 第二行数据
r2 = Row(2,'李四',19,'男')

# 将表结构和行对象放在一起创建dataframe
# 使用SparkSession类对象下的方法进行创建  固定写法
spark = SparkSession.builder.getOrCreate()

# createDataFrame方法将行和schema组合在一起形成dataframe数据
# createDataFrame(参数1，参数2)
# 参数1 就是行对象，以列表的形式将多个行对象放在一起
# 参数2 就是表结构
df_data = spark.createDataFrame([r1,r2],schema_type)

# 使用dataframe的show 方法查看dataframe数据内容
df_data.show() # 会打印输出数据