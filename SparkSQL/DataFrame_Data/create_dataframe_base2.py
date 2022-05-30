# 演示不指定schema类型，有spark自动对数据进行类型判断,生成表结构信息
from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row

# 创建row对象
# 创建对象是指定字段名字
r1 = Row(id=1,name='张三',age=20,gender='男')

# schema_type = StructType().\
#     add('id',IntegerType(),False).\
#     add('username',StringType()).\
#     add('age',IntegerType()).\
#     add('gender',StringType())

# 使用row对象创建dataframe，有spark自动判断类型
# 生成sparksession对象
spark = SparkSession.builder.getOrCreate()
# createDataFrame生成dataframe数据
df = spark.createDataFrame([r1])
# show方法查看数据
df.show()
# printSchema查看Schema信息
df.printSchema()