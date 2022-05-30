from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import *
import pandas


spark = SparkSession.builder.getOrCreate()

pd_df = pandas.DataFrame({
    'id':[1,2,3,4],
    'name':['a','b','c','d'],
    'age':[12,33,44,11]
})

df = spark.createDataFrame(pd_df)

# 自定义加1 方法
# 使用内置函数中udf装饰器注册自定义函数
# IntegerType() 指定返回值类型
@F.udf(IntegerType())
def func(data):
    # data 是接受参数，自己定义
    # 接受指定字段的数据内容
    # 接受一个int数据
    print(data)
    return data+1


# 装饰器注册只能在DLS中调用，不能在sql中使用
# DSL的调用
df.select(func(df['age']),df['age'],df['name']).show()

