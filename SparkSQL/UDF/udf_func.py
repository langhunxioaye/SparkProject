from pyspark.sql import SparkSession
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
def func(data):
    # data 是接受参数，自己定义
    # 接受指定字段的数据内容
    # 接受一个int数据
    print(data)
    return data+1

# 注册为spark的udf函数
# 参数1 是指定一个函数名方便在sql语句中进行调用， 参数2 是自定义的函数，进行注册  参数3 指定自定义函数的返回值类型
# func_add_dsl 接受变量值是在DSL方法进行调用的名字
func_add_dsl= spark.udf.register('func_add',func,IntegerType())

# DSL的调用
df.select(func_add_dsl(df['age'])).show()

# SQL 中调用
df.createTempView('stu')
spark.sql('select id,name,age,func_add(age) from stu ').show()
