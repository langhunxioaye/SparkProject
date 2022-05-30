# 导入spark的内置函数
from pyspark.sql import SparkSession,functions as F

import pandas as pd

# 创建sparksession对象
spark = SparkSession.builder.getOrCreate()

# 创建dataframe数据
pd_data = pd.DataFrame({
    'id':[1,2,3],
    'name':['张三','李四','王五'],
    'age':[20,22,33],
    'gender':['男','男','女'],
    'str_data':['a,b,c','d,f,g','q,w,e'],
    'create_time':['2022-03-30','2022-03-28','2022-02-28'],
    'uninx_time':['1428476400','1428476422','1428476455']
})
# 创建df数据
df = spark.createDataFrame(pd_data)

df.show()


# 内置函数对select字段数据进行处理
# 聚合计算处理  对整张的表的行数据进行聚合
# df.select(F.avg(df['age']),F.sum(df['age']),F.max(df['age'])).show()
# 对子段命名别称 alias
# df.select(F.avg(df['age']).alias('agv_data'),F.sum(df['age']).alias('sum_data'),F.max(df['age'])).show()


# 先分组，使用内置函数进行聚合操作 配合agg方法
# df.groupby('gender').agg(F.avg(df['age']).alias('agv_data'),F.sum(df['age']).alias('sum_data')).orderBy('agv_data').show()

# 字符串数据拼接
# concat
# df.select(F.concat(df['name'],df['gender']).alias('concat') ).show()
# concat_ws 指定分割符
# df.select(F.concat_ws(':',df['name'],df['gender']).alias('cw') ).show()
# 字符串切割
df.select(F.split(df['str_data'],',') ).show()
# 爆炸函数
df.select(F.explode(F.split(df['str_data'],',')) ).show()



# 日期操作  补全时间信息
# df.select(F.to_timestamp(df['create_time'],'yyyy-MM-dd')).show()
# 获取当前的uninx时间和日期
df.select(F.current_date()).show()
df.select(F.current_timestamp()).show()
df.select(F.unix_timestamp()).show()

# uninx 时间转化
df.select(F.from_unixtime(df['uninx_time'],format='yyyy-MM-dd')).show()

