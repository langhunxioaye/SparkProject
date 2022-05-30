# dataFrame的字段修改

from pyspark.sql import SparkSession,functions as F
import pandas as pd

# 创建sparksession对象
spark = SparkSession.builder.getOrCreate()

# 创建dataframe数据
pd_data1 = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['张三', '李四', '王五'],
    'age': [20, 22, 33],
    'gender': ['男', '男', '女'],
})

df  =spark.createDataFrame(pd_data1)

# 将计算的数据添加到新列,增加新字段
df.withColumn('new_filed',df['age']+2).show()

# 不支持聚合计算,聚合会改变行数 如下是错误
# df.withColumn('new_filed',F.sum(df['age'])).show()


# 修改字段名
df.withColumnRenamed('name','username').show()