# 导入spark的内置函数
from pyspark.sql import SparkSession,functions as F

import pandas as pd

# 创建sparksession对象
spark = SparkSession.builder.getOrCreate()
# 设置check的存储路径
sc = spark.sparkContext
sc.setCheckpointDir('hdfs://192.168.88.100:8020/spark_checkpoint')

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


# df缓存
# df.persist() # df数据会自动触发缓存

# 设置checkpoint
df.checkpoint() # df数据会自动触发缓存

# 后续使用df就是使用缓存的df
# 默认情况是自动删除缓存
# 手动删除
# df.unpersist()