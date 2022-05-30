from pyspark.sql import SparkSession
import pandas as pd
# 创建sparksession对象
spark = SparkSession.builder.getOrCreate()

# 生成dataframe数据
pd_df = pd.DataFrame(
    {
        'user':['1,张三','2,李四','3,王五']
    }
)


pd_df2 = pd.DataFrame(
    {
        'id':[1,2,3],
        'name':['张三','李四','王五'],
        'age':[19,19,20]

    }
)

df = spark.createDataFrame(pd_df)
df2 = spark.createDataFrame(pd_df2)
#  查看数据
df.show()
df2.show()

# 使用是df下方法
# text 写入,当成一整行写入
# df.write.text('file:///root/data/b')

# 使用json方式写入多个字段数据
# mode 指定写入方式
# append  追加写入  overwrite 覆盖写入
# df2.write.json('file:///root/data/c',mode='overwrite')
# 写入hdfs
# df2.write.json('hdfs://node1:8020/data',mode='append')

# 使用csv保存多个字段数据 表头和  分割符  写入数据可以指定是否写入表头信息和分割形式
# header=True
# df2.write.csv('hdfs://node1:8020/data1',mode='overwrite',sep='+',header=True)

# 以orc方式存储
# df2.write.orc('hdfs://node1:8020/data2',mode='overwrite')

# 以parquet方式存储
df2.write.parquet('hdfs://node1:8020/data3',mode='overwrite')